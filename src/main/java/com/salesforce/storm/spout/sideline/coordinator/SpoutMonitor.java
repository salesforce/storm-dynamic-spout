package com.salesforce.storm.spout.sideline.coordinator;

import com.salesforce.storm.spout.sideline.FactoryManager;
import com.salesforce.storm.spout.sideline.Tools;
import com.salesforce.storm.spout.sideline.MessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.DelegateSpout;
import com.salesforce.storm.spout.sideline.kafka.VirtualSpout;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.buffer.MessageBuffer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Monitors and manages the lifecycle of virtual spouts.
 */
public class SpoutMonitor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SpoutMonitor.class);

    /**
     * How often our monitor thread will output a status report, in milliseconds.
     */
    private static final long REPORT_STATUS_INTERVAL_MS = 60_000;

    /**
     * The executor service that we submit VirtualSidelineSpouts to run within.
     */
    private final ThreadPoolExecutor executor;

    /**
     * This Queue contains requests to our thread to fire up new VirtualSidelineSpouts.
     * Instances are taken off of this queue and put into the ExecutorService's task queue.
     */
    private final Queue<DelegateSpout> newSpoutQueue;

    /**
     * This buffer/queue holds tuples that are ready to be sent out to the topology.
     * It is filled by VirtualSidelineSpout instances, and drained by SidelineSpout.
     */
    private final MessageBuffer tupleOutputQueue;

    /**
     * This buffer/queue holds tuples that are ready to be acked by VirtualSidelineSpouts.
     * Its segmented by VirtualSidelineSpout ids => Queue of tuples to be acked.
     * It is filled by SidelineSpout, and drained by VirtualSidelineSpout instances.
     */
    private final Map<String,Queue<MessageId>> ackedTuplesQueue;

    /**
     * This buffer/queue holds tuples that are ready to be failed by VirtualSidelineSpouts.
     * Its segmented by VirtualSidelineSpout ids => Queue of tuples to be failed.
     * It is filled by SidelineSpout, and drained by VirtualSidelineSpout instances.
     */
    private final Map<String,Queue<MessageId>> failedTuplesQueue;

    /**
     * This latch allows the SpoutCoordinator to block on start up until its initial
     * set of VirtualSidelineSpout instances have started.
     */
    private final CountDownLatch latch;

    /**
     * Used to get the System time, allows easy mocking of System clock in tests.
     */
    private final Clock clock;

    /**
     * Storm topology configuration.
     */
    private final Map<String, Object> topologyConfig;

    /**
     * Metrics Recorder implementation for collecting metrics.
     */
    private final MetricsRecorder metricsRecorder;
    
    /**
     * Calculates progress of VirtualSideline spout instances.
     */
    private SpoutPartitionProgressMonitor consumerMonitor;

    private final Map<String, SpoutRunner> spoutRunners = new ConcurrentHashMap<>();
    private final Map<String,CompletableFuture> spoutThreads = new ConcurrentHashMap<>();

    /**
     * Flag used to determine if we should stop running or not.
     */
    private boolean keepRunning = true;

    /**
     * The last timestamp of a status report.
     */
    private long lastStatusReport = 0;

    public SpoutMonitor(
            final Queue<DelegateSpout> newSpoutQueue,
            final MessageBuffer tupleOutputQueue,
            final Map<String, Queue<MessageId>> ackedTuplesQueue,
            final Map<String, Queue<MessageId>> failedTuplesQueue,
            final CountDownLatch latch,
            final Clock clock,
            final Map<String, Object> topologyConfig,
            final MetricsRecorder metricsRecorder
            ) {
        this.newSpoutQueue = newSpoutQueue;
        this.tupleOutputQueue = tupleOutputQueue;
        this.ackedTuplesQueue = ackedTuplesQueue;
        this.failedTuplesQueue = failedTuplesQueue;
        this.latch = latch;
        this.clock = clock;
        this.topologyConfig = Tools.immutableCopy(topologyConfig);
        this.metricsRecorder = metricsRecorder;

        /**
         * Create our executor service with a fixed thread size.
         * Its configured to:
         *   - Time out idle threads after 1 minute
         *   - Keep at most 0 idle threads alive (after timing out).
         *   - Maximum of SPOUT_RUNNER_THREAD_POOL_SIZE threads running concurrently.
         *   - Use essentially an unbounded task queue.
         */
        this.executor = new ThreadPoolExecutor(
            // Number of idle threads to keep around
            getMaxConcurrentVirtualSpouts(),
            // Maximum number of threads to utilize
            getMaxConcurrentVirtualSpouts(),
            // How long to keep idle threads around for before closing them
            1L, TimeUnit.MINUTES,
            // Task input queue
            new LinkedBlockingQueue<>()
        );
    }

    @Override
    public void run() {
        try {
            // Rename our thread.
            Thread.currentThread().setName("VirtualSpoutMonitor");

            // Start monitoring loop.
            while (keepRunning()) {
                // Look for completed tasks
                watchForCompletedTasks();

                // Look for new VirtualSpouts that need to be started
                startNewSpoutTasks();

                // Periodically report status
                reportStatus();

                // Pause for a period before checking for more spouts
                try {
                    Thread.sleep(getMonitorThreadIntervalMs());
                } catch (InterruptedException ex) {
                    logger.warn("Thread interrupted, shutting down...");
                    return;
                }
            }
            logger.warn("Spout coordinator is ceasing to run due to shutdown request...");
        } catch (Exception ex) {
            // TODO: Should we restart the monitor?
            logger.error("!!!!!! SpoutMonitor threw an exception {}", ex);
        }
    }

    /**
     * Submit any new spout tasks as they show up.
     */
    private void startNewSpoutTasks() {
        // Look for new spouts to start.
        for (DelegateSpout spout; (spout = getNewSpoutQueue().poll()) != null;) {
            logger.info("Preparing thread for spout {}", spout.getVirtualSpoutId());

            final SpoutRunner spoutRunner = new SpoutRunner(
                    spout,
                    tupleOutputQueue,
                    ackedTuplesQueue,
                    failedTuplesQueue,
                    latch,
                    getClock(),
                    getTopologyConfig()
            );

            spoutRunners.put(spout.getVirtualSpoutId(), spoutRunner);

            // Run as a CompletableFuture
            final CompletableFuture completableFuture = CompletableFuture.runAsync(spoutRunner, getExecutor());
            spoutThreads.put(spout.getVirtualSpoutId(), completableFuture);
        }
    }

    /**
     * Look for any tasks that have finished running and handle them appropriately.
     */
    private void watchForCompletedTasks() {
        // Cleanup loop
        for (Map.Entry<String, CompletableFuture> entry: spoutThreads.entrySet()) {
            final String virtualSpoutId = entry.getKey();
            final CompletableFuture future = entry.getValue();
            final SpoutRunner spoutRunner = spoutRunners.get(virtualSpoutId);

            if (future.isDone()) {
                if (future.isCompletedExceptionally()) {
                    // This threw an exception.  We need to handle the error case.
                    // TODO: Handle errors here.
                    logger.error("{} seems to have errored", virtualSpoutId);
                } else {
                    // Successfully finished?
                    logger.info("{} seems to have finished, cleaning up", virtualSpoutId);
                }

                // Remove from spoutThreads?
                // Remove from spoutInstances?
                spoutRunners.remove(virtualSpoutId);
                spoutThreads.remove(virtualSpoutId);
            }
        }
    }

    /**
     * This method will periodically show a status report to our logger interface.
     */
    private void reportStatus() {
        // Get current time
        final long now = getClock().millis();

        // Set initial value if none set.
        if (lastStatusReport == 0) {
            lastStatusReport = now;
            return;
        }

        // Only report once every 60 seconds.
        if ((now - lastStatusReport) <= REPORT_STATUS_INTERVAL_MS) {
            // Do nothing.
            return;
        }
        // Update timestamp
        lastStatusReport = now;

        // Show a status report
        logger.info("Active Tasks: {}, Queued Tasks: {}, ThreadPool Size: {}/{}, Completed Tasks: {}, Total Tasks Submitted: {}",
            executor.getActiveCount(),
            executor.getQueue().size(),
            executor.getPoolSize(),
            executor.getMaximumPoolSize(),
            executor.getCompletedTaskCount(),
            executor.getTaskCount()
        );
        logger.info("MessageBuffer size: {}, Running VirtualSpoutIds: {}", tupleOutputQueue.size(), spoutThreads.keySet());

        // Report to metrics record
        getMetricsRecorder().assignValue(getClass(), "running", executor.getActiveCount());
        getMetricsRecorder().assignValue(getClass(), "queued", executor.getQueue().size());
        getMetricsRecorder().assignValue(getClass(), "completed", executor.getCompletedTaskCount());
        getMetricsRecorder().assignValue(getClass(), "poolSize", executor.getPoolSize());

        // Loop through spouts instances
        try {
            if (consumerMonitor == null) {
                // Create consumer monitor instance
                consumerMonitor = new SpoutPartitionProgressMonitor(new FactoryManager(getTopologyConfig()).createNewPersistenceAdapterInstance());
                consumerMonitor.open(getTopologyConfig());
            }

            // Loop thru all of them to get virtualSpout Ids.
            for (final SpoutRunner spoutRunner : spoutRunners.values()) {
                final DelegateSpout spout = spoutRunner.getSpout();
                Map<TopicPartition, SpoutPartitionProgressMonitor.PartitionProgress> progressMap = consumerMonitor.getStatus(spout);

                if (progressMap == null) {
                    continue;
                }

                logger.info("== VirtualSpoutId {} ({} filters applied) Status ==", spout.getVirtualSpoutId(), spout.getNumberOfFiltersApplied());

                // Calculate the progress
                for (Map.Entry<TopicPartition, SpoutPartitionProgressMonitor.PartitionProgress> entry : progressMap.entrySet()) {
                    final TopicPartition topicPartition = entry.getKey();
                    final SpoutPartitionProgressMonitor.PartitionProgress partitionProgress = entry.getValue();

                    // Log progress
                    logger.info("Partition: {} => {}% complete [{} of {} processed, {} remaining]",
                        topicPartition,
                        partitionProgress.getPercentageComplete(),
                        partitionProgress.getTotalProcessed(),
                        partitionProgress.getTotalMessages(),
                        partitionProgress.getTotalUnprocessed()
                    );

                    // Report to metric reporter
                    final String virtualSpoutMetricKey = spout.getVirtualSpoutId() + ".partition" + topicPartition.partition();
                    getMetricsRecorder().assignValue(VirtualSpout.class, virtualSpoutMetricKey + ".totalProcessed", partitionProgress.getTotalProcessed());
                    getMetricsRecorder().assignValue(VirtualSpout.class, virtualSpoutMetricKey + ".totalUnprocessed", partitionProgress.getTotalUnprocessed());
                    getMetricsRecorder().assignValue(VirtualSpout.class, virtualSpoutMetricKey + ".totalMessages", partitionProgress.getTotalMessages());
                    getMetricsRecorder().assignValue(VirtualSpout.class, virtualSpoutMetricKey + ".percentComplete", partitionProgress.getPercentageComplete());
                    getMetricsRecorder().assignValue(VirtualSpout.class, virtualSpoutMetricKey + ".startingOffset", partitionProgress.getStartingOffset());
                    getMetricsRecorder().assignValue(VirtualSpout.class, virtualSpoutMetricKey + ".currentOffset", partitionProgress.getCurrentOffset());
                    getMetricsRecorder().assignValue(VirtualSpout.class, virtualSpoutMetricKey + ".endingOffset", partitionProgress.getEndingOffset());
                }
                // Report how many filters are applied on this virtual spout.
                getMetricsRecorder().assignValue(VirtualSpout.class, spout.getVirtualSpoutId() + ".number_filters_applied", spout.getNumberOfFiltersApplied());
            }
        } catch (Throwable t) {
            logger.error("Caught exception during status checks {}", t.getMessage(), t);

            // Do basic cleanup.
            if (consumerMonitor != null) {
                consumerMonitor.close();
            }
            consumerMonitor = null;
        }
    }

    /**
     * Call this method to indicate that we want to stop all running VirtualSideline Spout instances as well
     * as finish running our monitor thread.
     */
    public void close() {
        keepRunning = false;

        // Ask the executor to shut down, this will prevent it from
        // accepting/starting new tasks.
        executor.shutdown();

        // Empty our run queue, this will prevent queued virtual spouts from getting started
        // during the shutdown process.
        executor.getQueue().clear();

        // Stop consumerMonitor
        if (consumerMonitor != null) {
            consumerMonitor.close();
            consumerMonitor = null;
        }

        // Loop through our runners and request stop on each
        for (SpoutRunner spoutRunner : spoutRunners.values()) {
            spoutRunner.requestStop();
        }

        // Wait for the executor to cleanly shut down
        try {
            logger.info("Waiting a maximum of {} ms for thread pool to shutdown", getMaxTerminationWaitTimeMs());
            executor.awaitTermination(getMaxTerminationWaitTimeMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            logger.error("Interrupted while stopping: {}", ex);
        }

        // If we didn't shut down cleanly
        if (!executor.isTerminated()) {
            // Force a shut down
            logger.warn("Forcing unclean shutdown of thread pool");
            executor.shutdownNow();
        }

        // Clear our our internal state.
        spoutRunners.clear();
        spoutThreads.clear();
    }

    /**
     * @return - return the number of spout runner instances.
     *           *note* - it doesn't mean all of these are actually running, some may be queued.
     */
    public int getTotalSpouts() {
        return spoutRunners.size();
    }

    /**
     * @return - Our system clock instance.
     */
    Clock getClock() {
        return clock;
    }

    /**
     * @return - Storm topology configuration.
     */
    Map<String, Object> getTopologyConfig() {
        return topologyConfig;
    }

    /**
     * @return - how often our monitor thread should run through its maintenance loop, in milliseconds.
     */
    long getMonitorThreadIntervalMs() {
        return ((Number) getTopologyConfig().get(SidelineSpoutConfig.MONITOR_THREAD_INTERVAL_MS)).longValue();
    }

    /**
     * @return - the maximum amount of time we'll wait for spouts to terminate before forcing them to stop, in milliseconds.
     */
    long getMaxTerminationWaitTimeMs() {
        return ((Number) getTopologyConfig().get(SidelineSpoutConfig.MAX_SPOUT_SHUTDOWN_TIME_MS)).longValue();
    }

    /**
     * @return - the maximum amount of concurrently running VirtualSpouts we'll start.
     */
    int getMaxConcurrentVirtualSpouts() {
        return ((Number) getTopologyConfig().get(SidelineSpoutConfig.MAX_CONCURRENT_VIRTUAL_SPOUTS)).intValue();
    }

    /**
     * @return Spouts metrics recorder.
     */
    MetricsRecorder getMetricsRecorder() {
        return metricsRecorder;
    }

    /**
     * @return - ThreadPoolExecutor Service that runs VirtualSpout instances.
     */
    ThreadPoolExecutor getExecutor() {
        return executor;
    }

    /**
     * @return - The new spout queue.
     */
    Queue<DelegateSpout> getNewSpoutQueue() {
        return newSpoutQueue;
    }

    /**
     * Flag to know if we should keep running, or shut down.
     * @return - true if we should keep running, false if we should be stopping/stopped.
     */
    boolean keepRunning() {
        return keepRunning && !Thread.interrupted();
    }
}
