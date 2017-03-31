package com.salesforce.storm.spout.sideline.coordinator;

import com.salesforce.storm.spout.sideline.FactoryManager;
import com.salesforce.storm.spout.sideline.Tools;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;
import com.salesforce.storm.spout.sideline.kafka.SidelineConsumerMonitor;
import com.salesforce.storm.spout.sideline.tupleBuffer.TupleBuffer;
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
    private final Queue<DelegateSidelineSpout> newSpoutQueue;

    /**
     * This buffer/queue holds tuples that are ready to be sent out to the topology.
     * It is filled by VirtualSidelineSpout instances, and drained by SidelineSpout.
     */
    private final TupleBuffer tupleOutputQueue;

    /**
     * This buffer/queue holds tuples that are ready to be acked by VirtualSidelineSpouts.
     * Its segmented by VirtualSidelineSpout ids => Queue of tuples to be acked.
     * It is filled by SidelineSpout, and drained by VirtualSidelineSpout instances.
     */
    private final Map<String,Queue<TupleMessageId>> ackedTuplesQueue;

    /**
     * This buffer/queue holds tuples that are ready to be failed by VirtualSidelineSpouts.
     * Its segmented by VirtualSidelineSpout ids => Queue of tuples to be failed.
     * It is filled by SidelineSpout, and drained by VirtualSidelineSpout instances.
     */
    private final Map<String,Queue<TupleMessageId>> failedTuplesQueue;

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
     * Calculates progress of VirtualSideline spout instances.
     */
    private SidelineConsumerMonitor consumerMonitor;

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
            final Queue<DelegateSidelineSpout> newSpoutQueue,
            final TupleBuffer tupleOutputQueue,
            final Map<String, Queue<TupleMessageId>> ackedTuplesQueue,
            final Map<String, Queue<TupleMessageId>> failedTuplesQueue,
            final CountDownLatch latch,
            final Clock clock,
            final Map<String, Object> topologyConfig
    ) {
        this.newSpoutQueue = newSpoutQueue;
        this.tupleOutputQueue = tupleOutputQueue;
        this.ackedTuplesQueue = ackedTuplesQueue;
        this.failedTuplesQueue = failedTuplesQueue;
        this.latch = latch;
        this.clock = clock;
        this.topologyConfig = Tools.immutableCopy(topologyConfig);

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
                    logger.warn("!!!!!! Thread interrupted, shutting down...");
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
        for (DelegateSidelineSpout spout; (spout = getNewSpoutQueue().poll()) != null;) {
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
        logger.info("TupleBuffer size: {}, Running VirtualSpoutIds: {}", tupleOutputQueue.size(), spoutThreads.keySet());

        // TODO: All of this is hacky.  And how do we calculate the fire hose status?
        // Loop through spouts instances
        if (consumerMonitor == null) {
            // Create consumer monitor instance
            consumerMonitor = new SidelineConsumerMonitor(new FactoryManager(getTopologyConfig()).createNewPersistenceAdapterInstance());
            consumerMonitor.open(getTopologyConfig());
        }

        // Loop thru all of them to get virtualSpout Ids.
        // This doesn't work for the 'firehose' instance tho..and we have no good way to identify which is the 'firehose'
        for (String virtualSpoutId: spoutRunners.keySet()) {
            Map<TopicPartition, SidelineConsumerMonitor.PartitionProgress> progressMap = consumerMonitor.getStatus(virtualSpoutId);
            if (progressMap == null) {
                continue;
            }
            logger.info("== VirtualSpoutId {} Status ==", virtualSpoutId);

            // Calculate the progress
            for (Map.Entry<TopicPartition,SidelineConsumerMonitor.PartitionProgress> entry : progressMap.entrySet()) {
                final TopicPartition topicPartition = entry.getKey();
                final SidelineConsumerMonitor.PartitionProgress partitionProgress = entry.getValue();

                logger.info("Partition: {} => {}% complete [{} of {} processed, {} remaining]",
                    topicPartition,
                    partitionProgress.getPercentageComplete(),
                    partitionProgress.getTotalProcessed(),
                    partitionProgress.getTotalMessages(),
                    partitionProgress.getTotalUnprocessed()
                );
            }
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
     * @return - ThreadPoolExecutor Service that runs VirtualSpout instances.
     */
    ThreadPoolExecutor getExecutor() {
        return executor;
    }

    /**
     * @return - The new spout queue.
     */
    Queue<DelegateSidelineSpout> getNewSpoutQueue() {
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
