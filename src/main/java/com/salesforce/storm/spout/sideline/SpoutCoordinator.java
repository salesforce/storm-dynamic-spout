package com.salesforce.storm.spout.sideline;

import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.coordinator.SpoutMonitor;
import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.tupleBuffer.TupleBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Spout Coordinator.
 *
 * Manages X number of spouts and coordinates their nextTuple(), ack() and fail() calls across threads
 */
public class SpoutCoordinator {
    // Logging.
    private static final Logger logger = LoggerFactory.getLogger(SpoutCoordinator.class);

    /**
     * How long we'll wait for all VirtualSpout's to cleanly shut down, before we stop
     * them with force, in Milliseconds.
     */
    public static final int MAX_SPOUT_STOP_TIME_MS = 10000;

    /**
     * How often we'll make sure each VirtualSpout persists its state, in Milliseconds.
     */
    public static final long FLUSH_INTERVAL_MS = 30000;

    /**
     * The size of the thread pool for running virtual spouts for sideline requests.
     */
    public static final int SPOUT_RUNNER_THREAD_POOL_SIZE = 10;

    /**
     * Which Clock instance to get reference to the system time.
     * We use this to allow injecting a fake System clock in tests.
     *
     * ThreadSafety - Lucky for us, Clock is all thread safe :)
     */
    private Clock clock = Clock.systemUTC();

    /**
     * Queue of spouts that need to be passed to the monitor and spun up.
     */
    private final Queue<DelegateSidelineSpout> newSpoutQueue = new ConcurrentLinkedQueue<>();

    /**
     * Queue for tuples that are ready to be emitted out into the topology.
     */
    private final TupleBuffer tupleBuffer;

    /**
     * Buffer by spout consumer id of messages that have been acked.
     */
    private final Map<String,Queue<TupleMessageId>> ackedTuplesQueue = new ConcurrentHashMap<>();

    /**
     * Buffer by spout consumer id of messages that have been failed.
     */
    private final Map<String,Queue<TupleMessageId>> failedTuplesQueue = new ConcurrentHashMap<>();

    /**
     * For capturing metrics.
     */
    private final MetricsRecorder metricsRecorder;

    /**
     * Thread Pool Executor.
     */
    private ExecutorService executor;

    /**
     * The spout monitor runnable, which handles spinning up threads for sideline spouts.
     */
    private SpoutMonitor spoutMonitor;

    /**
     * Copy of the Storm topology configuration.
     */
    private Map<String, Object> topologyConfig;

    /**
     * Create a new coordinator, supplying the 'fire hose' or the starting spouts.
     * @param spout Fire hose spout
     */
    public SpoutCoordinator(DelegateSidelineSpout spout, MetricsRecorder metricsRecorder, TupleBuffer tupleBuffer) {
        this.metricsRecorder = metricsRecorder;
        this.tupleBuffer = tupleBuffer;
        addSidelineSpout(spout);
    }

    /**
     * Add a new spout to the coordinator, this will get picked up by the coordinator's monitor, opened and
     * managed with teh other currently running spouts.
     * @param spout New delegate spout
     */
    public void addSidelineSpout(final DelegateSidelineSpout spout) {
        getNewSpoutQueue().add(spout);
    }

    /**
     * Open the coordinator and begin spinning up virtual spout threads.
     */
    public void open(Map<String, Object> topologyConfig) {
        // Create copy of topology config
        this.topologyConfig = Collections.unmodifiableMap(topologyConfig);

        // Create a countdown latch
        final CountDownLatch latch = new CountDownLatch(getNewSpoutQueue().size());

        // Create new single threaded executor.
        this.executor = Executors.newSingleThreadExecutor();

        // Create our spout monitor instance.
        spoutMonitor = new SpoutMonitor(
            getNewSpoutQueue(),
            getTupleBuffer(),
            getAckedTuplesQueue(),
            getFailedTuplesQueue(),
            latch,
            getClock()
        );

        // Configure how often it runs
        if (getTopologyConfig().containsKey(SidelineSpoutConfig.MONITOR_THREAD_INTERVAL_MS)) {
            spoutMonitor.setMonitorThreadInterval((long) getTopologyConfigItem(SidelineSpoutConfig.MONITOR_THREAD_INTERVAL_MS), TimeUnit.MILLISECONDS);
        }

        // Start executing the spout monitor in a new thread.
        executor.submit(spoutMonitor);

        try {
            latch.await();
        } catch (InterruptedException ex) {
            logger.error("Exception while waiting for the coordinator to open it's spouts {}", ex);
        }
    }

    /**
     * Acks a tuple on the spout that it belongs to.
     * @param id Tuple message id to ack
     */
    public void ack(final TupleMessageId id) {
        if (!getAckedTuplesQueue().containsKey(id.getSrcVirtualSpoutId())) {
            logger.warn("Acking tuple for unknown consumer");
            return;
        }

        getAckedTuplesQueue().get(id.getSrcVirtualSpoutId()).add(id);
    }

    /**
     * Fails a tuple on the spout that it belongs to.
     * @param id Tuple message id to fail
     */
    public void fail(final TupleMessageId id) {
        if (!getFailedTuplesQueue().containsKey(id.getSrcVirtualSpoutId())) {
            logger.warn("Failing tuple for unknown consumer");
            return;
        }

        getFailedTuplesQueue().get(id.getSrcVirtualSpoutId()).add(id);
    }

    /**
     * @return - Returns the next available KafkaMessage to be emitted into the topology.
     */
    public KafkaMessage nextMessage() {
        return getTupleBuffer().poll();
    }

    /**
     * Stop coordinating spouts, calling this should shut down and finish the coordinator's spouts.
     */
    public void close() {
        spoutMonitor.close();

        try {
            // Call shutdown, which starts a clean shutdown process.
            executor.shutdown();

            // Wait for clean termination
            executor.awaitTermination(MAX_SPOUT_STOP_TIME_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            logger.error("Interupted clean shutdown, forcing stop: {}", ex);
        }

        // If we haven't shut down yet..
        if (!executor.isShutdown()) {
            logger.warn("Shutdown was not completed within {} ms, forcing stop now", MAX_SPOUT_STOP_TIME_MS);
            executor.shutdownNow();
        }
    }

    /**
     * For testing, returns the total number of running spouts.
     * @return The total number of spouts the coordinator is running
     */
    int getTotalSpouts() {
        return spoutMonitor.getTotalSpouts();
    }

    /**
     * @return - TupleBuffer instance.
     */
    TupleBuffer getTupleBuffer() {
        return tupleBuffer;
    }

    /**
     * @return - The acked tuples queues.
     */
    Map<String, Queue<TupleMessageId>> getAckedTuplesQueue() {
        return ackedTuplesQueue;
    }

    /**
     * @return - The failed tuples queues.
     */
    Map<String, Queue<TupleMessageId>> getFailedTuplesQueue() {
        return failedTuplesQueue;
    }

    /**
     * @return - The new virtual spout instance queue.
     */
    Queue<DelegateSidelineSpout> getNewSpoutQueue() {
        return newSpoutQueue;
    }

    /**
     * @return - Clock instance, used for get local system time.
     */
    Clock getClock() {
        return clock;
    }

    /**
     * @return - The topology configuration map.
     */
    private Map<String, Object> getTopologyConfig() {
        return topologyConfig;
    }

    /**
     * Utility method to get a specific entry in the Storm topology config map.
     * @param key - the configuration item to retrieve
     * @return - the configuration item's value.
     */
    private Object getTopologyConfigItem(final String key) {
        return getTopologyConfig().get(key);
    }
}
