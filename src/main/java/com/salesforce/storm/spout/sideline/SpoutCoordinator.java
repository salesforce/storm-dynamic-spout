package com.salesforce.storm.spout.sideline;

import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.coordinator.SpoutMonitor;
import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.tupleBuffer.TupleBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
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
 * Manages X number of virtual spouts and coordinates their nextTuple(), ack() and fail() calls across threads
 */
public class SpoutCoordinator {
    // Logging.
    private static final Logger logger = LoggerFactory.getLogger(SpoutCoordinator.class);

    /**
     * Which Clock instance to get reference to the system time.
     * We use this to allow injecting a fake System clock in tests.
     *
     * ThreadSafety - Lucky for us, Clock is all thread safe :)
     */
    private final Clock clock = Clock.systemUTC();

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
        this.topologyConfig = Tools.immutableCopy(topologyConfig);

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
            getClock(),
            getTopologyConfig()
        );

        // Start executing the spout monitor in a new thread.
        executor.submit(spoutMonitor);

        // Block/wait for all of our VirtualSpout instances to start before continuing on.
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
     * Stop managed spouts, calling this should shut down and finish the coordinator's spouts.
     */
    public void close() {
        try {
            // Call shutdown, which prevents the executor from starting any new tasks.
            executor.shutdown();

            // Call close on the spout monitor
            spoutMonitor.close();

            // Wait for clean termination
            executor.awaitTermination(getMaxTerminationWaitTimeMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            logger.error("Interrupted clean shutdown, forcing stop: {}", ex);
        }

        // If we haven't shut down yet..
        if (!executor.isTerminated()) {
            logger.warn("Shutdown was not completed within {} ms, forcing stop of executor now", getMaxTerminationWaitTimeMs());
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
     * @return - the maximum amount of time we'll wait for spouts to terminate before forcing them to stop, in milliseconds.
     */
    private long getMaxTerminationWaitTimeMs() {
        return (long) getTopologyConfig().get(SidelineSpoutConfig.MAX_SPOUT_SHUTDOWN_TIME_MS);
    }
}
