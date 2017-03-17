package com.salesforce.storm.spout.sideline;

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
import java.util.concurrent.Future;
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
     * How long our monitor thread will sit around and sleep between monitoring
     * if new VirtualSpouts need to be started up, in Milliseconds.
     */
    public static final int MONITOR_THREAD_SLEEP_MS = 2000;

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
    private final Map<String,Queue<TupleMessageId>> ackedTuplesInputQueue = new ConcurrentHashMap<>();

    /**
     * Buffer by spout consumer id of messages that have been failed.
     */
    private final Map<String,Queue<TupleMessageId>> failedTuplesInputQueue = new ConcurrentHashMap<>();

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
     * Flag that gets set to false on shutdown, to signal to close up shop.
     * This probably should be renamed at some point.
     */
    private boolean isOpen = false;

    /**
     * Create a new coordinator, supplying the 'fire hose' or the starting spouts.
     * @param spout Fire hose spout
     */
    public SpoutCoordinator(
        final DelegateSidelineSpout spout,
        final MetricsRecorder metricsRecorder,
        final TupleBuffer tupleBuffer
    ) {
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
        newSpoutQueue.add(spout);
    }

    /**
     * Open the coordinator and begin spinning up virtual spout threads.
     */
    public void open() {
        // Mark us as being open
        isOpen = true;

        // Create a countdown latch
        final CountDownLatch latch = new CountDownLatch(newSpoutQueue.size());

        this.executor = Executors.newSingleThreadExecutor();

        spoutMonitor = new SpoutMonitor(
            newSpoutQueue,
            tupleBuffer,
            ackedTuplesInputQueue,
            failedTuplesInputQueue,
            latch,
            clock
        );

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
        if (!ackedTuplesInputQueue.containsKey(id.getSrcConsumerId())) {
            logger.warn("Acking tuple for unknown consumer");
            return;
        }

        ackedTuplesInputQueue.get(id.getSrcConsumerId()).add(id);
    }

    /**
     * Fails a tuple on the spout that it belongs to.
     * @param id Tuple message id to fail
     */
    public void fail(final TupleMessageId id) {
        if (!failedTuplesInputQueue.containsKey(id.getSrcConsumerId())) {
            logger.warn("Failing tuple for unknown consumer");
            return;
        }

        failedTuplesInputQueue.get(id.getSrcConsumerId()).add(id);
    }

    /**
     * @return - Returns the next available KafkaMessage to be emitted into the topology.
     */
    public KafkaMessage nextMessage() {
        return tupleBuffer.poll();
    }

    /**
     * Stop coordinating spouts, calling this should shut down and finish the coordinator's spouts.
     */
    public void close() {
        spoutMonitor.close();

        try {
            executor.awaitTermination(MAX_SPOUT_STOP_TIME_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            logger.error("Caught Exception while stopping: {}", ex);
        }

        executor.shutdownNow();

        // Will trigger the monitor thread to stop running, which should be the end of it
        isOpen = false;
    }

    /**
     * For testing, returns the total number of running spouts.
     * @return The total number of spouts the coordinator is running
     */
    int getTotalSpouts() {
        return spoutMonitor.getTotalSpouts();
    }


    /**
     * Monitors the lifecycle of spinning up virtual spouts.
     */
    private static class SpoutMonitor implements Runnable {

        private static final Logger logger = LoggerFactory.getLogger(SpoutMonitor.class);

        private final ExecutorService executor;
        private final Queue<DelegateSidelineSpout> newSpoutQueue;
        private final TupleBuffer tupleOutputQueue;
        private final Map<String,Queue<TupleMessageId>> ackedTuplesInputQueue;
        private final Map<String,Queue<TupleMessageId>> failedTuplesInputQueue;
        private final CountDownLatch latch;
        private final Clock clock;

        private final Map<String,SpoutRunner> spoutRunners = new ConcurrentHashMap<>();
        private final Map<String,Future> spoutThreads = new ConcurrentHashMap<>();
        private boolean isOpen = true;

        SpoutMonitor(
            final Queue<DelegateSidelineSpout> newSpoutQueue,
            final TupleBuffer tupleOutputQueue,
            final Map<String,Queue<TupleMessageId>> ackedTuplesInputQueue,
            final Map<String,Queue<TupleMessageId>> failedTuplesInputQueue,
            final CountDownLatch latch,
            final Clock clock
        ) {
            this.newSpoutQueue = newSpoutQueue;
            this.tupleOutputQueue = tupleOutputQueue;
            this.ackedTuplesInputQueue = ackedTuplesInputQueue;
            this.failedTuplesInputQueue = failedTuplesInputQueue;
            this.latch = latch;
            this.clock = clock;

            this.executor = Executors.newFixedThreadPool(SPOUT_RUNNER_THREAD_POOL_SIZE);
        }

        @Override
        public void run() {
            try {
                // Rename our thread.
                Thread.currentThread().setName("SidelineSpout-NewSpoutMonitor");

                // Start monitoring loop.
                while (isOpen) {
                    logger.info("Still here.. my input queue is {}", newSpoutQueue.size());

                    for (DelegateSidelineSpout spout; (spout = newSpoutQueue.poll()) != null;) {
                        logger.info("Preparing thread for spout {}", spout.getConsumerId());

                        final SpoutRunner spoutRunner = new SpoutRunner(
                            spout,
                            tupleOutputQueue,
                            ackedTuplesInputQueue,
                            failedTuplesInputQueue,
                            latch,
                            clock
                        );

                        spoutRunners.put(spout.getConsumerId(), spoutRunner);

                        final Future spoutInstance = executor.submit(spoutRunner);

                        spoutThreads.put(spout.getConsumerId(), spoutInstance);
                    }

                    // Pause for a period before checking for more spouts
                    try {
                        Thread.sleep(MONITOR_THREAD_SLEEP_MS);
                    } catch (InterruptedException ex) {
                        logger.warn("!!!!!! Thread interrupted, shutting down...");
                        return;
                    }
                }

                logger.warn("!!!!!! Spout coordinator is ceasing to run...");
            } catch (Exception ex) {
                // TODO: Should we restart the monitor?
                logger.error("SpoutMonitor threw an exception {}", ex);

            }
        }

        public void close() {
            isOpen = false;

            for (SpoutRunner spoutRunner : spoutRunners.values()) {
                spoutRunner.requestStop();
            }

            try {
                executor.awaitTermination(MAX_SPOUT_STOP_TIME_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                logger.error("Caught Exception while stopping: {}", ex);
            }

            executor.shutdownNow();

            spoutRunners.clear();
            spoutThreads.clear();
        }

        public int getTotalSpouts() {
            return spoutRunners.size();
        }
    }

    private static class SpoutRunner implements Runnable {

        private static final Logger logger = LoggerFactory.getLogger(SpoutRunner.class);

        private final DelegateSidelineSpout spout;
        private final TupleBuffer tupleOutputQueue;
        private final Map<String,Queue<TupleMessageId>> ackedTupleInputQueue;
        private final Map<String,Queue<TupleMessageId>> failedTupleInputQueue;
        private final CountDownLatch latch;
        private final Clock clock;

        SpoutRunner(
            final DelegateSidelineSpout spout,
            final TupleBuffer tupleOutputQueue,
            final Map<String,Queue<TupleMessageId>> ackedTupleInputQueue,
            final Map<String,Queue<TupleMessageId>> failedTupleInputQueue,
            final CountDownLatch latch,
            final Clock clock
        ) {
            this.spout = spout;
            this.tupleOutputQueue = tupleOutputQueue;
            this.ackedTupleInputQueue = ackedTupleInputQueue;
            this.failedTupleInputQueue = failedTupleInputQueue;
            this.latch = latch;
            this.clock = clock;
        }

        @Override
        public void run() {
            try {
                logger.info("Opening {} spout", spout.getConsumerId());

                // Rename thread to use the spout's consumer id
                Thread.currentThread().setName(spout.getConsumerId());

                spout.open();

                tupleOutputQueue.addVirtualSpoutId(spout.getConsumerId());
                ackedTupleInputQueue.put(spout.getConsumerId(), new ConcurrentLinkedQueue<>());
                failedTupleInputQueue.put(spout.getConsumerId(), new ConcurrentLinkedQueue<>());

                latch.countDown();

                long lastFlush = clock.millis();

                // Loop forever until someone requests the spout to stop
                while (!spout.isStopRequested()) {
                    // First look for any new tuples to be emitted.
                    logger.debug("Requesting next tuple for spout {}", spout.getConsumerId());

                    final KafkaMessage message = spout.nextTuple();

                    if (message != null) {
                        try {
                            tupleOutputQueue.put(message);
                        } catch (InterruptedException ex) {
                            // TODO: Revisit this
                            logger.error("{}", ex);
                        }
                    }

                    // Lemon's note: Should we ack and then remove from the queue? What happens in the event
                    //  of a failure in ack(), the tuple will be removed from the queue despite a failed ack

                    // Ack anything that needs to be acked
                    while (!ackedTupleInputQueue.get(spout.getConsumerId()).isEmpty()) {
                        TupleMessageId id = ackedTupleInputQueue.get(spout.getConsumerId()).poll();
                        spout.ack(id);
                    }

                    // Fail anything that needs to be failed
                    while (!failedTupleInputQueue.get(spout.getConsumerId()).isEmpty()) {
                        TupleMessageId id = failedTupleInputQueue.get(spout.getConsumerId()).poll();
                        spout.fail(id);
                    }

                    // Periodically we flush the state of the spout to capture progress
                    if (lastFlush + FLUSH_INTERVAL_MS < clock.millis()) {
                        logger.info("Flushing state for spout {}", spout.getConsumerId());
                        spout.flushState();
                        lastFlush = clock.millis();
                    }
                }

                // Looks like someone requested that we stop this instance.
                // So we call close on it.
                logger.info("Finishing {} spout", spout.getConsumerId());
                spout.close();

                // Remove our entries from the acked and failed queue.
                tupleOutputQueue.removeVirtualSpoutId(spout.getConsumerId());
                ackedTupleInputQueue.remove(spout.getConsumerId());
                failedTupleInputQueue.remove(spout.getConsumerId());
            } catch (Exception ex) {
                // TODO: Should we restart the SpoutRunner?
                logger.error("SpoutRunner for {} threw an exception {}", spout.getConsumerId(), ex);
            }
        }

        public void requestStop() {
            this.spout.requestStop();
        }
    }
}
