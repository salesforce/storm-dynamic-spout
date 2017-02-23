package com.salesforce.storm.spout.sideline;

import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class SpoutCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(SpoutCoordinator.class);

    private static final int MONITOR_THREAD_SLEEP = 10;
    private static final int SPOUT_THREAD_SLEEP = 10;
    private static final int MAX_SPOUT_STOP_TIME = 5000;

    private boolean running = false;

    private final Queue<DelegateSidelineSpout> sidelineSpouts = new ConcurrentLinkedQueue<>();
    private final Map<String,DelegateSidelineSpout> runningSpouts = new ConcurrentHashMap<>();
    private final Map<String,Queue<TupleMessageId>> acked = new ConcurrentHashMap<>();
    private final Map<String,Queue<TupleMessageId>> failed = new ConcurrentHashMap<>();

    /**
     *
     * @param spout
     */
    public SpoutCoordinator(final DelegateSidelineSpout spout) {
        addSidelineSpout(spout);
    }

    /**
     *
     * @param spout
     */
    public void addSidelineSpout(final DelegateSidelineSpout spout) {
        sidelineSpouts.add(spout);
    }

    public int getTotalSpouts() {
        return runningSpouts.size();
    }

    /**
     *
     * @param consumer
     */
    public void start(final Consumer<KafkaMessage> consumer) {
        running = true;

        final CountDownLatch startSignal = new CountDownLatch(sidelineSpouts.size());

        CompletableFuture.runAsync(() -> {
            while (running) {
                for (DelegateSidelineSpout spout : sidelineSpouts) {
                    if (!runningSpouts.containsKey(spout.getConsumerId())) {
                        sidelineSpouts.remove(spout);

                        runningSpouts.put(spout.getConsumerId(), spout);

                        CompletableFuture.runAsync(() -> {
                            logger.info("Opening {} spout", spout.getConsumerId());

                            spout.open();

                            acked.put(spout.getConsumerId(), new ConcurrentLinkedQueue<>());
                            failed.put(spout.getConsumerId(), new ConcurrentLinkedQueue<>());

                            startSignal.countDown();

                            while (!spout.isFinished()) {
                                logger.debug("Requesting next tuple for spout {}", spout.getConsumerId());

                                final KafkaMessage message = spout.nextTuple();

                                if (message != null) {
                                    // Lambda that passes the tuple back to the main spout
                                    consumer.accept(message);
                                }

                                // Lemon's note: Should we ack and then remove from the queue? What happens in the event
                                //  of a failure in ack(), the tuple will be removed from the queue despite a failed ack

                                // Ack anything that needs to be acked
                                while (!acked.get(spout.getConsumerId()).isEmpty()) {
                                    TupleMessageId id = acked.get(spout.getConsumerId()).poll();
                                    spout.ack(id);
                                }

                                // Fail anything that needs to be failed
                                while (!acked.get(spout.getConsumerId()).isEmpty()) {
                                    TupleMessageId id = acked.get(spout.getConsumerId()).poll();
                                    spout.fail(id);
                                }

                                try {
                                    Thread.sleep(SPOUT_THREAD_SLEEP);
                                } catch (InterruptedException ex) {
                                    logger.warn("Thread interrupted, shutting down...");
                                    spout.finish();
                                }
                            }

                            logger.info("Finishing {} spout", spout.getConsumerId());

                            spout.close();

                            acked.remove(spout.getConsumerId());
                            failed.remove(spout.getConsumerId());
                        }).thenRun(() -> {
                            runningSpouts.remove(spout.getConsumerId());
                        });

                    }
                }

                try {
                    Thread.sleep(MONITOR_THREAD_SLEEP);
                } catch (InterruptedException ex) {
                    logger.warn("Thread interrupted, shutting down...");
                    //this.stop();
                }
            }
        });

        try {
            startSignal.await();
        } catch (InterruptedException ex) {
            logger.error("Exception while waiting for the coordinator to open it's spouts {}", ex);
        }
    }

    /**
     *
     * @param id
     */
    public void ack(final TupleMessageId id) {
        if (!acked.containsKey(id.getSrcConsumerId())) {
            logger.warn("Acking tuple for unknown consumer");
            return;
        }

        acked.get(id.getSrcConsumerId()).add(id);
    }

    /**
     *
     * @param id
     */
    public void fail(final TupleMessageId id) {
        if (!failed.containsKey(id.getSrcConsumerId())) {
            logger.warn("Failing tuple for unknown consumer");
            return;
        }

        failed.get(id.getSrcConsumerId()).add(id);
    }

    /**
     *
     */
    public void stop() {
        // Tell every spout to finish what they're doing
        for (DelegateSidelineSpout spout : runningSpouts.values()) {
            // Marking it as finished will cause the thread to end, remove it from the thread map
            // and ultimately remove it from the list of spouts
            spout.finish();
        }

        final Duration timeout = Duration.ofMillis(MAX_SPOUT_STOP_TIME);

        final ExecutorService executor = Executors.newSingleThreadExecutor();

        final Future handler = executor.submit(() -> {
            while (!runningSpouts.isEmpty()) {}
        });

        try {
            handler.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            handler.cancel(true);
        } catch (InterruptedException e) {
        } catch (ExecutionException e) {
        }

        executor.shutdownNow();

        // Will trigger the monitor thread to stop running, which should be the end of it
        running = false;
    }
}
