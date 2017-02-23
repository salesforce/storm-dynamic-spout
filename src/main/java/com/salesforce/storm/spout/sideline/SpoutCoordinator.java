package com.salesforce.storm.spout.sideline;

import com.google.common.collect.Iterables;
import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class SpoutCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(SpoutCoordinator.class);
    private static final int MONITOR_THREAD_SLEEP = 10;
    private static final int SPOUT_THREAD_SLEEP = 10;
    private final Queue<DelegateSidelineSpout> sidelineSpouts = new ConcurrentLinkedQueue<>();
    private final ConcurrentMap<String,Thread> sidelineSpoutThreads = new ConcurrentHashMap<>();

    private final ConcurrentMap<String,Queue<TupleMessageId>> acked = new ConcurrentHashMap<>();
    private final ConcurrentMap<String,Queue<TupleMessageId>> failed = new ConcurrentHashMap<>();

    public SpoutCoordinator(DelegateSidelineSpout fireHoseSpout) {
        addSidelineSpout(fireHoseSpout);
    }

    public void addSidelineSpout(DelegateSidelineSpout spout) {
        sidelineSpouts.add(spout);
    }

    public void start(CountDownLatch openSignal, Consumer<KafkaMessage> consumer) {
        final Thread spoutMonitorThread = new Thread(() -> {

            for (DelegateSidelineSpout spout : Iterables.cycle(sidelineSpouts)) {
                if (!sidelineSpoutThreads.containsKey(spout.getConsumerId())) {
                    Thread spoutThread = new Thread(() -> {
                        spout.open();

                        openSignal.countDown();

                        acked.put(spout.getConsumerId(), new ConcurrentLinkedQueue<>());
                        failed.put(spout.getConsumerId(), new ConcurrentLinkedQueue<>());

                        while (!spout.isFinished()) {
                            KafkaMessage message = spout.nextTuple();

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

                        spout.close();

                        acked.remove(spout.getConsumerId());
                        failed.remove(spout.getConsumerId());

                        // When the thread returns it's shutting down, so we remove it from our map
                        sidelineSpoutThreads.remove(spout.getConsumerId());
                        // No more thread, no more spout
                        sidelineSpouts.remove(spout);
                    });

                    sidelineSpoutThreads.put(spout.getConsumerId(), spoutThread);

                    spoutThread.start();
                }
            }

            try {
                Thread.sleep(MONITOR_THREAD_SLEEP);
            } catch (InterruptedException ex) {
                logger.warn("Thread interrupted, shutting down...");
                this.stop();
            }
        });
        spoutMonitorThread.start();
    }

    public void ack(TupleMessageId id) {
        if (!acked.containsKey(id.getSrcConsumerId())) {
            logger.warn("Acking tuple for unknown consumer");
            return;
        }

        acked.get(id.getSrcConsumerId()).add(id);
    }

    public void fail(TupleMessageId id) {
        if (!failed.containsKey(id.getSrcConsumerId())) {
            logger.warn("Failing tuple for unknown consumer");
            return;
        }

        failed.get(id.getSrcConsumerId()).add(id);
    }

    public void stop() {
        for (DelegateSidelineSpout spout : sidelineSpouts) {
            // Marking it as finished will cause the thread to end, remove it from the thread map
            // and ultimately remove it from the list of spouts
            spout.finish();
        }
    }
}
