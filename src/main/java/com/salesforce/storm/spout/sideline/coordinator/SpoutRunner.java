package com.salesforce.storm.spout.sideline.coordinator;

import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.SpoutCoordinator;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;
import com.salesforce.storm.spout.sideline.tupleBuffer.TupleBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class SpoutRunner implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SpoutRunner.class);

    private final DelegateSidelineSpout spout;
    private final TupleBuffer tupleOutputQueue;
    private final Map<String, Queue<TupleMessageId>> ackedTupleInputQueue;
    private final Map<String, Queue<TupleMessageId>> failedTupleInputQueue;
    private final CountDownLatch latch;
    private final Clock clock;

    SpoutRunner(
            final DelegateSidelineSpout spout,
            final TupleBuffer tupleOutputQueue,
            final Map<String, Queue<TupleMessageId>> ackedTupleInputQueue,
            final Map<String, Queue<TupleMessageId>> failedTupleInputQueue,
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
                final KafkaMessage message = spout.nextTuple();
                if (message != null) {
                    try {
                        tupleOutputQueue.put(message);
                    } catch (InterruptedException ex) {
                        logger.error("Shutting down due to interruption {}", ex);
                        spout.requestStop();
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
                if (lastFlush + SpoutCoordinator.FLUSH_INTERVAL_MS < clock.millis()) {
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
            ex.printStackTrace();

            // We re-throw the exception
            // SpoutMonitor should detect this failed.
            // TODO: DO we even want to bother catching this here?
            throw ex;
        }
    }

    public void requestStop() {
        this.spout.requestStop();
    }
}
