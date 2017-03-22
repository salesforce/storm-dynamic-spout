package com.salesforce.storm.spout.sideline.coordinator;

import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;
import com.salesforce.storm.spout.sideline.tupleBuffer.TupleBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

/**
 * This Runnable handles running a SidelineVirtualSpout within a separate thread.
 * It handles all of the cross-thread communication via its Concurrent Queues data structures.
 */
public class SpoutRunner implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SpoutRunner.class);

    /**
     * This is the VirtualSideline Spout instance we are going to be managing.
     */
    private final DelegateSidelineSpout spout;

    /**
     * This is the queue we put messages that need to be emitted out to the topology onto.
     */
    private final TupleBuffer tupleOutputQueue;

    /**
     * This is the queue we read tuples that need to be acked off of.
     */
    private final Map<String, Queue<TupleMessageId>> ackedTupleQueue;

    /**
     * This is the queue we read tuples that need to be failed off of.
     */
    private final Map<String, Queue<TupleMessageId>> failedTupleQueue;

    /**
     * For access to the system clock.
     */
    private final Clock clock;

    /**
     * For thread synchronization.
     */
    private final CountDownLatch latch;

    /**
     * Storm topology configuration.
     */
    private final Map<String, Object> topologyConfig;

    SpoutRunner(
            final DelegateSidelineSpout spout,
            final TupleBuffer tupleOutputQueue,
            final Map<String, Queue<TupleMessageId>> ackedTupleQueue,
            final Map<String, Queue<TupleMessageId>> failedTupleInputQueue,
            final CountDownLatch latch,
            final Clock clock,
            final Map<String, Object> topologyConfig
    ) {
        this.spout = spout;
        this.tupleOutputQueue = tupleOutputQueue;
        this.ackedTupleQueue = ackedTupleQueue;
        this.failedTupleQueue = failedTupleInputQueue;
        this.latch = latch;
        this.clock = clock;
        this.topologyConfig = Collections.unmodifiableMap(topologyConfig);
    }

    @Override
    public void run() {
        try {
            // Rename thread to use the spout's consumer id
            Thread.currentThread().setName(spout.getConsumerId());

            logger.info("Opening {} spout", spout.getConsumerId());
            spout.open();

            // Let all of our queues know about our new instance.
            tupleOutputQueue.addVirtualSpoutId(spout.getConsumerId());
            ackedTupleQueue.put(spout.getConsumerId(), new ConcurrentLinkedQueue<>());
            failedTupleQueue.put(spout.getConsumerId(), new ConcurrentLinkedQueue<>());

            // Count down our latch for thread synchronization.
            latch.countDown();

            // Record the last time we flushed.
            long lastFlush = getClock().millis();

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
                while (!ackedTupleQueue.get(spout.getConsumerId()).isEmpty()) {
                    TupleMessageId id = ackedTupleQueue.get(spout.getConsumerId()).poll();
                    spout.ack(id);
                }

                // Fail anything that needs to be failed
                while (!failedTupleQueue.get(spout.getConsumerId()).isEmpty()) {
                    TupleMessageId id = failedTupleQueue.get(spout.getConsumerId()).poll();
                    spout.fail(id);
                }

                // Periodically we flush the state of the spout to capture progress
                final long now = getClock().millis();
                if ((lastFlush + getConsumerStateFlushIntervalMs()) < now) {
                    logger.info("Flushing state for spout {}", spout.getConsumerId());
                    spout.flushState();
                    lastFlush = now;
                }
            }

            // Looks like someone requested that we stop this instance.
            // So we call close on it.
            logger.info("Closing {} spout", spout.getConsumerId());
            spout.close();

            // Remove our entries from our queues.
            tupleOutputQueue.removeVirtualSpoutId(spout.getConsumerId());
            ackedTupleQueue.remove(spout.getConsumerId());
            failedTupleQueue.remove(spout.getConsumerId());
        } catch (Exception ex) {
            // TODO: Should we restart the SpoutRunner?  I'd guess that SpoutMonitor should handle re-starting
            logger.error("SpoutRunner for {} threw an exception {}", spout.getConsumerId(), ex);
            ex.printStackTrace();

            // We re-throw the exception
            // SpoutMonitor should detect this failed.
            // TODO: DO we even want to bother catching this here?
            throw ex;
        }
    }

    /**
     * Request that our thread stop and shut down.
     */
    public void requestStop() {
        this.spout.requestStop();
    }

    /**
     * @return - our System clock instance.
     */
    private Clock getClock() {
        return clock;
    }

    /**
     * @return - Storm topology configuration.
     */
    private Map<String, Object> getTopologyConfig() {
        return topologyConfig;
    }

    /**
     * @return - How frequently, in milliseconds, we should flush consumer state.
     */
    private long getConsumerStateFlushIntervalMs() {
        return (long) getTopologyConfig().get(SidelineSpoutConfig.CONSUMER_STATE_FLUSH_INTERVAL_MS);
    }
}
