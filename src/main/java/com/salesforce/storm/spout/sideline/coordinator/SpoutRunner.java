package com.salesforce.storm.spout.sideline.coordinator;

import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.Tools;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;
import com.salesforce.storm.spout.sideline.tupleBuffer.TupleBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

/**
 * Manages running a VirtualSpout instance.
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
    private final TupleBuffer tupleQueue;

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

    /**
     * Records when this instance was started, so we can calculate total run time on close.
     */
    private final long startTime;

    /**
     * This flag is used to signal for this instance to cleanly stop.
     * Marked as volatile because currently its accessed via multiple threads.
     */
    private volatile boolean requestedStop = false;

    SpoutRunner(
            final DelegateSidelineSpout spout,
            final TupleBuffer tupleQueue,
            final Map<String, Queue<TupleMessageId>> ackedTupleQueue,
            final Map<String, Queue<TupleMessageId>> failedTupleInputQueue,
            final CountDownLatch latch,
            final Clock clock,
            final Map<String, Object> topologyConfig
    ) {
        this.spout = spout;
        this.tupleQueue = tupleQueue;
        this.ackedTupleQueue = ackedTupleQueue;
        this.failedTupleQueue = failedTupleInputQueue;
        this.latch = latch;
        this.clock = clock;
        this.topologyConfig = Tools.immutableCopy(topologyConfig);

        // Record start time.
        this.startTime = getClock().millis();
    }

    @Override
    public void run() {
        try {
            // Rename thread to use the spout's consumer id
            Thread.currentThread().setName(spout.getVirtualSpoutId());

            logger.info("Opening {} spout", spout.getVirtualSpoutId());
            spout.open();

            // Let all of our queues know about our new instance.
            tupleQueue.addVirtualSpoutId(spout.getVirtualSpoutId());
            ackedTupleQueue.put(spout.getVirtualSpoutId(), new ConcurrentLinkedQueue<>());
            failedTupleQueue.put(spout.getVirtualSpoutId(), new ConcurrentLinkedQueue<>());

            // Count down our latch for thread synchronization.
            latch.countDown();

            // Record the last time we flushed.
            long lastFlush = getClock().millis();

            // Loop forever until someone requests the spout to stop
            while (!isStopRequested() && !spout.isStopRequested() && !Thread.interrupted()) {
                // First look for any new tuples to be emitted.
                final KafkaMessage message = spout.nextTuple();
                if (message != null) {
                    try {
                        tupleQueue.put(message);
                    } catch (InterruptedException ex) {
                        logger.error("Shutting down due to interruption {}", ex);
                        spout.requestStop();
                    }
                }

                // Lemon's note: Should we ack and then remove from the queue? What happens in the event
                //  of a failure in ack(), the tuple will be removed from the queue despite a failed ack

                // Ack anything that needs to be acked
                while (!ackedTupleQueue.get(spout.getVirtualSpoutId()).isEmpty()) {
                    TupleMessageId id = ackedTupleQueue.get(spout.getVirtualSpoutId()).poll();
                    spout.ack(id);
                }

                // Fail anything that needs to be failed
                while (!failedTupleQueue.get(spout.getVirtualSpoutId()).isEmpty()) {
                    TupleMessageId id = failedTupleQueue.get(spout.getVirtualSpoutId()).poll();
                    spout.fail(id);
                }

                // Periodically we flush the state of the spout to capture progress
                final long now = getClock().millis();
                if ((lastFlush + getConsumerStateFlushIntervalMs()) < now) {
                    logger.debug("Flushing state for spout {}", spout.getVirtualSpoutId());
                    spout.flushState();
                    lastFlush = now;
                }
            }

            // Looks like someone requested that we stop this instance.
            // So we call close on it, and log our run time.
            final Duration runtime = Duration.ofMillis(getClock().millis() - getStartTime());
            logger.info("Closing {} spout, total run time was {}", spout.getVirtualSpoutId(), Tools.prettyDuration(runtime));
            spout.close();

            // Remove our entries from our queues.
            getTupleQueue().removeVirtualSpoutId(spout.getVirtualSpoutId());
            getAckedTupleQueue().remove(spout.getVirtualSpoutId());
            getFailedTupleQueue().remove(spout.getVirtualSpoutId());
        } catch (Exception ex) {
            // TODO: Should we restart the SpoutRunner?  I'd guess that SpoutMonitor should handle re-starting
            logger.error("SpoutRunner for {} threw an exception {}", spout.getVirtualSpoutId(), ex);
            ex.printStackTrace();

            // We re-throw the exception
            // SpoutMonitor should detect this failed.
            // TODO: DO we even want to bother catching this here?
            throw ex;
        }
    }

    /**
     * Call this method to request this SpoutRunner instance
     * to cleanly stop.
     *
     * Synchronized because this can be called from multiple threads.
     */
    public void requestStop() {
        logger.info("Requested stop");
        requestedStop = true;
    }

    /**
     * Determine if anyone has requested stop on this instance.
     *
     * @return - true if so, false if not.
     */
    public boolean isStopRequested() {
        return requestedStop;
    }

    /**
     * @return - our System clock instance.
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
     * @return - How frequently, in milliseconds, we should flush consumer state.
     */
    long getConsumerStateFlushIntervalMs() {
        return ((Number) getTopologyConfig().get(SidelineSpoutConfig.CONSUMER_STATE_FLUSH_INTERVAL_MS)).longValue();
    }

    DelegateSidelineSpout getSpout() {
        return spout;
    }

    Map<String, Queue<TupleMessageId>> getAckedTupleQueue() {
        return ackedTupleQueue;
    }

    Map<String, Queue<TupleMessageId>> getFailedTupleQueue() {
        return failedTupleQueue;
    }

    TupleBuffer getTupleQueue() {
        return tupleQueue;
    }

    CountDownLatch getLatch() {
        return latch;
    }

    long getStartTime() {
        return startTime;
    }
}
