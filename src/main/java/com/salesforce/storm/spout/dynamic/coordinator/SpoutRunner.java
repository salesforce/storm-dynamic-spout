/**
 * Copyright (c) 2017, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 *   disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * * Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or promote products
 *   derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.salesforce.storm.spout.dynamic.coordinator;

import com.salesforce.storm.spout.dynamic.Message;
import com.salesforce.storm.spout.dynamic.Tools;
import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.VirtualSpoutCoordinator;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.DelegateSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 * Manages running a VirtualSpout instance.
 * It handles all of the cross-thread communication via its Concurrent Queues data structures.
 */
public class SpoutRunner implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SpoutRunner.class);

    /**
     * This is the {@link com.salesforce.storm.spout.dynamic.VirtualSpout} instance we are going to be managing.
     */
    private final DelegateSpout spout;

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

    private final VirtualSpoutCoordinator virtualSpoutCoordinator;

    SpoutRunner(
        final DelegateSpout spout,
        final VirtualSpoutCoordinator virtualSpoutCoordinator,
        final CountDownLatch latch,
        final Clock clock,
        final Map<String, Object> topologyConfig
    ) {
        this.spout = spout;
        this.virtualSpoutCoordinator = virtualSpoutCoordinator;
        this.latch = latch;
        this.clock = clock;
        this.topologyConfig = Tools.immutableCopy(topologyConfig);

        // Record start time.
        this.startTime = getClock().millis();
    }

    @Override
    public void run() {
        try {
            final VirtualSpoutIdentifier virtualSpoutId = spout.getVirtualSpoutId();

            // Rename thread to use the spout's consumer id
            Thread.currentThread().setName(virtualSpoutId.toString());

            logger.info("Opening {} spout", virtualSpoutId);
            spout.open();

            // Let all of our queues know about our new instance.
            virtualSpoutCoordinator.registerVirtualSpout(virtualSpoutId);

            // Count down our latch for thread synchronization.
            latch.countDown();

            // Record the last time we flushed.
            long lastFlush = getClock().millis();

            // Loop forever until someone requests the spout to stop
            while (!isStopRequested() && !spout.isStopRequested() && !Thread.interrupted()) {
                // First look for any new tuples to be emitted.
                final Message message = spout.nextTuple();
                if (message != null) {
                    try {
                        virtualSpoutCoordinator.publishMessage(message);
                    } catch (final InterruptedException interruptedException) {
                        logger.error("Shutting down due to interruption {}", interruptedException.getMessage(), interruptedException);
                        spout.requestStop();
                    }
                }

                // Lemon's note: Should we ack and then remove from the queue? What happens in the event
                //  of a failure in ack(), the tuple will be removed from the queue despite a failed ack

                // Ack anything that needs to be acked
                Optional<MessageId> messageId;
                while ((messageId = virtualSpoutCoordinator.getAckedMessage(virtualSpoutId)).isPresent()) {
                    spout.ack(messageId.get());
                }

                // Fail anything that needs to be failed
                while ((messageId = virtualSpoutCoordinator.getFailedMessage(virtualSpoutId)).isPresent()) {
                    spout.fail(messageId);
                }

                // Periodically we flush the state of the spout to capture progress
                final long now = getClock().millis();
                if ((lastFlush + getConsumerStateFlushIntervalMs()) < now) {
                    logger.debug("Flushing state for spout {}", virtualSpoutId);
                    spout.flushState();
                    lastFlush = now;
                }
            }

            // Looks like someone requested that we stop this instance.
            // So we call close on it, and log our run time.
            final Duration runtime = Duration.ofMillis(getClock().millis() - getStartTime());
            logger.info("Closing {} spout, total run time was {}", virtualSpoutId, Tools.prettyDuration(runtime));
            spout.close();

            // Remove our entries from our queues.
            virtualSpoutCoordinator.unregisterVirtualSpout(virtualSpoutId);
        } catch (final Exception ex) {
            // We don't handle restarting this instance.  Instead its Spout Monitor which that ownership falls to.
            // We'll log the error, and bubble up the exception.
            logger.error("SpoutRunner for {} threw an exception {}", spout.getVirtualSpoutId(), ex.getMessage(), ex);

            // We re-throw the exception, SpoutMonitor will handle this.
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
        return ((Number) getTopologyConfig().get(SpoutConfig.CONSUMER_STATE_FLUSH_INTERVAL_MS)).longValue();
    }

    DelegateSpout getSpout() {
        return spout;
    }

    CountDownLatch getLatch() {
        return latch;
    }

    long getStartTime() {
        return startTime;
    }
}
