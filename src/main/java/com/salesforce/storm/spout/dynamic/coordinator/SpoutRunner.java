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
import com.salesforce.storm.spout.dynamic.VirtualSpoutMessageBus;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.DelegateSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * Manages running a VirtualSpout instance.
 * It handles all of the cross-thread communication via its Concurrent Queues data structures.
 */
class SpoutRunner implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SpoutRunner.class);

    /**
     * This is the {@link com.salesforce.storm.spout.dynamic.VirtualSpout} instance we are going to be managing.
     */
    private final DelegateSpout spout;

    /**
     * ThreadSafe MessageBus for communicating between DynamicSpout and VirtualSpouts.
     */
    private final VirtualSpoutMessageBus virtualSpoutMessageBus;

    /**
     * For access to the system clock.
     */
    private final Clock clock;

    /**
     * Spout configuration.
     */
    private final AbstractConfig spoutConfig;

    /**
     * Records when this instance was started, so we can calculate total run time on close.
     */
    private final long startTime;

    /**
     * This flag is used to signal for this instance to cleanly stop.
     * Marked as volatile because currently its accessed via multiple threads.
     */
    private volatile boolean requestedStop = false;

    /**
     * Constructor that makes use of a count down latch.
     * Countdown latches are useful for orchestrating startup.
     *
     * @param spout The VirtualSpout instance to run.
     * @param virtualSpoutMessageBus The ThreadSafe message bus for communicating between DynamicSpout and VirtualSpout.
     * @param clock Clock instance.
     * @param spoutConfig Spout configuration.
     */
    SpoutRunner(
        final DelegateSpout spout,
        final VirtualSpoutMessageBus virtualSpoutMessageBus,
        final Clock clock,
        final AbstractConfig spoutConfig
    ) {
        this.spout = spout;
        this.virtualSpoutMessageBus = virtualSpoutMessageBus;
        this.clock = clock;
        this.spoutConfig = spoutConfig;

        // Record start time.
        this.startTime = getClock().millis();
    }

    @Override
    public void run() {
        try {
            final VirtualSpoutIdentifier virtualSpoutId = spout.getVirtualSpoutId();

            // Append VirtualSpoutId to thread name
            Thread.currentThread().setName(Thread.currentThread().getName() + virtualSpoutId.toString());

            logger.info("Opening {} spout", virtualSpoutId);
            spout.open();

            // Let all of our queues know about our new instance.
            getVirtualSpoutMessageBus().registerVirtualSpout(virtualSpoutId);

            // Record the last time we flushed.
            long lastFlush = getClock().millis();

            // Loop forever until someone requests the spout to stop
            while (!isStopRequested() && !spout.isStopRequested() && !Thread.interrupted()) {
                // First look for any new tuples to be emitted.
                final Message message = spout.nextTuple();
                if (message != null) {
                    try {
                        getVirtualSpoutMessageBus().publishMessage(message);
                    } catch (final InterruptedException interruptedException) {
                        logger.error("Shutting down due to interruption {}", interruptedException.getMessage(), interruptedException);
                        spout.requestStop();
                    }
                }

                // Lemon's note: Should we ack and then remove from the queue? What happens in the event
                //  of a failure in ack(), the tuple will be removed from the queue despite a failed ack

                // Ack everything that needs to be acked
                MessageId messageId;
                do {
                    messageId = getVirtualSpoutMessageBus().getAckedMessage(virtualSpoutId);
                    if (messageId != null) {
                        spout.ack(messageId);
                    }
                }
                while (messageId != null);

                // Fail everything that needs to be failed
                do {
                    messageId = getVirtualSpoutMessageBus().getFailedMessage(virtualSpoutId);
                    if (messageId != null) {
                        spout.fail(messageId);
                    }
                }
                while (messageId != null);

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
            getVirtualSpoutMessageBus().unregisterVirtualSpout(virtualSpoutId);
        } catch (final Exception ex) {
            // We don't handle restarting this instance.  Instead its Spout Monitor which that ownership falls to.
            // We'll log the error, and bubble up the exception.
            logger.error("SpoutRunner for {} threw an exception {}", spout.getVirtualSpoutId(), ex.getMessage(), ex);

            // We re-throw the exception, SpoutCoordinator will handle this.
            throw ex;
        }
    }

    /**
     * Call this method to request this SpoutRunner instance
     * to cleanly stop.
     */
    void requestStop() {
        logger.info("Requested stop on {}", spout.getVirtualSpoutId());
        requestedStop = true;
    }

    /**
     * Determine if anyone has requested stop on this instance.
     *
     * @return - true if so, false if not.
     */
    private boolean isStopRequested() {
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
    AbstractConfig getSpoutConfig() {
        return spoutConfig;
    }

    /**
     * @return ThreadSafe MessageBus for communicating between DynamicSpout and VirtualSpouts.
     */
    private VirtualSpoutMessageBus getVirtualSpoutMessageBus() {
        return virtualSpoutMessageBus;
    }

    /**
     * @return - How frequently, in milliseconds, we should flush consumer state.
     */
    long getConsumerStateFlushIntervalMs() {
        return getSpoutConfig().getLong(SpoutConfig.CONSUMER_STATE_FLUSH_INTERVAL_MS);
    }

    DelegateSpout getSpout() {
        return spout;
    }

    /**
     * @return Unix timestamp (in milliseconds) of when the instance was created.
     */
    long getStartTime() {
        return startTime;
    }
}
