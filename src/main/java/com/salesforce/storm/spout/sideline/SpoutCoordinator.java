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
package com.salesforce.storm.spout.sideline;

import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.coordinator.SpoutMonitor;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.buffer.MessageBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
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
     * Track whether or not the coordinator has been opened.
     */
    private boolean isOpen = false;

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
    private final Queue<DelegateSpout> newSpoutQueue = new ConcurrentLinkedQueue<>();

    /**
     * Queue for tuples that are ready to be emitted out into the topology.
     */
    private final MessageBuffer messageBuffer;

    /**
     * Buffer by spout consumer id of messages that have been acked.
     */
    private final Map<VirtualSpoutIdentifier, Queue<MessageId>> ackedTuplesQueue = new ConcurrentHashMap<>();

    /**
     * Buffer by spout consumer id of messages that have been failed.
     */
    private final Map<VirtualSpoutIdentifier, Queue<MessageId>> failedTuplesQueue = new ConcurrentHashMap<>();

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
     * @param metricsRecorder Recorder for capturing metrics
     * @param messageBuffer Buffer for messages from consumers on the various virtual spouts
     */
    public SpoutCoordinator(MetricsRecorder metricsRecorder, MessageBuffer messageBuffer) {
        this.metricsRecorder = metricsRecorder;
        this.messageBuffer = messageBuffer;
    }

    /**
     * Add a new spout to the coordinator, this will get picked up by the coordinator's monitor, opened and
     * managed with teh other currently running spouts.
     * @param spout New delegate spout
     */
    public void addVirtualSpout(final DelegateSpout spout) {
        if (!isOpen) {
            throw new IllegalStateException("You cannot add a spout to the coordinator before it has been opened!");
        }
        if (spoutMonitor.hasSpout(spout.getVirtualSpoutId())) {
            throw new IllegalStateException("A spout with id " + spout.getVirtualSpoutId() + " already exists in the spout coordinator!");
        }
        getNewSpoutQueue().add(spout);
    }

    /**
     * Open the coordinator and begin spinning up virtual spout threads.
     */
    public void open(Map<String, Object> topologyConfig) {
        if (isOpen) {
            logger.warn("SpoutCoordinator is already opened, refusing to open again!");
            return;
        }

        // Create copy of topology config
        this.topologyConfig = Tools.immutableCopy(topologyConfig);

        // Create a countdown latch
        final CountDownLatch latch = new CountDownLatch(getNewSpoutQueue().size());

        // Create new single threaded executor.
        this.executor = Executors.newSingleThreadExecutor();

        // Create our spout monitor instance.
        spoutMonitor = new SpoutMonitor(
            getNewSpoutQueue(),
            getMessageBuffer(),
            getAckedTuplesQueue(),
            getFailedTuplesQueue(),
            latch,
            getClock(),
            getTopologyConfig(),
            getMetricsRecorder()
        );

        // Start executing the spout monitor in a new thread.
        startSpoutMonitor();

        // Block/wait for all of our VirtualSpout instances to start before continuing on.
        try {
            latch.await();

            isOpen = true;
        } catch (InterruptedException ex) {
            logger.error("Exception while waiting for the coordinator to open it's spouts {}", ex);
        }
    }

    private void startSpoutMonitor() {
        CompletableFuture.runAsync(spoutMonitor, getExecutor()).exceptionally((t) -> {
            // On errors, we need to restart it.  We throttle restarts @ 10 seconds to prevent thrashing.
            logger.error("Spout monitor died unnaturally.  Will restart after 10 seconds. {}", t.getMessage(), t);
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                logger.error("Caught InterruptedException, Will not restart SpouMonitor.");
                return null;
            }
            logger.info("Restarting SpoutMonitor");
            startSpoutMonitor();
            return null;
        });
    }

    /**
     * Acks a tuple on the spout that it belongs to.
     * @param id Tuple message id to ack
     */
    public void ack(final MessageId id) {
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
    public void fail(final MessageId id) {
        if (!getFailedTuplesQueue().containsKey(id.getSrcVirtualSpoutId())) {
            logger.warn("Failing tuple for unknown consumer");
            return;
        }

        getFailedTuplesQueue().get(id.getSrcVirtualSpoutId()).add(id);
    }

    /**
     * @return - Returns the next available Message to be emitted into the topology.
     */
    public Message nextMessage() {
        return getMessageBuffer().poll();
    }

    /**
     * Stop managed spouts, calling this should shut down and finish the coordinator's spouts.
     */
    public void close() {
        try {
            // Call shutdown, which prevents the executor from starting any new tasks.
            getExecutor().shutdown();

            // Call close on the spout monitor
            spoutMonitor.close();

            // Wait for clean termination
            getExecutor().awaitTermination(getMaxTerminationWaitTimeMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            logger.error("Interrupted clean shutdown, forcing stop: {}", ex);
        }

        // If we haven't shut down yet..
        if (!getExecutor().isTerminated()) {
            logger.warn("Shutdown was not completed within {} ms, forcing stop of executor now", getMaxTerminationWaitTimeMs());
            getExecutor().shutdownNow();
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
     * @return MessageBuffer instance.
     */
    MessageBuffer getMessageBuffer() {
        return messageBuffer;
    }

    /**
     * @return - The acked tuples queues.
     */
    Map<VirtualSpoutIdentifier, Queue<MessageId>> getAckedTuplesQueue() {
        return ackedTuplesQueue;
    }

    /**
     * @return - The failed tuples queues.
     */
    Map<VirtualSpoutIdentifier, Queue<MessageId>> getFailedTuplesQueue() {
        return failedTuplesQueue;
    }

    /**
     * @return - The new virtual spout instance queue.
     */
    Queue<DelegateSpout> getNewSpoutQueue() {
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
     * @return Spout's metric recorder.
     */
    private MetricsRecorder getMetricsRecorder() {
        return metricsRecorder;
    }

    /**
     * @return - the maximum amount of time we'll wait for spouts to terminate before forcing them to stop, in milliseconds.
     */
    private long getMaxTerminationWaitTimeMs() {
        return ((Number) getTopologyConfig().get(SidelineSpoutConfig.MAX_SPOUT_SHUTDOWN_TIME_MS)).longValue();
    }

    /**
     * @return - our internal executor service.
     */
    ExecutorService getExecutor() {
        return executor;
    }
}
