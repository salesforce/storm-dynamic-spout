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

package com.salesforce.storm.spout.dynamic;

import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.coordinator.SpoutMonitor;
import com.salesforce.storm.spout.dynamic.coordinator.SpoutMonitorFactory;
import com.salesforce.storm.spout.dynamic.exception.SpoutAlreadyExistsException;
import com.salesforce.storm.spout.dynamic.exception.SpoutDoesNotExistException;
import com.salesforce.storm.spout.dynamic.metrics.MetricsRecorder;
import com.salesforce.storm.spout.dynamic.buffer.MessageBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.Optional;
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
public class Coordinator implements SpoutCoordinator, VirtualSpoutCoordinator {
    // Logging.
    private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);

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
     * Buffer for errors that need to be reported.
     */
    private final Queue<Throwable> reportedErrorsQueue = new ConcurrentLinkedQueue<>();

    /**
     * For capturing metrics.
     */
    private final MetricsRecorder metricsRecorder;

    /**
     * Thread Pool Executor.
     */
    private ExecutorService executor;

    /**
     * SpoutMonitorFactory for creating new SpoutMonitors.
     */
    private final SpoutMonitorFactory spoutMonitorFactory;

    /**
     * The spout monitor runnable, which handles spinning up threads for virtual spouts.
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
    public Coordinator(final MetricsRecorder metricsRecorder, final MessageBuffer messageBuffer) {
        this(metricsRecorder, messageBuffer, new SpoutMonitorFactory());
    }

    /**
     * Constructor used for injecting a mock SpoutMonitorFactory instance.
     * @param metricsRecorder Recorder for capturing metrics
     * @param messageBuffer Buffer for messages from consumers on the various virtual spouts
     * @param spoutMonitorFactory A Factory for creating SpoutMonitors.
     */
    Coordinator(
        final MetricsRecorder metricsRecorder,
        final MessageBuffer messageBuffer,
        final SpoutMonitorFactory spoutMonitorFactory
    ) {

        this.metricsRecorder = metricsRecorder;
        this.messageBuffer = messageBuffer;
        this.spoutMonitorFactory = spoutMonitorFactory;
    }

    /**
     * Add a new VirtualSpout to the coordinator, this will get picked up by the coordinator's monitor, opened and
     * managed with teh other currently running spouts.
     * @param spout New delegate spout
     * @throws SpoutAlreadyExistsException if a spout already exists with the same VirtualSpoutIdentifier.
     */
    @Override
    public void addVirtualSpout(final DelegateSpout spout) throws SpoutAlreadyExistsException  {
        if (hasVirtualSpout(spout.getVirtualSpoutId())) {
            throw new SpoutAlreadyExistsException(
                "A spout with id " + spout.getVirtualSpoutId() + " already exists in the spout coordinator!",
                spout
            );
        }
        getNewSpoutQueue().add(spout);
    }

    /**
     * Remove a new VirtualSpout from the coordinator. This will signal to the monitor to request that the VirtualSpout
     * be stopped and ultimately removed.
     *
     * This method will blocked until the VirtualSpout has completely stopped.
     *
     * @param virtualSpoutIdentifier identifier of the VirtualSpout to be removed.
     * @throws SpoutDoesNotExistException If no VirtualSpout exists with the VirtualSpoutIdentifier.
     */
    @Override
    public void removeVirtualSpout(final VirtualSpoutIdentifier virtualSpoutIdentifier) throws SpoutDoesNotExistException {
        if (!hasVirtualSpout(virtualSpoutIdentifier)) {
            throw new SpoutDoesNotExistException(
                "A spout with id " + virtualSpoutIdentifier + " does not exist in the spout coordinator!",
                virtualSpoutIdentifier
            );
        }

        getSpoutMonitor().removeVirtualSpout(virtualSpoutIdentifier);

        // Let's block until we no longer detect that the spout is in monitor.
        while (getSpoutMonitor().hasVirtualSpout(virtualSpoutIdentifier)) {
            logger.info("Checking for VirtualSpout {} to see if it has finished stopping.", virtualSpoutIdentifier);

            try {
                Thread.sleep(1000L);
            } catch (InterruptedException ex) {
                logger.error(
                    "Something went wrong pausing between checking for VirtualSpout {} to stop. {}",
                    virtualSpoutIdentifier,
                    ex
                );
            }
        }

        logger.info("VirtualSpout {} is no longer running.", virtualSpoutIdentifier);
    }

    /**
     * Check if a given spout already exists in the spout coordinator.
     * @param spoutIdentifier spout identifier to check the coordinator for.
     * @return true when the spout exists, false when it does not.
     */
    @Override
    public boolean hasVirtualSpout(final VirtualSpoutIdentifier spoutIdentifier) {
        if (!isOpen) {
            throw new IllegalStateException("You cannot check for a spout in the coordinator before it has been opened!");
        }
        return getSpoutMonitor().hasVirtualSpout(spoutIdentifier);
    }

    /**
     * Open the coordinator and begin spinning up virtual spout threads.
     * @param config topology configuration.
     */
    @Override
    public void open(final Map<String, Object> config) {
        if (isOpen) {
            logger.warn("Coordinator is already opened, refusing to open again!");
            return;
        }

        // Create copy of topology config
        this.topologyConfig = Tools.immutableCopy(config);

        // Create a countdown latch
        // TODO I think this latch is now not needed, as at open time, nothing can be in the queue yet.
        // TODO we should remove it as its just extra clutter being passed all over.
        final CountDownLatch latch = new CountDownLatch(getNewSpoutQueue().size());

        // Create new single threaded executor.
        this.executor = Executors.newSingleThreadExecutor();

        // Create our spout monitor instance.
        spoutMonitor = getSpoutMonitorFactory().create(
            getNewSpoutQueue(),
            (VirtualSpoutCoordinator) this,
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
        } catch (final InterruptedException ex) {
            logger.error("Exception while waiting for the coordinator to open it's spouts {}", ex.getMessage(), ex);
        }
    }

    /**
     * This starts up the Spout Monitor thread.
     * It also handles if it dies un-naturally and re-starts it.
     */
    private void startSpoutMonitor() {
        CompletableFuture.runAsync(getSpoutMonitor(), getExecutor()).exceptionally((exception) -> {
            // This fires if SpoutMonitor dies because it threw an unhandled exception.

            // On errors, we need to restart it.  We throttle restarts @ 10 seconds to prevent thrashing.
            logger.error("Spout monitor died unnaturally.  Will restart after 10 seconds. {}", exception.getMessage(), exception);
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(10));
            } catch (final InterruptedException interruptedException) {
                logger.error("Caught InterruptedException, Will not restart Spout Monitor.");
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
    @Override
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
    @Override
    public void fail(final MessageId id) {
        if (!getFailedTuplesQueue().containsKey(id.getSrcVirtualSpoutId())) {
            logger.warn("Failing tuple for unknown consumer");
            return;
        }

        getFailedTuplesQueue().get(id.getSrcVirtualSpoutId()).add(id);
    }

    /**
     * @return Returns the next available Message to be emitted into the topology.
     */
    @Override
    public Optional<Message> nextMessage() {
        return Optional.ofNullable(getMessageBuffer().poll());
    }

    /**
     * @return Returns any errors that should be reported up to the topology.
     */
    @Override
    public Optional<Throwable> getErrors() {
        // Poll is non-blocking.
        return Optional.ofNullable(reportedErrorsQueue.poll());
    }

    /**
     * Stop managed spouts, calling this should shut down and finish the coordinator's spouts.
     */
    @Override
    public void close() {
        try {
            // Call shutdown, which prevents the executor from starting any new tasks.
            getExecutor().shutdown();

            // Call close on the spout monitor
            if (getSpoutMonitor() != null) {
                getSpoutMonitor().close();
            }

            // Wait for clean termination
            getExecutor().awaitTermination(getMaxTerminationWaitTimeMs(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException interruptedException) {
            logger.error("Interrupted clean shutdown, forcing stop: {}", interruptedException.getMessage(), interruptedException);
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
        return getSpoutMonitor().getTotalSpouts();
    }

    /**
     * @return MessageBuffer instance.
     */
    MessageBuffer getMessageBuffer() {
        return messageBuffer;
    }

    /**
     * @return The acked tuples queues.
     */
    Map<VirtualSpoutIdentifier, Queue<MessageId>> getAckedTuplesQueue() {
        return ackedTuplesQueue;
    }

    /**
     * @return The failed tuples queues.
     */
    Map<VirtualSpoutIdentifier, Queue<MessageId>> getFailedTuplesQueue() {
        return failedTuplesQueue;
    }

    /**
     * @return The new virtual spout instance queue.
     */
    Queue<DelegateSpout> getNewSpoutQueue() {
        return newSpoutQueue;
    }

    /**
     * @return The reported errors queue.
     */
    Queue<Throwable> getReportedErrorsQueue() {
        return reportedErrorsQueue;
    }

    /**
     * @return Clock instance, used for get local system time.
     */
    Clock getClock() {
        return clock;
    }

    /**
     * @return The topology configuration map.
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
     * @return Factory for creating SpoutMonitors.
     */
    private SpoutMonitorFactory getSpoutMonitorFactory() {
        return spoutMonitorFactory;
    }

    /**
     * @return The spout monitor runnable, which handles spinning up threads for virtual spouts.
     */
    private SpoutMonitor getSpoutMonitor() {
        return spoutMonitor;
    }

    /**
     * @return The maximum amount of time we'll wait for spouts to terminate before forcing them to stop, in milliseconds.
     */
    private long getMaxTerminationWaitTimeMs() {
        return ((Number) getTopologyConfig().get(SpoutConfig.MAX_SPOUT_SHUTDOWN_TIME_MS)).longValue();
    }

    /**
     * @return Our internal executor service.
     */
    ExecutorService getExecutor() {
        return executor;
    }

    @Override
    public void registerVirtualSpout(final VirtualSpoutIdentifier virtualSpoutIdentifier) {
        messageBuffer.addVirtualSpoutId(virtualSpoutIdentifier);
        ackedTuplesQueue.put(virtualSpoutIdentifier, new ConcurrentLinkedQueue<>());
        failedTuplesQueue.put(virtualSpoutIdentifier, new ConcurrentLinkedQueue<>());
    }

    @Override
    public void publishMessage(final Message message) throws InterruptedException {
        messageBuffer.put(message);
    }

    @Override
    public int messageSize() {
        return messageBuffer.size();
    }

    @Override
    public void publishError(final Throwable throwable) {
        reportedErrorsQueue.add(throwable);
    }

    @Override
    public Optional<MessageId> getAckedMessage(final VirtualSpoutIdentifier virtualSpoutIdentifier) {
        final MessageId id = ackedTuplesQueue.get(virtualSpoutIdentifier).poll();
        return Optional.ofNullable(id);
    }

    @Override
    public Optional<MessageId> getFailedMessage(final VirtualSpoutIdentifier virtualSpoutIdentifier) {
        final MessageId id = failedTuplesQueue.get(virtualSpoutIdentifier).poll();
        return Optional.ofNullable(id);
    }

    @Override
    public void unregisterVirtualSpout(final VirtualSpoutIdentifier virtualSpoutIdentifier) {
        messageBuffer.removeVirtualSpoutId(virtualSpoutIdentifier);
        ackedTuplesQueue.remove(virtualSpoutIdentifier);
        failedTuplesQueue.remove(virtualSpoutIdentifier);
    }
}
