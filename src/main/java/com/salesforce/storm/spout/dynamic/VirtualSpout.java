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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.consumer.Consumer;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerPeerContext;
import com.salesforce.storm.spout.dynamic.consumer.Record;
import com.salesforce.storm.spout.dynamic.filter.FilterChain;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerState;
import com.salesforce.storm.spout.dynamic.handler.VirtualSpoutHandler;
import com.salesforce.storm.spout.dynamic.metrics.SpoutMetrics;
import com.salesforce.storm.spout.dynamic.retry.RetryManager;
import com.salesforce.storm.spout.dynamic.metrics.MetricsRecorder;
import com.salesforce.storm.spout.dynamic.persistence.PersistenceAdapter;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * VirtualSpout is a Spout that exists within another Spout instance. It doesn't fully implement the
 * Storm IRichSpout interface because its not a 'real' spout, but it follows it fairly closely.
 * These instances are designed to live within a {@link DynamicSpout} instance. instance.  During the
 * lifetime of a {@link DynamicSpout}, many VirtualSpout instances can get created and destroyed.
 *
 * The VirtualSpout will use the configured {@link Consumer} to consume from a source and return
 * {@link Message} from its {@link #nextTuple()} method.  These will eventually get converted to the
 * appropriate Tuples and emitted out by the {@link DynamicSpout}.
 *
 * As acks/fails come into {@link DynamicSpout}, they will be routed to the appropriate VirtualSpout instance
 * and handled by the {@link #ack(Object)} and {@link #fail(Object)} methods.
 */
public class VirtualSpout implements DelegateSpout {
    // Logging
    private static final Logger logger = LoggerFactory.getLogger(VirtualSpout.class);

    /**
     * Holds reference to our topologyContext.
     */
    private final TopologyContext topologyContext;

    /**
     * Holds reference to our spout configuration.
     */
    private final SpoutConfig spoutConfig;

    /**
     * Our Factory Manager.
     */
    private final FactoryManager factoryManager;

    /**
     * Our underlying Consumer implementation.
     */
    private Consumer consumer;

    /**
     * Filter chain applied to messages for this virtual spout.
     */
    private final FilterChain filterChain = new FilterChain();

    /**
     * Starting point for the partitions for this spouts consumer.
     */
    private ConsumerState startingState = null;

    /**
     * Ending point for the partitions for this spouts consumer.
     */
    private ConsumerState endingState = null;

    /**
     * Flag to maintain if open() has been called already.
     */
    private boolean isOpened = false;

    /**
     * This flag is used to signal for this instance to cleanly stop.
     * Marked as volatile because currently its accessed via multiple threads.
     */
    private volatile boolean requestedStop = false;

    /**
     * This flag represents when we have finished consuming all that needs to be consumed.
     */
    private boolean isCompleted = false;

    /**
     * Is our unique VirtualSpoutId.
     */
    private VirtualSpoutIdentifier virtualSpoutId;

    /**
     * For tracking failed messages, and knowing when to replay them.
     */
    private RetryManager retryManager;
    private final Map<MessageId, Message> trackedMessages = Maps.newHashMap();

    /**
     * For metric reporting.
     */
    private final MetricsRecorder metricsRecorder;

    /**
     * Handler for callbacks at various stages of a virtual spout's lifecycle.
     */
    private VirtualSpoutHandler virtualSpoutHandler;

    /**
     * Create a new VirtualSpout instance.
     *
     * @param virtualSpoutId Identifier for this VirtualSpout instance.
     * @param spoutConfig Our topology config
     * @param topologyContext Our topology context
     * @param factoryManager FactoryManager instance.
     * @param metricsRecorder MetricsRecorder instance.
     * @param startingState Where the underlying consumer should start from, Null if start from head.
     * @param endingState Where the underlying consumer should stop processing.  Null if process forever.
     */
    public VirtualSpout(
        final VirtualSpoutIdentifier virtualSpoutId,
        final SpoutConfig spoutConfig,
        final TopologyContext topologyContext,
        final FactoryManager factoryManager,
        final MetricsRecorder metricsRecorder,
        final ConsumerState startingState,
        final ConsumerState endingState
    ) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(virtualSpoutId.toString()), "Consumer id cannot be null or empty!");
        Preconditions.checkNotNull(spoutConfig, "Spout configuration cannot be null!");
        Preconditions.checkNotNull(topologyContext, "Topology context cannot be null!");
        Preconditions.checkNotNull(factoryManager, "Factory manager cannot be null!");

        this.virtualSpoutId = virtualSpoutId;

        // Save reference to topology context
        this.topologyContext = topologyContext;

        // Save reference to SpoutConfig.
        this.spoutConfig = spoutConfig;

        // Save factory manager instance
        this.factoryManager = factoryManager;

        // Save metric recorder instance.
        this.metricsRecorder = metricsRecorder;

        // Save state
        this.startingState = startingState;
        this.endingState = endingState;
    }

    /**
     * Initializes the "Virtual Spout."
     */
    @Override
    public void open() {
        // Maintain state if this has been opened or not.
        if (isOpened) {
            throw new IllegalStateException("Cannot call open more than once!");
        }

        logger.info("Open for VirtualSpout {} has starting state {} and ending state {}", getVirtualSpoutId(), startingState, endingState);

        // Create our failed msg retry manager & open
        retryManager = getFactoryManager().createNewFailedMsgRetryManagerInstance();
        retryManager.open(getSpoutConfig());

        // Create underlying Consumer implementation - Normal Behavior is for this to be null here.
        // The only time this would be non-null would be if it was injected for tests.
        if (consumer == null) {
            // Create consumer from Factory Manager.
            consumer = getFactoryManager().createNewConsumerInstance();
        }

        // Create consumer dependencies
        final PersistenceAdapter persistenceAdapter = getFactoryManager().createNewPersistenceAdapterInstance();
        persistenceAdapter.open(getSpoutConfig());

        // Define consumer cohort definition.
        final ConsumerPeerContext consumerPeerContext = new ConsumerPeerContext(
            topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size(),
            topologyContext.getThisTaskIndex()
        );

        // Open consumer
        consumer.open(getSpoutConfig(), getVirtualSpoutId(), consumerPeerContext, persistenceAdapter, metricsRecorder, startingState);

        // This is an approximation, after the consumer has been opened since we were not provided with a starting state
        if (startingState == null) {
            logger.debug("I do not have a starting state, so I am setting it to the consumer's current state instead.");
            startingState = consumer.getCurrentState();
        }

        // Set state to true, once we've successfully opened everything up.  We do NOT block for the spout handlers.
        isOpened = true;

        virtualSpoutHandler = getFactoryManager().createVirtualSpoutHandler();
        virtualSpoutHandler.open(getSpoutConfig());
        virtualSpoutHandler.onVirtualSpoutOpen(this);
    }

    @Override
    public void close() {
        // If we've successfully completed processing
        if (isCompleted()) {
            // Let our handler know that the virtual spout has been completed
            virtualSpoutHandler.onVirtualSpoutCompletion(this);

            // We should clean up consumer state
            consumer.removeConsumerState();
        } else {
            // We are just closing up shop,
            // First flush our current consumer state.
            consumer.flushConsumerState();
        }
        // Call close & null reference.
        consumer.close();
        consumer = null;

        virtualSpoutHandler.onVirtualSpoutClose(this);
        virtualSpoutHandler.close();

        startingState = null;
        endingState = null;
    }

    /**
     * @return - The next Message that should be played into the topology.
     */
    @Override
    public Message nextTuple() {
        // Talk to a "failed tuple manager interface" object to see if any tuples
        // that failed previously are ready to be replayed.  This is an interface
        // meaning you can implement your own behavior here.  Maybe failed tuples never get replayed,
        // Maybe they get replayed a maximum number of times?  Maybe they get replayed forever but have
        // an exponential back off time period between fails?  Who knows/cares, not us cuz its an interface.
        // If so, emit that and return.
        final MessageId nextFailedMessageId = retryManager.nextFailedMessageToRetry();
        if (nextFailedMessageId != null) {
            if (trackedMessages.containsKey(nextFailedMessageId)) {
                // Emit the tuple.
                return trackedMessages.get(nextFailedMessageId);
            } else {
                logger.warn("Unable to find tuple that should be replayed due to a fail {}", nextFailedMessageId);
                retryManager.acked(nextFailedMessageId);
            }
        }

        // Grab the next message from Consumer instance.
        final Record record = consumer.nextRecord();
        if (record == null) {
            logger.debug("Unable to find any new messages from consumer");
            return null;
        }

        // Create a Tuple Message Id
        final MessageId messageId = new MessageId(record.getNamespace(), record.getPartition(), record.getOffset(), getVirtualSpoutId());

        // Determine if this tuple exceeds our ending offset
        if (doesMessageExceedEndingOffset(messageId)) {
            logger.debug("Tuple {} exceeds max offset, acking", messageId);

            // Unsubscribe partition this tuple belongs to.
            unsubscribeTopicPartition(messageId.getNamespace(), messageId.getPartition());

            // We don't need to ack the tuple because it never got emitted out.
            // Simply return null.
            return null;
        }

        // Create Message
        final Message message = new Message(messageId, record.getValues());

        // Determine if this tuple should be filtered. If it IS filtered, loop and find the next one?
        // Loops through each step in the chain to filter a filter before emitting
        final boolean isFiltered  = getFilterChain().filter(message);

        // Keep Track of the tuple in this spout somewhere so we can replay it if it happens to fail.
        if (isFiltered) {
            // Increment filtered metric
            getMetricsRecorder()
                .count(SpoutMetrics.VIRTUAL_SPOUT_FILTERED, getVirtualSpoutId().toString());

            // Ack
            ack(messageId);

            // return null.
            return null;
        }

        // Track it message for potential retries.
        trackedMessages.put(messageId, message);

        // Return it.
        return message;
    }

    /**
     * For the given MessageId, does it exceed any defined ending offsets?
     * @param messageId - The MessageId to check.
     * @return - Boolean - True if it does, false if it does not.
     */
    boolean doesMessageExceedEndingOffset(final MessageId messageId) {
        // If no end offsets defined
        if (endingState == null) {
            // Then this check is a no-op, return false
            return false;
        }

        final long currentOffset = messageId.getOffset();

        // Find ending offset for this namespace partition
        final Long endingOffset = endingState.getOffsetForNamespaceAndPartition(messageId.getNamespace(), messageId.getPartition());
        if (endingOffset == null) {
            // None defined?  Probably an error
            throw new IllegalStateException(
                "Consuming from a namespace/partition without a defined end offset? ["
                    + messageId.getNamespace() + "-" + messageId.getPartition() + "] not in (" + endingState + ")"
            );
        }

        // If its > the ending offset
        return currentOffset > endingOffset;
    }

    @Override
    public void ack(Object msgId) {
        if (msgId == null) {
            logger.warn("Null msg id passed, ignoring");
            return;
        }

        // Convert to MessageId
        final MessageId messageId;
        try {
            messageId = (MessageId) msgId;
        } catch (final ClassCastException e) {
            throw new IllegalArgumentException("Invalid msgId object type passed " + msgId.getClass());
        }

        // Talk to Consumer and mark the offset completed.
        consumer.commitOffset(messageId.getNamespace(), messageId.getPartition(), messageId.getOffset());

        // Remove this tuple from the spout where we track things in-case the tuple fails.
        trackedMessages.remove(messageId);

        // Mark it as completed in the failed message handler if it exists.
        retryManager.acked(messageId);

        // update ack metric
        getMetricsRecorder()
            .count(SpoutMetrics.VIRTUAL_SPOUT_ACK, getVirtualSpoutId().toString());
    }

    @Override
    public void fail(Object msgId) {
        if (msgId == null) {
            logger.warn("Null msg id passed, ignoring");
            return;
        }

        // Convert to MessageId
        final MessageId messageId;
        try {
            messageId = (MessageId) msgId;
        } catch (final ClassCastException e) {
            throw new IllegalArgumentException("Invalid msgId object type passed " + msgId.getClass());
        }

        // If this tuple shouldn't be replayed again
        if (!retryManager.retryFurther(messageId)) {
            logger.warn("Not retrying failed msgId any further {}", messageId);

            // Mark it as acked in retryManager
            retryManager.acked(messageId);

            // Ack it in the consumer
            consumer.commitOffset(messageId.getNamespace(), messageId.getPartition(), messageId.getOffset());

            // Update metric
            getMetricsRecorder()
                .count(SpoutMetrics.VIRTUAL_SPOUT_EXCEEDED_RETRY_LIMIT, getVirtualSpoutId().toString());

            // Done.
            return;
        }

        // Otherwise mark it as failed.
        retryManager.failed(messageId);

        // Update metric
        getMetricsRecorder()
            .count(SpoutMetrics.VIRTUAL_SPOUT_FAIL, getVirtualSpoutId().toString());
    }

    /**
     * Call this method to request this {@link VirtualSpout} instance
     * to cleanly stop.
     *
     * Synchronized because this can be called from multiple threads.
     */
    public void requestStop() {
        synchronized (this) {
            requestedStop = true;
        }
    }

    /**
     * Determine if anyone has requested stop on this instance.
     * Synchronized because this can be called from multiple threads.
     *
     * @return - true if so, false if not.
     */
    public boolean isStopRequested() {
        synchronized (this) {
            return requestedStop || Thread.interrupted();
        }
    }

    /**
     * This this method to determine if the spout was marked as 'completed'.
     * We define 'completed' meaning it reached its ending state.
     * @return - True if 'completed', false if not.
     */
    private boolean isCompleted() {
        return isCompleted;
    }

    /**
     * Mark this spout as 'completed.'
     * We define 'completed' meaning it reached its ending state.
     */
    private void setCompleted() {
        isCompleted = true;
    }

    /**
     * @return - Return this instance's unique virtual spout it.
     */
    @Override
    public VirtualSpoutIdentifier getVirtualSpoutId() {
        return virtualSpoutId;
    }

    @Override
    public FilterChain getFilterChain() {
        return filterChain;
    }

    /**
     * Get the spout's current consumer state.
     * @return current consumer state
     */
    @Override
    public ConsumerState getCurrentState() {
        // This could happen is someone tries calling this method before the vspout is opened
        if (consumer == null) {
            return null;
        }
        return consumer.getCurrentState();
    }

    @Override
    public ConsumerState getStartingState() {
        return startingState;
    }

    @Override
    public ConsumerState getEndingState() {
        return endingState;
    }

    /**
     * Set the ending state of the {@link DelegateSpout}.for when it should be marked as complete.
     *
     * @param endingState ending consumer state for when the {@link DelegateSpout} should be marked as complete.
     */
    @Override
    public void setEndingState(final ConsumerState endingState) {
        this.endingState = endingState;
    }

    /**
     * @return SpoutConfig instance.
     */
    SpoutConfig getSpoutConfig() {
        return spoutConfig;
    }

    public TopologyContext getTopologyContext() {
        return topologyContext;
    }

    /**
     * Access the FactoryManager for creating various configurable classes.
     * @return factory manager instance.
     */
    FactoryManager getFactoryManager() {
        return factoryManager;
    }

    /**
     * Get the consumer instance configured and used by the virtual spout.
     * @return Consumer instance.
     */
    public Consumer getConsumer() {
        return consumer;
    }

    /**
     * Unsubscribes the underlying consumer from the specified namespace/partition.
     *
     * @param namespace the namespace to unsubscribe from.
     * @param partition the partition to unsubscribe from.
     * @return boolean true if successfully unsubscribed, false if not.
     */
    public boolean unsubscribeTopicPartition(final String namespace, final int partition) {
        final boolean result = consumer.unsubscribeConsumerPartition(new ConsumerPartition(namespace, partition));
        if (result) {
            logger.info("Unsubscribed from partition [{}-{}]", namespace, partition);
        }
        return result;
    }

    /**
     * @return configured metric record instance.
     */
    MetricsRecorder getMetricsRecorder() {
        return metricsRecorder;
    }

    /**
     * Our maintenance loop.
     */
    @Override
    public void flushState() {
        // Flush consumer state to our persistence layer.
        consumer.flushConsumerState();

        // See if we can finish and close out this consumer.
        attemptToComplete();
    }

    /**
     * Internal method that determines if this consumer is finished.
     */
    private void attemptToComplete() {
        // If we don't have a defined ending state
        if (endingState == null) {
            // Then we never finished.
            return;
        }

        // If we're still tracking msgs
        if (!trackedMessages.isEmpty()) {
            // We cannot finish.
            return;
        }

        // Get current state and compare it against our ending state
        final ConsumerState currentState = consumer.getCurrentState();

        // Compare it against our ending state
        for (final ConsumerPartition consumerPartition: currentState.getConsumerPartitions()) {
            // currentOffset contains the last "committed" offset our consumer has fully processed
            final long currentOffset = currentState.getOffsetForNamespaceAndPartition(consumerPartition);

            // endingOffset contains the last offset we want to process.
            final long endingOffset = endingState.getOffsetForNamespaceAndPartition(consumerPartition);

            // If the current offset is < ending offset
            if (currentOffset < endingOffset) {
                // Then we cannot end
                return;
            }
            // Log that this partition is finished, and make sure we unsubscribe from it.
            if (consumer.unsubscribeConsumerPartition(consumerPartition)) {
                logger.debug(
                    "On {} Current Offset: {}  Ending Offset: {} (This partition is completed!)",
                    consumerPartition,
                    currentOffset,
                    endingOffset
                );
            }
        }

        // If we made it all the way through the above loop, we completed!
        // Lets flip our flag to true.
        logger.info("Looks like all partitions are complete!  Lets wrap this up.");
        setCompleted();

        // Cleanup consumerState.
        // Request that we stop.
        requestStop();
    }
}
