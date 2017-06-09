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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.consumer.Consumer;
import com.salesforce.storm.spout.sideline.consumer.ConsumerPeerContext;
import com.salesforce.storm.spout.sideline.consumer.Record;
import com.salesforce.storm.spout.sideline.filter.FilterChain;
import com.salesforce.storm.spout.sideline.consumer.ConsumerState;
import com.salesforce.storm.spout.sideline.retry.RetryManager;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * VirtualSidelineSpout is essentially a Spout instance within a Spout. It doesn't fully implement the
 * Storm IRichSpout interface because its not a 'real' spout, but it follows it fairly closely.
 * These instances are designed to live within a {@link com.salesforce.storm.spout.sideline.SidelineSpout}
 * instance.  During the lifetime of a SidelineSpout, many VirtualSidelineSpouts can get created and destroyed.
 *
 * The VirtualSidelineSpout will consume from a configured namespace and return {@link Message} from its
 * {@link #nextTuple()} method.  These will eventually get converted to the appropriate Tuples and emitted
 * out by the SidelineSpout.
 *
 * As acks/fails come into SidelineSpout, they will be routed to the appropriate VirtualSidelineSpout instance
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
    private final Map<String, Object> spoutConfig;

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

    // TEMP
    private final Map<String, Long> nextTupleTimeBuckets = Maps.newHashMap();
    private final Map<String, Long> ackTimeBuckets = Maps.newHashMap();

    /**
     * Constructor.
     * Use this constructor for your "FireHose" instance.  IE an instance that has no starting or ending state.
     * @param spoutConfig - our topology config
     * @param topologyContext - our topology context
     * @param factoryManager - FactoryManager instance.
     */
    public VirtualSpout(Map spoutConfig, TopologyContext topologyContext, FactoryManager factoryManager, MetricsRecorder metricsRecorder) {
        this(spoutConfig, topologyContext, factoryManager, metricsRecorder, null, null);
    }

    /**
     * Constructor.
     * Use this constructor for your Sidelined instances.  IE an instance that has a specified starting and ending
     * state.
     * @param spoutConfig - our topology config
     * @param topologyContext - our topology context
     * @param factoryManager - FactoryManager instance.
     * @param startingState - Where the underlying consumer should start from, Null if start from head.
     * @param endingState - Where the underlying consumer should stop processing.  Null if process forever.
     */
    public VirtualSpout(Map spoutConfig, TopologyContext topologyContext, FactoryManager factoryManager, MetricsRecorder metricsRecorder, ConsumerState startingState, ConsumerState endingState) {
        // Save reference to topology context
        this.topologyContext = topologyContext;

        // Save an immutable clone of the config
        this.spoutConfig = Tools.immutableCopy(spoutConfig);

        // Save factory manager instance
        this.factoryManager = factoryManager;

        // Save metric recorder instance.
        this.metricsRecorder = metricsRecorder;

        // Save state
        this.startingState = startingState;
        this.endingState = endingState;
    }

    /**
     * For testing only! Constructor used in testing to inject SidelineConsumer instance.
     */
    protected VirtualSpout(Map spoutConfig, TopologyContext topologyContext, FactoryManager factoryManager, MetricsRecorder metricsRecorder, Consumer consumer, ConsumerState startingState, ConsumerState endingState) {
        this(spoutConfig, topologyContext, factoryManager, metricsRecorder, startingState, endingState);

        // Inject the consumer.
        this.consumer = consumer;
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
        // Set state to true.
        isOpened = true;

        // For debugging purposes
        logger.info("Open has starting state: {}", startingState);
        logger.info("Open has ending state: {}", endingState);

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
        consumer.open(spoutConfig, getVirtualSpoutId(), consumerPeerContext, persistenceAdapter, startingState);

        // Temporary metric buckets.
        // TODO - convert to using proper metrics recorder?
        nextTupleTimeBuckets.put("failedRetry", 0L);
        nextTupleTimeBuckets.put("isFiltered", 0L);
        nextTupleTimeBuckets.put("nextRecord", 0L);
        nextTupleTimeBuckets.put("messageId", 0L);
        nextTupleTimeBuckets.put("doesExceedEndOffset", 0L);
        nextTupleTimeBuckets.put("message", 0L);
        nextTupleTimeBuckets.put("totalTime", 0L);
        nextTupleTimeBuckets.put("totalCalls", 0L);

        ackTimeBuckets.put("TotalTime", 0L);
        ackTimeBuckets.put("TotalCalls", 0L);
        ackTimeBuckets.put("FailedMsgAck", 0L);
        ackTimeBuckets.put("RemoveTracked", 0L);
        ackTimeBuckets.put("CommitOffset", 0L);
        ackTimeBuckets.put("MessageId", 0L);
    }

    @Override
    public void close() {
        // If we've successfully completed processing
        if (isCompleted()) {
            // We should clean up consumer state
            consumer.removeConsumerState();

            // TODO: This should be moved out of here so that the vspout has no notion of a sideline request
            final SidelineRequestIdentifier sidelineRequestIdentifier = ((SidelineVirtualSpoutIdentifier) getVirtualSpoutId()).getSidelineRequestIdentifier();

            // Clean up sideline request
            if (sidelineRequestIdentifier != null && startingState != null) { // TODO: Probably should find a better way to pull a list of partitions
                for (final ConsumerPartition consumerPartition : startingState.getConsumerPartitions()) {
                    consumer.getPersistenceAdapter().clearSidelineRequest(
                        sidelineRequestIdentifier,
                        consumerPartition.partition()
                    );
                }
            }
        } else {
            // We are just closing up shop,
            // First flush our current consumer state.
            consumer.flushConsumerState();
        }
        // Call close & null reference.
        consumer.close();
        consumer = null;

        startingState = null;
        endingState = null;
    }

    /**
     * @return - The next Message that should be played into the topology.
     */
    @Override
    public Message nextTuple() {
        long totalTime = System.currentTimeMillis();

        // Talk to a "failed tuple manager interface" object to see if any tuples
        // that failed previously are ready to be replayed.  This is an interface
        // meaning you can implement your own behavior here.  Maybe failed tuples never get replayed,
        // Maybe they get replayed a maximum number of times?  Maybe they get replayed forever but have
        // an exponential back off time period between fails?  Who knows/cares, not us cuz its an interface.
        // If so, emit that and return.
        long startTime = System.currentTimeMillis();
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
        nextTupleTimeBuckets.put("failedRetry", nextTupleTimeBuckets.get("failedRetry") + (System.currentTimeMillis() - startTime));

        // Grab the next message from Consumer instance.
        startTime = System.currentTimeMillis();
        final Record record = consumer.nextRecord();
        if (record == null) {
            logger.debug("Unable to find any new messages from consumer");
            return null;
        }
        nextTupleTimeBuckets.put("nextRecord", nextTupleTimeBuckets.get("nextRecord") + (System.currentTimeMillis() - startTime));

        // Create a Tuple Message Id
        startTime = System.currentTimeMillis();
        final MessageId messageId = new MessageId(record.getNamespace(), record.getPartition(), record.getOffset(), getVirtualSpoutId());
        nextTupleTimeBuckets.put("messageId", nextTupleTimeBuckets.get("messageId") + (System.currentTimeMillis() - startTime));

        // Determine if this tuple exceeds our ending offset
        startTime = System.currentTimeMillis();
        if (doesMessageExceedEndingOffset(messageId)) {
            logger.debug("Tuple {} exceeds max offset, acking", messageId);

            // Unsubscribe partition this tuple belongs to.
            unsubscribeTopicPartition(messageId.getNamespace(), messageId.getPartition());

            // We don't need to ack the tuple because it never got emitted out.
            // Simply return null.
            return null;
        }
        nextTupleTimeBuckets.put("doesExceedEndOffset", nextTupleTimeBuckets.get("doesExceedEndOffset") + (System.currentTimeMillis() - startTime));

        // Create Message
        startTime = System.currentTimeMillis();
        final Message message = new Message(messageId, record.getValues());
        nextTupleTimeBuckets.put("message", nextTupleTimeBuckets.get("message") + (System.currentTimeMillis() - startTime));

        // Determine if this tuple should be filtered. If it IS filtered, loop and find the next one?
        // Loops through each step in the chain to filter a filter before emitting
        startTime = System.currentTimeMillis();
        final boolean isFiltered  = getFilterChain().filter(message);
        nextTupleTimeBuckets.put("isFiltered", nextTupleTimeBuckets.get("isFiltered") + (System.currentTimeMillis() - startTime));

        // Keep Track of the tuple in this spout somewhere so we can replay it if it happens to fail.
        if (isFiltered) {
            // Increment filtered metric
            getMetricsRecorder().count(VirtualSpout.class, getVirtualSpoutId() + ".filtered");

            // Ack
            ack(messageId);

            // return null.
            return null;
        }

        // Track it message for potential retries.
        trackedMessages.put(messageId, message);

        // record total time and total calls
        nextTupleTimeBuckets.put("totalTime", nextTupleTimeBuckets.get("totalTime") + (System.currentTimeMillis() - totalTime));
        nextTupleTimeBuckets.put("totalCalls", nextTupleTimeBuckets.get("totalCalls") + 1);

        // TEMP Every so often display stats
        if (nextTupleTimeBuckets.get("totalCalls") % 10_000_000 == 0) {
            totalTime = nextTupleTimeBuckets.get("totalTime");
            logger.info("==== nextTuple() Totals after {} calls ====", nextTupleTimeBuckets.get("totalCalls"));
            for (Map.Entry<String, Long> entry : nextTupleTimeBuckets.entrySet()) {
                logger.info("nextTuple() {} => {} ms ({}%)", entry.getKey(), entry.getValue(), ((float) entry.getValue() / totalTime) * 100);
            }
        }

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
            throw new IllegalStateException("Consuming from a namespace/partition without a defined end offset? [" + messageId.getNamespace() + "-" + messageId.getPartition() + "] not in (" + endingState + ")");
        }

        // If its > the ending offset
        return currentOffset > endingOffset;
    }

    @Override
    public void ack(Object msgId) {
        long totalTime = System.currentTimeMillis();

        if (msgId == null) {
            logger.warn("Null msg id passed, ignoring");
            return;
        }

        // Convert to MessageId
        long start = System.currentTimeMillis();
        final MessageId messageId;
        try {
            messageId = (MessageId) msgId;
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Invalid msgId object type passed " + msgId.getClass());
        }
        ackTimeBuckets.put("MessageId", ackTimeBuckets.get("MessageId") + (System.currentTimeMillis() - start));

        // Talk to sidelineConsumer and mark the offset completed.
        start = System.currentTimeMillis();
        consumer.commitOffset(messageId.getNamespace(), messageId.getPartition(), messageId.getOffset());
        ackTimeBuckets.put("CommitOffset", ackTimeBuckets.get("CommitOffset") + (System.currentTimeMillis() - start));

        // Remove this tuple from the spout where we track things in-case the tuple fails.
        start = System.currentTimeMillis();
        trackedMessages.remove(messageId);
        ackTimeBuckets.put("RemoveTracked", ackTimeBuckets.get("RemoveTracked") + (System.currentTimeMillis() - start));

        // Mark it as completed in the failed message handler if it exists.
        start = System.currentTimeMillis();
        retryManager.acked(messageId);
        ackTimeBuckets.put("FailedMsgAck", ackTimeBuckets.get("FailedMsgAck") + (System.currentTimeMillis() - start));

        // Increment totals
        ackTimeBuckets.put("TotalTime", ackTimeBuckets.get("TotalTime") + (System.currentTimeMillis() - totalTime));
        ackTimeBuckets.put("TotalCalls", ackTimeBuckets.get("TotalCalls") + 1);

        // TEMP Every so often display stats
        if (ackTimeBuckets.get("TotalCalls") % 10_000_000 == 0) {
            totalTime = ackTimeBuckets.get("TotalTime");
            logger.info("==== ack() Totals after {} calls ====", ackTimeBuckets.get("TotalCalls"));
            for (Map.Entry<String, Long> entry : ackTimeBuckets.entrySet()) {
                logger.info("ack() {} => {} ms ({}%)", entry.getKey(), entry.getValue(), ((float) entry.getValue() / totalTime) * 100);
            }
        }
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
        } catch (ClassCastException e) {
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
            getMetricsRecorder().count(VirtualSpout.class, getVirtualSpoutId() + ".exceeded_retry_limit");

            // Done.
            return;
        }

        // Otherwise mark it as failed.
        retryManager.failed(messageId);

        // Update metric
        getMetricsRecorder().count(VirtualSpout.class, getVirtualSpoutId() + ".fail");
    }

    /**
     * Call this method to request this VirtualSidelineSpout instance
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

    /**
     * Define the virtualSpoutId for this VirtualSpout.
     * @param virtualSpoutId - The unique identifier for this consumer.
     */
    public void setVirtualSpoutId(final VirtualSpoutIdentifier virtualSpoutId) {
        if (Strings.isNullOrEmpty(virtualSpoutId.toString())) {
            throw new IllegalStateException("Consumer id cannot be null or empty! (" + virtualSpoutId + ")");
        }
        this.virtualSpoutId = virtualSpoutId;
    }

    public FilterChain getFilterChain() {
        return filterChain;
    }

    public ConsumerState getCurrentState() {
        return consumer.getCurrentState();
    }

    @Override
    public double getMaxLag() {
        return consumer.getMaxLag();
    }

    @Override
    public int getNumberOfFiltersApplied() {
        return getFilterChain().getSteps().size();
    }

    public Map<String, Object> getSpoutConfig() {
        return spoutConfig;
    }

    public TopologyContext getTopologyContext() {
        return topologyContext;
    }

    public Object getSpoutConfigItem(final String key) {
        return spoutConfig.get(key);
    }

    /**
     * Used in tests.
     */
    protected FactoryManager getFactoryManager() {
        return factoryManager;
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

        // See if we can finish and close out this VirtualSidelineConsumer.
        attemptToComplete();
    }

    /**
     * Internal method that determines if this sideline consumer is finished.
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
                logger.debug("On {} Current Offset: {}  Ending Offset: {} (This partition is completed!)", consumerPartition, currentOffset, endingOffset);
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
