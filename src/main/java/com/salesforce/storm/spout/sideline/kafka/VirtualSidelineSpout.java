package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.filter.FilterChain;
import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Deserializer;
import com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers.FailedMsgRetryManager;
import com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers.NoRetryFailedMsgRetryManager;
import com.salesforce.storm.spout.sideline.metrics.LogRecorder;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.persistence.PersistenceManager;
import com.salesforce.storm.spout.sideline.persistence.ZookeeperPersistenceManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A Rough outline of what this internal spout could look like.
 * This doesn't implement the Storm IRichSpout interface because its not a 'real' spout,
 * but sits within the larger "SideLineSpout"  Many instances of these can get created and destroyed
 * during the life time of the "real" "SideLineSpout"
 */
public class VirtualSidelineSpout implements DelegateSidelineSpout {
    // Logging
    private static final Logger logger = LoggerFactory.getLogger(VirtualSidelineSpout.class);

    /**
     * Holds reference to our topologyContext.
     */
    private final TopologyContext topologyContext;

    /**
     * Holds reference to our topology configuration.
     */
    private final Map topologyConfig;

    private SidelineConsumer sidelineConsumer;
    private Deserializer deserializer;
    private FilterChain filterChain = new FilterChain();
    private boolean finished = false;

    // Define starting and ending offsets.
    private ConsumerState startingState = null;
    private ConsumerState endingState = null;

    /**
     * Flag to maintain if open() has been called already.
     */
    private boolean isOpened = false;

    /**
     * Is our unique ConsumerId.
     */
    private String consumerId;

    /**
     * For collecting metrics.
     */
    private MetricsRecorder metricsRecorder;

    /**
     * For tracking failed messages, and knowing when to replay them.
     */
    private FailedMsgRetryManager failedMsgRetryManager;
    private Map<TupleMessageId, KafkaMessage> trackedMessages = Maps.newHashMap();

    public VirtualSidelineSpout(Map topologyConfig, TopologyContext topologyContext, Deserializer deserializer, FailedMsgRetryManager failedMsgRetryManager, MetricsRecorder metricsRecorder) {
        this(topologyConfig, topologyContext, deserializer, failedMsgRetryManager, metricsRecorder, null, null);
    }

    /**
     * Constructor.
     * @param topologyConfig
     * @param topologyContext
     * @param startingState
     * @param endingState
     */
    public VirtualSidelineSpout(Map topologyConfig, TopologyContext topologyContext, Deserializer deserializer, FailedMsgRetryManager failedMsgRetryManager, MetricsRecorder metricsRecorder, ConsumerState startingState, ConsumerState endingState) {
        // Save reference to topology context
        this.topologyContext = topologyContext;

        // Save an immutable clone of the config
        this.topologyConfig = Collections.unmodifiableMap(topologyConfig);

        // Save deserializer instance
        this.deserializer = deserializer;

        // Save failed msg retry manager instance.
        this.failedMsgRetryManager = failedMsgRetryManager;

        // Save metrics record
        this.metricsRecorder = metricsRecorder;

        // Save state
        this.startingState = startingState;
        this.endingState = endingState;
    }

    /**
     * For testing only! Constructor used in testing to inject SidelineConsumer instance.
     *
     * @param config
     * @param topologyContext
     * @param deserializer - Injected deserializer instance, typically used for testing.
     * @param sidelineConsumer
     */
    protected VirtualSidelineSpout(Map config, TopologyContext topologyContext, Deserializer deserializer, FailedMsgRetryManager failedMsgRetryManager, SidelineConsumer sidelineConsumer) {
        this(config, topologyContext, deserializer, failedMsgRetryManager, (MetricsRecorder) null);
        this.sidelineConsumer = sidelineConsumer;
    }

    /**
     * For testing only! Constructor used in testing to inject SidelineConsumer instance.
     *
     * @param config
     * @param topologyContext
     * @param deserializer - Injected deserializer instance, typically used for testing.
     * @param sidelineConsumer
     */
    protected VirtualSidelineSpout(Map config, TopologyContext topologyContext, Deserializer deserializer, SidelineConsumer sidelineConsumer, ConsumerState startingState, ConsumerState endingState) {
        this(config, topologyContext, deserializer, new NoRetryFailedMsgRetryManager(), null, startingState, endingState);
        this.sidelineConsumer = sidelineConsumer;
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

        // TODO - This should get passed in.
        if (metricsRecorder == null) {
            logger.error("TODO REMOVE THIS");
            metricsRecorder = new LogRecorder();
            metricsRecorder.open(topologyConfig, topologyContext);
        }

        // For debugging purposes
        logger.info("Open has Starting State: {}", startingState);
        logger.info("Open has Ending State: {}", endingState);

        // Call open on failed msg retry manager instance.
        failedMsgRetryManager.open(topologyConfig);

        // Construct SidelineConsumerConfig from incoming config
        final List<String> kafkaBrokers = (List<String>) getTopologyConfigItem(SidelineSpoutConfig.KAFKA_BROKERS);
        final String topic = (String) getTopologyConfigItem(SidelineSpoutConfig.KAFKA_TOPIC);
        final SidelineConsumerConfig consumerConfig = new SidelineConsumerConfig(kafkaBrokers, getConsumerId(), topic);

        // Do we need to set starting offset here somewhere?  Probably.
        // Either we need to set the offsets from the incoming config,
        // Or we need to tell it to start from somewhere

        // TODO: this should be removed I believe?  We always inject one.
        // Create a consumer, but..
        // if one was injected via the constructor, just use it.
        if (sidelineConsumer == null) {
            // Build our implementation of PersistenceManager and open() it.
            final PersistenceManager persistenceManager = new ZookeeperPersistenceManager();
            persistenceManager.open(getTopologyConfig());

            sidelineConsumer = new SidelineConsumer(consumerConfig, persistenceManager);
        }

        // Connect the consumer
        sidelineConsumer.open(startingState);

        // If we have an ending state, assign values
        if (endingState != null) {
            // Update our metrics
            updateMetrics(endingState, "endingOffset");
        }
    }

    @Override
    public void close() {
        // TODO: Do we need to clean up state and remove it?

        // Close out the consumer
        sidelineConsumer.close();
        sidelineConsumer = null;
    }

    /**
     * Shoudl this return a tuple?  Some other intermediate abstraction?  The deserialized object
     * from the kafka message?  Unsure
     * @return unknown.
     */
    @Override
    public KafkaMessage nextTuple() {
        // Talk to a "failed tuple manager interface" object to see if any tuples
        // that failed previously are ready to be replayed.  This is an interface
        // meaning you can implement your own behavior here.  Maybe failed tuples never get replayed,
        // Maybe they get replayed a maximum number of times?  Maybe they get replayed forever but have
        // an exponential back off time period between fails?  Who knows/cares, not us cuz its an interface.
        // If so, emit that and return.
        final TupleMessageId nextFailedMessageId = failedMsgRetryManager.nextFailedMessageToRetry();
        if (nextFailedMessageId != null) {
            if (trackedMessages.containsKey(nextFailedMessageId)) {
                // Mark this as having a retry started
                failedMsgRetryManager.retryStarted(nextFailedMessageId);

                // Emit the tuple.
                logger.info("Emitting previously failed tuple with msgId {}", nextFailedMessageId);
                return trackedMessages.get(nextFailedMessageId);
            } else {
                logger.warn("Unable to find tuple that should be replayed due to a fail {}", nextFailedMessageId);
                failedMsgRetryManager.acked(nextFailedMessageId);
            }
        }

        // Grab the next message from kafka
        ConsumerRecord<byte[], byte[]> record = sidelineConsumer.nextRecord();
        if (record == null) {
            logger.warn("Unable to find any new messages from consumer");
            return null;
        }

        // Create a Tuple Message Id
        final TupleMessageId tupleMessageId = new TupleMessageId(record.topic(), record.partition(), record.offset(), getConsumerId());

        // Attempt to deserialize.
        final Values deserializedValues = deserializer.deserialize(record.topic(), record.partition(), record.offset(), record.key(), record.value());
        if (deserializedValues == null) {
            // Failed to deserialize, just ack and return null?
            logger.error("Deserialization returned null");
            ack(tupleMessageId);
            return null;
        }

        // Create KafkaMessage
        final KafkaMessage message = new KafkaMessage(tupleMessageId, deserializedValues);

        // Determine if this tuple exceeds our ending offset
        if (doesMessageExceedEndingOffset(tupleMessageId)) {
            logger.info("Tuple {} exceeds max offset, acking", tupleMessageId);

            // Unsubscribe partition this tuple belongs to.
            unsubscribeTopicPartition(tupleMessageId.getTopicPartition());

            // Ack tuple
            ack(tupleMessageId);

            // Return null.
            return null;
        }

        // Determine if this tuple should be filtered. If it IS filtered, loop and find the next one?
        // Loops through each step in the chain to filter a filter before emitting
        final boolean isFiltered  = this.filterChain.filter(message);

        // Keep Track of the tuple in this spout somewhere so we can replay it if it happens to fail.
        if (isFiltered) {
            logger.info("Tuple {} is filtered, acking", message.getTupleMessageId());
            // Ack
            ack(tupleMessageId);

            // return null.
            return null;
        }

        // Track it message for potential retries.
        trackedMessages.put(tupleMessageId, message);

        // Return it.
        return message;
    }

    /**
     * For the given TupleMessageId, does it exceed any defined ending offsets?
     * @param tupleMessageId - The TupleMessageId to check.
     * @return - Boolean - True if it does, false if it does not.
     */
    protected boolean doesMessageExceedEndingOffset(final TupleMessageId tupleMessageId) {
        // If no end offsets defined
        if (endingState == null) {
            // Then this check is a no-op, return false
            return false;
        }

        final TopicPartition topicPartition = tupleMessageId.getTopicPartition();
        final long currentOffset = tupleMessageId.getOffset();

        // Find ending offset for this topic partition
        final Long endingOffset = endingState.getOffsetForTopicAndPartition(topicPartition);
        if (endingOffset == null) {
            // None defined?  Probably an error
            throw new IllegalStateException("Consuming from a topic/partition without a defined end offset? " + topicPartition + " not in (" + endingState + ")");
        }

        // If its > the ending offset
        logger.info("Current Offset: {} EndingOffset: {}", currentOffset, endingOffset);
        if (currentOffset > endingOffset) {
            // Then
            return true;
        }
        return false;
    }

    @Override
    public void ack(Object msgId) {
        if (msgId == null) {
            logger.warn("Null msg id passed, ignoring");
            return;
        }

        // Convert to TupleMessageId
        final TupleMessageId tupleMessageId;
        try {
            tupleMessageId = (TupleMessageId) msgId;
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Invalid msgId object type passed " + msgId.getClass());
        }

        // Talk to sidelineConsumer and mark the offset completed.
        sidelineConsumer.commitOffset(tupleMessageId.getTopicPartition(), tupleMessageId.getOffset());

        // Remove this tuple from the spout where we track things incase the tuple fails.
        trackedMessages.remove(tupleMessageId);

        // Mark it as completed in the failed message handler if it exists.
        failedMsgRetryManager.acked(tupleMessageId);

        // Update our metrics,
        // TODO: probably doesn't need to happen every ack?
        updateMetrics(sidelineConsumer.getCurrentState(), "currentOffset");

        // See if we can finished
        attemptToFinish();
    }

    /**
     * Internal method that determines if this sideline consumer is finished.
     */
    private void attemptToFinish() {
        // If we're still tracking msgs
        if (!trackedMessages.isEmpty()) {
            // We cannot finish.
            return;
        }

        // Check to see if we are still subscribed to any partitions
        if (!sidelineConsumer.getAssignedPartitions().isEmpty()) {
            // We still are subscribed to some partitions, so cannot finish
            return;
        }

        // If we're here, we are no longer tracking any tuples, and we are not subscribed to anything
        // So we can finish
        finish();
    }

    private void updateMetrics(final ConsumerState consumerState, final String keyPrefix) {
        for (TopicPartition partition: consumerState.getState().keySet()) {
            final String key = getConsumerId() +  "." + keyPrefix + ".partition" + partition.partition();
            metricsRecorder.assignValue(getClass(), key, consumerState.getOffsetForTopicAndPartition(partition));
        }
    }

    @Override
    public void fail(Object msgId) {
        if (msgId == null) {
            logger.warn("Null msg id passed, ignoring");
            return;
        }

        // Convert to TupleMessageId
        final TupleMessageId tupleMessageId;
        try {
            tupleMessageId = (TupleMessageId) msgId;
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Invalid msgId object type passed " + msgId.getClass());
        }

        // Add this tuple to a "failed tuple manager interface" object
        if (!failedMsgRetryManager.retryFurther(tupleMessageId)) {
            logger.info("Not retrying failed msgId any further {}", tupleMessageId);

            // Mark it as acked in failedMsgRetryManager
            failedMsgRetryManager.acked(tupleMessageId);

            // Ack it in the consumer
            sidelineConsumer.commitOffset(tupleMessageId.getTopicPartition(), tupleMessageId.getOffset());

            // Done.
            return;
        }

        // Otherwise mark it as failed.
        failedMsgRetryManager.failed(tupleMessageId);
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public FilterChain getFilterChain() {
        return filterChain;
    }

    public ConsumerState getCurrentState() {
        return sidelineConsumer.getCurrentState();
    }

    public Map<String, Object> getTopologyConfig() {
        return topologyConfig;
    }

    public TopologyContext getTopologyContext() {
        return topologyContext;
    }

    public Object getTopologyConfigItem(final String key) {
        return topologyConfig.get(key);
    }

    /**
     * Unsubscribes the underlying consumer from the specified topic/partition.
     *
     * @param topicPartition - the topic/partition to unsubscribe from.
     * @return boolean - true if successfully unsub'd, false if not.
     */
    public boolean unsubscribeTopicPartition(TopicPartition topicPartition) {
        logger.info("Unsubscribing from partition {}", topicPartition);
        return sidelineConsumer.unsubscribeTopicPartition(topicPartition);
    }
}
