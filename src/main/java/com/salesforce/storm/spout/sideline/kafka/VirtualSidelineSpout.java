package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.FactoryManager;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.filter.FilterChain;
import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Deserializer;
import com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers.FailedMsgRetryManager;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.persistence.PersistenceManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * VirtualSidelineSpout is essentially a Spout instance within a Spout. It doesn't fully implement the
 * Storm IRichSpout interface because its not a 'real' spout, but it follows it fairly closely.
 * These instances are designed to live within a {@link com.salesforce.storm.spout.sideline.SidelineSpout}
 * instance.  During the lifetime of a SidelineSpout, many VirtualSidelineSpouts can get created and destroyed.
 *
 * The VirtualSidelineSpout will consume from a configured topic and return {@link KafkaMessage} from its
 * {@link #nextTuple()} method.  These will eventually get converted to the appropriate Tuples and emitted
 * out by the SidelineSpout.
 *
 * As acks/fails come into SidelineSpout, they will be routed to the appropriate VirtualSidelineSpout instance
 * and handled by the {@link #ack(Object)} and {@link #fail(Object)} methods.
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

    /**
     * Our Factory Manager.
     */
    private final FactoryManager factoryManager;

    /**
     * Our underlying Kafka Consumer.
     */
    private SidelineConsumer sidelineConsumer;

    /**
     * Our Deserializer, it deserializes messages from Kafka into objects.
     */
    private Deserializer deserializer;


    private FilterChain filterChain = new FilterChain();

    // Define starting and ending offsets.
    private ConsumerState startingState = null;
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

    // TEMP
    private final Map<String, Long> nextTupleTimeBuckets = Maps.newHashMap();
    private final Map<String, Long> ackTimeBuckets = Maps.newHashMap();

    /**
     * Constructor.
     * Use this constructor for your "FireHose" instance.  IE an instance that has no starting or ending state.
     * @param topologyConfig - our topology config
     * @param topologyContext - our topology context
     * @param factoryManager - FactoryManager instance.
     * @param metricsRecorder - For recording metrics.
     */
    public VirtualSidelineSpout(Map topologyConfig, TopologyContext topologyContext, FactoryManager factoryManager, MetricsRecorder metricsRecorder) {
        this(topologyConfig, topologyContext, factoryManager, metricsRecorder, null, null);
    }

    /**
     * Constructor.
     * Use this constructor for your Sidelined instances.  IE an instance that has a specified starting and ending
     * state.
     * @param topologyConfig - our topology config
     * @param topologyContext - our topology context
     * @param factoryManager - FactoryManager instance.
     * @param metricsRecorder - For recording metrics.
     * @param startingState - Where the underlying consumer should start from, Null if start from head.
     * @param endingState - Where the underlying consumer should stop processing.  Null if process forever.
     */
    public VirtualSidelineSpout(Map topologyConfig, TopologyContext topologyContext, FactoryManager factoryManager, MetricsRecorder metricsRecorder, ConsumerState startingState, ConsumerState endingState) {
        // Save reference to topology context
        this.topologyContext = topologyContext;

        // Save an immutable clone of the config
        this.topologyConfig = ImmutableMap.copyOf(topologyConfig);

        // Save factory manager instance
        this.factoryManager = factoryManager;

        // Save metrics recorder
        this.metricsRecorder = metricsRecorder;

        // Save state
        this.startingState = startingState;
        this.endingState = endingState;
    }

    /**
     * For testing only! Constructor used in testing to inject SidelineConsumer instance.
     */
    protected VirtualSidelineSpout(Map topologyConfig, TopologyContext topologyContext, FactoryManager factoryManager, MetricsRecorder metricsRecorder, SidelineConsumer sidelineConsumer, ConsumerState startingState, ConsumerState endingState) {
        this(topologyConfig, topologyContext, factoryManager, metricsRecorder, startingState, endingState);

        // Inject the sideline consumer.
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

        // For debugging purposes
        logger.info("Open has starting state: {}", startingState);
        logger.info("Open has ending state: {}", endingState);

        // Create our deserializer
        deserializer = getFactoryManager().createNewDeserializerInstance();

        // Create our failed msg retry manager & open
        failedMsgRetryManager = getFactoryManager().createNewFailedMsgRetryManagerInstance();
        failedMsgRetryManager.open(getTopologyConfig());

        // Create underlying kafka consumer - Normal Behavior is for this to be null here.
        // The only time this would be non-null would be if it was injected for tests.
        if (sidelineConsumer == null) {
            // Create persistence manager instance and open.
            final PersistenceManager persistenceManager = getFactoryManager().createNewPersistenceManagerInstance();
            persistenceManager.open(getTopologyConfig());

            // Construct SidelineConsumerConfig based on topology config.
            final List<String> kafkaBrokers = (List<String>) getTopologyConfigItem(SidelineSpoutConfig.KAFKA_BROKERS);
            final String topic = (String) getTopologyConfigItem(SidelineSpoutConfig.KAFKA_TOPIC);
            final SidelineConsumerConfig consumerConfig = new SidelineConsumerConfig(kafkaBrokers, getConsumerId(), topic);

            // Create sideline consumer
            sidelineConsumer = new SidelineConsumer(consumerConfig, persistenceManager);
        }

        // Open the consumer
        sidelineConsumer.open(startingState, getPartitions());

        // If we have an ending state, we set some metrics.
        if (endingState != null) {
            // TODO: commented out for now.
            // Update our metrics
            //updateMetrics(endingState, "endingOffset");
        }

        // TEMP
        nextTupleTimeBuckets.put("failedRetry", 0L);
        nextTupleTimeBuckets.put("isFiltered", 0L);
        nextTupleTimeBuckets.put("nextRecord", 0L);
        nextTupleTimeBuckets.put("tupleMessageId", 0L);
        nextTupleTimeBuckets.put("doesExceedEndOffset", 0L);
        nextTupleTimeBuckets.put("deserialize", 0L);
        nextTupleTimeBuckets.put("kafkaMessage", 0L);
        nextTupleTimeBuckets.put("totalTime", 0L);
        nextTupleTimeBuckets.put("totalCalls", 0L);

        ackTimeBuckets.put("TotalTime", 0L);
        ackTimeBuckets.put("TotalCalls", 0L);
        ackTimeBuckets.put("FailedMsgAck", 0L);
        ackTimeBuckets.put("RemoveTracked", 0L);
        ackTimeBuckets.put("CommitOffset", 0L);
        ackTimeBuckets.put("TupleMessageId", 0L);
    }

    /**
     * Get the partitions that this particular spout instance should consume from.
     * @return List of partitions to consume from
     */
    private List<PartitionInfo> getPartitions() {
        final int numInstances = topologyContext.getComponentTasks(topologyContext.getThisComponentId()).size();

        final int instanceIndex = topologyContext.getThisTaskIndex();

        final List<PartitionInfo> allPartitions = sidelineConsumer.getPartitions();

        final List<PartitionInfo> myPartitions = new ArrayList<>();

        for (PartitionInfo partition : allPartitions) {
            if (partition.partition() % numInstances == instanceIndex) {
                myPartitions.add(partition);
            }
        }

        return myPartitions;
    }

    @Override
    public void close() {
        // If we've successfully completed processing
        if (isCompleted) {
            // We should clean up consumer state
            sidelineConsumer.removeConsumerState();
        } else {
            // We are just closing up shop,
            // First flush our current consumer state.
            sidelineConsumer.flushConsumerState();
        }
        // Call close & null reference.
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
        long totalTime = System.currentTimeMillis();

        // Talk to a "failed tuple manager interface" object to see if any tuples
        // that failed previously are ready to be replayed.  This is an interface
        // meaning you can implement your own behavior here.  Maybe failed tuples never get replayed,
        // Maybe they get replayed a maximum number of times?  Maybe they get replayed forever but have
        // an exponential back off time period between fails?  Who knows/cares, not us cuz its an interface.
        // If so, emit that and return.
        long startTime = System.currentTimeMillis();
        final TupleMessageId nextFailedMessageId = failedMsgRetryManager.nextFailedMessageToRetry();
        if (nextFailedMessageId != null) {
            if (trackedMessages.containsKey(nextFailedMessageId)) {
                // Emit the tuple.
                return trackedMessages.get(nextFailedMessageId);
            } else {
                logger.warn("Unable to find tuple that should be replayed due to a fail {}", nextFailedMessageId);
                failedMsgRetryManager.acked(nextFailedMessageId);
            }
        }
        nextTupleTimeBuckets.put("failedRetry", nextTupleTimeBuckets.get("failedRetry") + (System.currentTimeMillis() - startTime));

        // Grab the next message from kafka
        startTime = System.currentTimeMillis();
        ConsumerRecord<byte[], byte[]> record = sidelineConsumer.nextRecord();
        if (record == null) {
            logger.warn("Unable to find any new messages from consumer");
            return null;
        }
        nextTupleTimeBuckets.put("nextRecord", nextTupleTimeBuckets.get("nextRecord") + (System.currentTimeMillis() - startTime));

        // Create a Tuple Message Id
        startTime = System.currentTimeMillis();
        final TupleMessageId tupleMessageId = new TupleMessageId(record.topic(), record.partition(), record.offset(), getConsumerId());
        nextTupleTimeBuckets.put("tupleMessageId", nextTupleTimeBuckets.get("tupleMessageId") + (System.currentTimeMillis() - startTime));

        // Determine if this tuple exceeds our ending offset
        startTime = System.currentTimeMillis();
        if (doesMessageExceedEndingOffset(tupleMessageId)) {
            logger.debug("Tuple {} exceeds max offset, acking", tupleMessageId);

            // Unsubscribe partition this tuple belongs to.
            unsubscribeTopicPartition(tupleMessageId.getTopicPartition());

            // We don't need to ack the tuple because it never got emitted out.
            // Simply return null.
            return null;
        }
        nextTupleTimeBuckets.put("doesExceedEndOffset", nextTupleTimeBuckets.get("doesExceedEndOffset") + (System.currentTimeMillis() - startTime));

        // Attempt to deserialize.
        startTime = System.currentTimeMillis();
        final Values deserializedValues = deserializer.deserialize(record.topic(), record.partition(), record.offset(), record.key(), record.value());
        if (deserializedValues == null) {
            // Failed to deserialize, just ack and return null?
            logger.error("Deserialization returned null");
            ack(tupleMessageId);
            return null;
        }
        nextTupleTimeBuckets.put("deserialize", nextTupleTimeBuckets.get("deserialize") + (System.currentTimeMillis() - startTime));

        // Create KafkaMessage
        startTime = System.currentTimeMillis();
        final KafkaMessage message = new KafkaMessage(tupleMessageId, deserializedValues);
        nextTupleTimeBuckets.put("kafkaMessage", nextTupleTimeBuckets.get("kafkaMessage") + (System.currentTimeMillis() - startTime));

        // Determine if this tuple should be filtered. If it IS filtered, loop and find the next one?
        // Loops through each step in the chain to filter a filter before emitting
        startTime = System.currentTimeMillis();
        final boolean isFiltered  = this.filterChain.filter(message);
        nextTupleTimeBuckets.put("isFiltered", nextTupleTimeBuckets.get("isFiltered") + (System.currentTimeMillis() - startTime));

        // Keep Track of the tuple in this spout somewhere so we can replay it if it happens to fail.
        if (isFiltered) {
            // Ack
            ack(tupleMessageId);

            // return null.
            return null;
        }

        // Track it message for potential retries.
        trackedMessages.put(tupleMessageId, message);

        // record total time and total calls
        nextTupleTimeBuckets.put("totalTime", nextTupleTimeBuckets.get("totalTime") + (System.currentTimeMillis() - totalTime));
        nextTupleTimeBuckets.put("totalCalls", nextTupleTimeBuckets.get("totalCalls") + 1);

        // TEMP Every so often display stats
        if (nextTupleTimeBuckets.get("totalCalls") % 10_000_000 == 0) {
            totalTime = nextTupleTimeBuckets.get("totalTime");
            logger.info("==== nextTuple() Totals after {} calls ====", nextTupleTimeBuckets.get("totalCalls"));
            for (String key : nextTupleTimeBuckets.keySet()) {
                logger.info("nextTuple() {} => {} ms ({}%)", key, nextTupleTimeBuckets.get(key), ((float) nextTupleTimeBuckets.get(key) / totalTime) * 100);
            }
        }

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
        if (currentOffset > endingOffset) {
            // Then
            return true;
        }
        return false;
    }

    @Override
    public void ack(Object msgId) {
        long totalTime = System.currentTimeMillis();

        if (msgId == null) {
            logger.warn("Null msg id passed, ignoring");
            return;
        }

        // Convert to TupleMessageId
        long start = System.currentTimeMillis();
        final TupleMessageId tupleMessageId;
        try {
            tupleMessageId = (TupleMessageId) msgId;
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Invalid msgId object type passed " + msgId.getClass());
        }
        ackTimeBuckets.put("TupleMessageId", ackTimeBuckets.get("TupleMessageId") + (System.currentTimeMillis() - start));

        // Talk to sidelineConsumer and mark the offset completed.
        start = System.currentTimeMillis();
        sidelineConsumer.commitOffset(tupleMessageId.getTopicPartition(), tupleMessageId.getOffset());
        ackTimeBuckets.put("CommitOffset", ackTimeBuckets.get("CommitOffset") + (System.currentTimeMillis() - start));

        // Remove this tuple from the spout where we track things incase the tuple fails.
        start = System.currentTimeMillis();
        trackedMessages.remove(tupleMessageId);
        ackTimeBuckets.put("RemoveTracked", ackTimeBuckets.get("RemoveTracked") + (System.currentTimeMillis() - start));

        // Mark it as completed in the failed message handler if it exists.
        start = System.currentTimeMillis();
        failedMsgRetryManager.acked(tupleMessageId);
        ackTimeBuckets.put("FailedMsgAck", ackTimeBuckets.get("FailedMsgAck") + (System.currentTimeMillis() - start));

        // Update our metrics,
        // TODO: probably doesn't need to happen every ack?
        // Commented out because this is massively slow.  Cannot run this on every ack.
        //updateMetrics(sidelineConsumer.getCurrentState(), "currentOffset");

        // Increment totals
        ackTimeBuckets.put("TotalTime", ackTimeBuckets.get("TotalTime") + (System.currentTimeMillis() - totalTime));
        ackTimeBuckets.put("TotalCalls", ackTimeBuckets.get("TotalCalls") + 1);

        // TEMP Every so often display stats
        if (ackTimeBuckets.get("TotalCalls") % 10_000_000 == 0) {
            totalTime = ackTimeBuckets.get("TotalTime");
            logger.info("==== ack() Totals after {} calls ====", ackTimeBuckets.get("TotalCalls"));
            for (String key : ackTimeBuckets.keySet()) {
                logger.info("ack() {} => {} ms ({}%)", key, ackTimeBuckets.get(key), ((float) ackTimeBuckets.get(key) / totalTime) * 100);
            }
        }
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

        // If this tuple shouldn't be replayed again
        if (!failedMsgRetryManager.retryFurther(tupleMessageId)) {
            logger.warn("Not retrying failed msgId any further {}", tupleMessageId);

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

    /**
     * Call this method to request this VirtualSidelineSpout instance
     * to cleanly stop.
     */
    public void requestStop() {
        synchronized (this) {
            requestedStop = true;
        }
    }

    /**
     * Determine if anyone has requested stop on this instance.
     * @return - true if so, false if not.
     */
    public boolean isStopRequested() {
        synchronized (this) {
            return requestedStop;
        }
    }

    @Override
    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        if (Strings.isNullOrEmpty(consumerId)) {
            throw new IllegalStateException("Consumer id cannot be null or empty! (" + consumerId + ")");
        }
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
     * Used in tests.
     */
    protected FactoryManager getFactoryManager() {
        return factoryManager;
    }

    /**
     * Unsubscribes the underlying consumer from the specified topic/partition.
     *
     * @param topicPartition - the topic/partition to unsubscribe from.
     * @return boolean - true if successfully unsub'd, false if not.
     */
    public boolean unsubscribeTopicPartition(TopicPartition topicPartition) {
        final boolean result = sidelineConsumer.unsubscribeTopicPartition(topicPartition);
        if (result) {
            logger.info("Unsubscribed from partition {}", topicPartition);
        }
        return result;
    }

    /**
     * Our maintenance loop.
     */
    @Override
    public void flushState() {
        // Flush consumer state to our persistence layer.
        sidelineConsumer.flushConsumerState();

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
        final ConsumerState currentState = sidelineConsumer.getCurrentState();

        // Compare it against our ending state
        for (TopicPartition topicPartition: currentState.getTopicPartitions()) {
            final long currentOffset = currentState.getOffsetForTopicAndPartition(topicPartition);
            final long endingOffset = endingState.getOffsetForTopicAndPartition(topicPartition);

            // If the current offset is < ending offset
            if (currentOffset < endingOffset) {
                // Then we cannot end
                return;
            }
            // Log that this partition is finished, and make sure we unsubscribe from it.
            logger.debug("{} Current Offset: {}  Ending Offset: {} (This partition is completed!)", topicPartition, currentOffset, endingOffset);
            sidelineConsumer.unsubscribeTopicPartition(topicPartition);
        }

        // If we made it all the way thru the above loop, we completed!
        // Lets flip our flag to true.
        logger.info("Looks like all partitions are complete!  Lets wrap this up.");
        isCompleted = true;

        // Cleanup consumerState.
        // Request that we stop.
        requestStop();
    }
}
