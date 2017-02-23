package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerStateManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A high level kafka consumer that handles state/log offset management in a way that supports
 * marking messages as being processed in no particular order.  On consumer restarts, this implementation errs on the side
 * of re-playing a previously processed message (at least once semantics) over accidentally skipping
 * un-processed messages.  This means there exists certain scenarios where it could replay previously processed
 * messages.
 *
 * TODO: Add clear example of how this can happen for those that care.
 */
public class SidelineConsumer {
    // For logging.
    private static final Logger logger = LoggerFactory.getLogger(SidelineConsumer.class);

    // Kafka Consumer Instance and its Config.
    private final SidelineConsumerConfig consumerConfig;
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;

    /**
     * Boolean to prevent double initialization.
     */
    private boolean hasCalledConnect = false;

    /**
     * State/offset management.
     * ConsumerStateManager - Used to manage persisting consumer state, and when the consumer is restarted,
     * loading the last known consumer state back in.
     */
    private final ConsumerStateManager consumerStateManager;

    /**
     * Since offsets are managed on a per partition basis, each topic/partition has its own ConsumerPartitionStateManagers
     * instance to track its own offset.  The state of these are what gets persisted via the ConsumerStateManager.
     */
    private final Map<TopicPartition, PartitionOffsetManager> partitionStateManagers = Maps.newHashMap();

    /**
     * Used to buffers messages read from Kafka.
     */
    private ConsumerRecords<byte[], byte[]> buffer = null;
    private Iterator<ConsumerRecord<byte[], byte[]>> bufferIterator = null;

    /**
     * Constructor.
     * @param consumerConfig - Configuration for our consumer
     * @param consumerStateManager - Implementation of ConsumerStateManager to use.
     */
    public SidelineConsumer(SidelineConsumerConfig consumerConfig, ConsumerStateManager consumerStateManager) {
        this.consumerConfig = consumerConfig;
        this.consumerStateManager = consumerStateManager;
    }

    /**
     * This constructor is used to inject a mock KafkaConsumer for tests.
     * @param consumerConfig - Configuration for our consumer
     * @param consumerStateManager - Implementation of ConsumerStateManager to use.
     * @param kafkaConsumer - Inject a mock KafkaConsumer for tests.
     */
    protected SidelineConsumer(SidelineConsumerConfig consumerConfig, ConsumerStateManager consumerStateManager, KafkaConsumer<byte[], byte[]> kafkaConsumer) {
        this(consumerConfig, consumerStateManager);
        this.kafkaConsumer = kafkaConsumer;
    }

    /**
     * Handles connecting to the Kafka cluster, determining which partitions to subscribe to,
     * and based on previously saved state from ConsumerStateManager, seek to the last positions processed on
     * each partition.
     */
    public void connect(ConsumerState startingState) {
        // Simple state enforcement.
        if (hasCalledConnect) {
            throw new RuntimeException("Cannot call connect more than once...");
        }
        hasCalledConnect = true;

        // Connect our consumer state manager
        consumerStateManager.init();

        // If we have a starting offset, lets persist it
        if (startingState != null) {
            // If we persist it here, when sideline consumer starts up, it should start from this position.
            // Maybe this is a bit dirty and we should interact w/ SidelineConsumer instead?
            consumerStateManager.persistState(startingState);
        }

        // Load initial positions
        final ConsumerState initialState = consumerStateManager.getState();

        // If kafkaConsumer is not null, we'll create one.
        // If it is NOT null, we'll re-use the instance we got passed in from the constructor.
        // Typically you'd pass in an instance for testing.
        if (kafkaConsumer == null) {
            // Construct new consumer
            kafkaConsumer = new KafkaConsumer<>(getConsumerConfig().getKafkaConsumerProperties());
        }

        // Grab all partitions available for our topic
        List<PartitionInfo> availablePartitions = kafkaConsumer.partitionsFor(getConsumerConfig().getTopic());
        logger.info("Available partitions {}", availablePartitions);

        // Assign them all to this consumer
        List<TopicPartition> allTopicPartitions = Lists.newArrayList();
        for (PartitionInfo availablePartitionInfo: availablePartitions) {
            allTopicPartitions.add(new TopicPartition(availablePartitionInfo.topic(), availablePartitionInfo.partition()));
        }
        kafkaConsumer.assign(allTopicPartitions);

        // Seek to specific positions based on our initial state
        List<TopicPartition> noStatePartitions = Lists.newArrayList();
        for (PartitionInfo availablePartitionInfo: availablePartitions) {
            final TopicPartition availableTopicPartition = new TopicPartition(availablePartitionInfo.topic(), availablePartitionInfo.partition());
            Long offset = initialState.getOffsetForTopicAndPartition(availableTopicPartition);
            if (offset == null) {
                noStatePartitions.add(availableTopicPartition);
                offset = 0L;
            } else {
                logger.info("Resuming topic {} partition {} at offset {}", availableTopicPartition.topic(), availableTopicPartition.partition(), offset);
                kafkaConsumer.seek(availableTopicPartition, offset);
                logger.info("Will resume at offset {}", kafkaConsumer.position(availableTopicPartition));
            }

            // Starting managing offsets for this partition
            partitionStateManagers.put(availableTopicPartition, new PartitionOffsetManager(availableTopicPartition.topic(), availableTopicPartition.partition(), offset));
        }
        if (!noStatePartitions.isEmpty()) {
            logger.info("Starting from head on {} (<Topic>-<Partition>)", noStatePartitions);
            kafkaConsumer.seekToBeginning(noStatePartitions);
        }
    }

    /**
     * Ask the consumer for the next message from Kafka.
     * @return KafkaMessage - the next message read from kafka, or null if no such msg is available.
     */
    public ConsumerRecord<byte[], byte[]> nextRecord() {
        // Fill our buffer if its empty
        fillBuffer();

        // Check our iterator for the next message
        if (!bufferIterator.hasNext()) {
            // Oh no!  No new msg found.
            logger.info("Unable to fill buffer...nothing new!");
            return null;
        }

        // Iterate to next result and return
        ConsumerRecord<byte[], byte[]> nextRecord = bufferIterator.next();

        // Track this new message's state
        TopicPartition topicPartition = new TopicPartition(nextRecord.topic(), nextRecord.partition());
        partitionStateManagers.get(topicPartition).startOffset(nextRecord.offset());

        // Return the filter
        return nextRecord;
    }

    /**
     * Mark a particular offset on a Topic/Partition as having been successfully processed.
     * @param topicPartition - The Topic & Partition the offset belongs to
     * @param offset - The offset that should be marked as completed.
     */
    public void commitOffset(TopicPartition topicPartition, long offset) {
        partitionStateManagers.get(topicPartition).finishOffset(offset);
    }

    /**
     * Mark a particular message as having been successfully processed.
     * @param consumerRecord - the consumer record to mark as completed.
     */
    public void commitOffset(ConsumerRecord consumerRecord) {
        commitOffset(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), consumerRecord.offset());
    }

    /**
     * Forces the Consumer's current state to be persisted.
     * @return - A copy of the state that was persisted.
     */
    public ConsumerState flushConsumerState() {
        // Create a consumer state instance.
        ConsumerState consumerState = new ConsumerState();

        // Loop thru all of our partition managers
        for (Map.Entry<TopicPartition, PartitionOffsetManager> entry: partitionStateManagers.entrySet()) {
            // Get the current offset for each partition.
            consumerState.setOffset(entry.getKey(), entry.getValue().lastFinishedOffset());
        }

        // Persist this state.
        consumerStateManager.persistState(consumerState);

        // Return the state.
        return consumerStateManager.getState();
    }

    /**
     * Internal method used to fill internal message buffer from kafka.
     * Maybe this should be marked private.
     */
    public void fillBuffer() {
        // If our buffer is null, or our iterator is at the end
        if (buffer == null || !bufferIterator.hasNext()) {
            // Time to refill the buffer
            buffer = kafkaConsumer.poll(3000);

            // Create new iterator
            bufferIterator = buffer.iterator();
        }
    }

    /**
     * Close out Kafka connections.
     */
    public void close() {
        if (kafkaConsumer == null) {
            return;
        }

        // Flush state first?
        flushConsumerState();

        // Close out consumer state manager.
        if (consumerStateManager != null) {
            consumerStateManager.close();
        }

        // Call close on underlying consumer
        kafkaConsumer.close();
        kafkaConsumer = null;
    }

    /**
     * @return - get the defined consumer config.
     */
    public SidelineConsumerConfig getConsumerConfig() {
        return consumerConfig;
    }

    /**
     * @return ConsumerStateManager instance.
     */
    public ConsumerStateManager getConsumerStateManager() {
        return consumerStateManager;
    }

    /**
     * @return - A set of all the partitions currently subscribed to.
     */
    public Set<TopicPartition> getAssignedPartitions() {
        return kafkaConsumer.assignment();
    }

    /**
     * Unsubscribe the consumer from a specific topic/partition.
     * @param topicPartition - the Topic/Partition to stop consuming from.
     * @return boolean, true if unsubscribed, false if it did not.
     */
    public boolean unsubscribeTopicPartition(final TopicPartition topicPartition) {
        // Determine what we're currently assigned to,
        // We clone the returned set so we can modify it.
        Set<TopicPartition> assignedTopicPartitions = Sets.newHashSet(getAssignedPartitions());

        // If it doesn't contain our topic partition
        if (!assignedTopicPartitions.contains(topicPartition)) {
            // For now return false, but maybe we should throw exception?
            return false;
        }
        // Remove it
        assignedTopicPartitions.remove(topicPartition);

        // Reassign consumer
        kafkaConsumer.assign(assignedTopicPartitions);

        // return boolean
        return true;
    }
}
