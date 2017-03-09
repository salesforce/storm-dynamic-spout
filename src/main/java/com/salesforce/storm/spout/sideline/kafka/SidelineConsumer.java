package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import com.salesforce.storm.spout.sideline.persistence.PersistenceManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
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
    private final PersistenceManager persistenceManager;

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
     * Used to control how often we flush state using PersistenceManager.
     */
    private transient Clock clock = Clock.systemUTC();
    private long lastFlushTime = 0;

    /**
     * Constructor.
     * @param consumerConfig - Configuration for our consumer
     * @param persistenceManager - Implementation of PersistenceManager to use for storing consumer state.
     */
    public SidelineConsumer(SidelineConsumerConfig consumerConfig, PersistenceManager persistenceManager) {
        this.consumerConfig = consumerConfig;
        this.persistenceManager = persistenceManager;
    }

    /**
     * This constructor is used to inject a mock KafkaConsumer for tests.
     * @param consumerConfig - Configuration for our consumer
     * @param persistenceManager - Implementation of PersistenceManager to use for storing consumer state.
     * @param kafkaConsumer - Inject a mock KafkaConsumer for tests.
     */
    protected SidelineConsumer(SidelineConsumerConfig consumerConfig, PersistenceManager persistenceManager, KafkaConsumer<byte[], byte[]> kafkaConsumer) {
        this(consumerConfig, persistenceManager);
        this.kafkaConsumer = kafkaConsumer;
    }

    /**
     * Handles connecting to the Kafka cluster, determining which partitions to subscribe to,
     * and based on previously saved state from ConsumerStateManager, seek to the last positions processed on
     * each partition.
     */
    public void open(ConsumerState startingState) {
        // Simple state enforcement.
        if (hasCalledConnect) {
            throw new RuntimeException("Cannot call open more than once...");
        }
        hasCalledConnect = true;

        // If we have a starting offset, lets persist it
        if (startingState != null) {
            // If we persist it here, when sideline consumer starts up, it should start from this position.
            // Maybe this is a bit dirty and we should interact w/ SidelineConsumer instead?
            // TODO - If resuming a topology, will this overwrite existing state?
            persistenceManager.persistConsumerState(getConsumerId(), startingState);
        }

        // Load initial positions, if null returned, use an empty ConsumerState instance.
        ConsumerState initialState = persistenceManager.retrieveConsumerState(getConsumerId());
        if (initialState == null) {
            initialState = new ConsumerState();
        }

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
                // Unstarted partitions should "begin" at -1
                noStatePartitions.add(availableTopicPartition);
                offset = -1L;
            } else {
                // We start consuming at our offset + 1, otherwise we'd replay a previously "finished" offset.
                logger.info("Resuming topic {} partition {} at offset {}", availableTopicPartition.topic(), availableTopicPartition.partition(), (offset + 1));
                kafkaConsumer.seek(availableTopicPartition, offset + 1);
                logger.info("Will resume at offset {}", kafkaConsumer.position(availableTopicPartition));
            }

            // Starting managing offsets for this partition
            // Set our completed offset to our 'completed' offset, not it + 1, otherwise we could skip the next uncompleted offset.
            partitionStateManagers.put(availableTopicPartition, new PartitionOffsetManager(availableTopicPartition.topic(), availableTopicPartition.partition(), offset));
        }
        if (!noStatePartitions.isEmpty()) {
            logger.info("Starting from head on TopicPartitions(s): {}", noStatePartitions);
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
        // Track internally which offsets we've marked completed
        partitionStateManagers.get(topicPartition).finishOffset(offset);

        // TODO: We can probably drop this.
        // Occasionally flush
        timedFlushConsumerState();
    }

    /**
     * Mark a particular message as having been successfully processed.
     * @param consumerRecord - the consumer record to mark as completed.
     */
    public void commitOffset(ConsumerRecord consumerRecord) {
        commitOffset(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), consumerRecord.offset());
    }

    /**
     * Conditionally flushes the consumer state to the persistence layer based
     * on a time-out condition.
     *
     * @return boolean - true if we flushed state, false if we didn't
     */
    protected boolean timedFlushConsumerState() {
        // Set initial state if not defined
        if (lastFlushTime == 0) {
            lastFlushTime = Instant.now(getClock()).toEpochMilli();
            return false;
        }

        // Determine if we should flush.
        final long currentTime = Instant.now(getClock()).toEpochMilli();
        if (currentTime - lastFlushTime > getConsumerConfig().getFlushStateTimeMS()) {
            flushConsumerState();
            lastFlushTime = currentTime;
            return true;
        }
        return false;
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
        logger.debug("Flushing consumer state {}", consumerState);
        persistenceManager.persistConsumerState(getConsumerId(), consumerState);

        // Return the state that was persisted.
        return consumerState;
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

            // Count is potentially expensive, remove the call from the info line.
            //logger.debug("Done filling buffer with {} entries", buffer.count());
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
        if (persistenceManager != null) {
            persistenceManager.close();
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
     * @return PersistenceManager instance.
     */
    public PersistenceManager getPersistenceManager() {
        return persistenceManager;
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

    public ConsumerState getCurrentState() {
        ConsumerState consumerState = new ConsumerState();
        for (TopicPartition topicPartition : partitionStateManagers.keySet()) {
            consumerState.setOffset(topicPartition, partitionStateManagers.get(topicPartition).lastFinishedOffset());
        }
        return consumerState;
    }

    /**
     * @return - returns our unique consumer identifier.
     */
    public String getConsumerId() {
        return getConsumerConfig().getConsumerId();
    }

    /**
     * @return - return our clock implementation.  Useful for testing.
     */
    protected Clock getClock() {
        return clock;
    }

    /**
     * For injecting a clock implementation.  Useful for testing.
     * @param clock - the clock implementation to use.
     */
    protected void setClock(Clock clock) {
        this.clock = clock;
    }
}
