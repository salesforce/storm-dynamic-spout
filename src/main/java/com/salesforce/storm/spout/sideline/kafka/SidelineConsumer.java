package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.sideline.persistence.PersistenceManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A high level kafka consumer that handles state/log offset management in a way that supports
 * marking messages as being processed in non-sequential order.  On consumer restarts, this implementation
 * errs on the side of re-playing a previously processed message (at least once semantics) over accidentally skipping
 * un-processed messages.  This means there exists certain scenarios where it could replay previously processed
 * messages.
 *
 * How does this Consumer track completed offsets?
 * This consumer will emit messages out the same sequential order as it consumes it from Kafka.  However there
 * is no guarantee what order those messages will get processed by a storm topology.  The topology could process
 * and ack those messages in any order.  So lets imagine the following scenario:
 *   Emit Offsets: 0,1,2,3,4,5
 *
 * For whatever reason offset #3 takes longer to process, so we get acks in the following order back from storm:
 *   Ack Offsets: 0,1,4,5,2
 *
 * At this point internally this consumer knows it has processed the above offsets, but is missing offset #3.
 * This consumer tracks completed offsets sequentially, meaning it will mark offset #2 as being the last finished offset
 * because it is the largest offset that we know we have acked every offset preceding it.  If at this point the topology
 * was stopped, and the consumer shut down, when the topology was redeployed, this consumer would resume consuming at
 * offset #3, re-emitting the following:
 *   Emit Offsets: 3,4,5
 *
 * Now imagine the following acks come in:
 *   Ack Offsets: 4,5,3
 *
 * Internally the consumer will recognize that 3 -> 5 are all complete, and now mark offset #5 as the last finished offset.
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
    private boolean isOpen = false;

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
     * Get a list of the partition for the given kafka topic.
     * @return List of partition for the topic
     */
    public List<PartitionInfo> getPartitions() {
        return getKafkaConsumer().partitionsFor(getConsumerConfig().getTopic());
    }

    /**
     * Get the kafka consumer, if it has been retried yet, set it up.
     * @return Kafka consumer
     */
    private KafkaConsumer getKafkaConsumer() {
        // If kafkaConsumer is not null, we'll create one.
        // If it is NOT null, we'll re-use the instance we got passed in from the constructor.
        // Typically you'd pass in an instance for testing.
        if (kafkaConsumer == null) {
            // Construct new consumer
            kafkaConsumer = new KafkaConsumer<>(getConsumerConfig().getKafkaConsumerProperties());
        }

        return kafkaConsumer;
    }

    /**
     * Handles connecting to the Kafka cluster, determining which partitions to subscribe to,
     * and based on previously saved state from ConsumerStateManager, seek to the last positions processed on
     * each partition.
     *
     * Warning: Consumes from ALL partitions.
     *
     * @param startingState Starting state of the consumer
     */
    public void open(ConsumerState startingState) {
        open(
            startingState,
            getPartitions()
        );
    }

    /**
     * Handles connecting to the Kafka cluster, determining which partitions to subscribe to,
     * and based on previously saved state from ConsumerStateManager, seek to the last positions processed on
     * each partition.
     *
     * Warning: Consumes from ALL partitions.
     *
     * @param startingState Starting state of the consumer
     * @param partitions The partitions to consume from
     */
    public void open(ConsumerState startingState, List<PartitionInfo> partitions) {
        // Simple state enforcement.
        if (isOpen) {
            throw new RuntimeException("Cannot call open more than once...");
        }
        isOpen = true;

        // If we have a starting offset, lets persist it
        if (startingState != null) {
            // If we persist it here, when sideline consumer starts up, it should start from this position
            // when it reads out its state from persistenceManager.
            persistenceManager.persistConsumerState(getConsumerId(), startingState);
        }

        // Load initial positions,
        ConsumerState initialState = persistenceManager.retrieveConsumerState(getConsumerId());
        if (initialState == null) {
            // if null returned, use an empty ConsumerState instance.
            initialState = ConsumerState.builder().build();
        }

        final KafkaConsumer kafkaConsumer = getKafkaConsumer();

        // Assign them all to this consumer
        List<TopicPartition> allTopicPartitions = Lists.newArrayList();
        for (PartitionInfo partition : partitions) {
            allTopicPartitions.add(new TopicPartition(partition.topic(), partition.partition()));
        }
        kafkaConsumer.assign(allTopicPartitions);

        // Seek to specific positions based on our initial state
        List<TopicPartition> noStatePartitions = Lists.newArrayList();
        for (PartitionInfo partition : partitions) {
            final TopicPartition availableTopicPartition = new TopicPartition(partition.topic(), partition.partition());
            Long offset = initialState.getOffsetForTopicAndPartition(availableTopicPartition);
            if (offset == null) {
                // Un-started partitions should "begin" at the earliest available offset
                // We determine this in the resetPartitionsToEarliest() method.
                noStatePartitions.add(availableTopicPartition);
            } else {
                // We start consuming at our offset + 1, otherwise we'd replay a previously "finished" offset.
                logger.info("Resuming topic {} partition {} at offset {}", availableTopicPartition.topic(), availableTopicPartition.partition(), (offset + 1));
                getKafkaConsumer().seek(availableTopicPartition, (offset + 1));

                // Starting managing offsets for this partition
                // Set our completed offset to our 'completed' offset, not it + 1, otherwise we could skip the next uncompleted offset.
                partitionStateManagers.put(availableTopicPartition, new PartitionOffsetManager(availableTopicPartition.topic(), availableTopicPartition.partition(), offset));
            }
        }
        if (!noStatePartitions.isEmpty()) {
            logger.info("Starting from earliest on TopicPartitions(s): {}", noStatePartitions);
            resetPartitionsToEarliest(noStatePartitions);
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

        // Return the record
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
        // If we have auto commit off, don't commit
        if (!getConsumerConfig().isConsumerStateAutoCommit()) {
            return false;
        }

        // Get current system time.
        final long now = getClock().millis();

        // Set initial state if not defined
        if (lastFlushTime == 0) {
            lastFlushTime = now;
            return false;
        }

        // Determine if we should flush.
        if ((now - lastFlushTime) > getConsumerConfig().getConsumerStateAutoCommitIntervalMs()) {
            flushConsumerState();
            lastFlushTime = now;
            return true;
        }
        return false;
    }

    /**
     * Forces the Consumer's current state to be persisted.
     * @return - A copy of the state that was persisted.
     */
    public ConsumerState flushConsumerState() {
        // Create a consumer state builder.
        ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();

        // Loop thru all of our partition managers
        for (Map.Entry<TopicPartition, PartitionOffsetManager> entry: partitionStateManagers.entrySet()) {
            // Get the current offset for each partition.
            builder.withPartition(entry.getKey(), entry.getValue().lastFinishedOffset());
        }

        // Build
        final ConsumerState consumerState = builder.build();

        // Persist state.
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
            try {
                buffer = kafkaConsumer.poll(3000);
            } catch (OffsetOutOfRangeException outOfRangeException) {
                // Handle it
                handleOffsetOutOfRange(outOfRangeException);

                // Clear out so we can attempt next time.
                buffer = null;
                bufferIterator = null;

                // TODO: heh... this should be bounded most likely...
                fillBuffer();
                return;
            }

            // Create new iterator
            bufferIterator = buffer.iterator();
        }
    }

    /**
     * This method handles when a partition seek/retrieve request was out of bounds.
     * This happens in two scenarios:
     *  1 - The offset is too old and was cleaned up / removed by the broker.
     *  2 - The offset just plain does not exist.
     *
     * This is particularly nasty in that if the poll() was able to pull SOME messages from
     * SOME partitions before the exception was thrown, those messages are considered "consumed"
     * by KafkaClient, and there's no way to get them w/o seeking back to them for those partitions.
     *
     * @param outOfRangeException - the exception that was raised by the consumer.
     */
    private void handleOffsetOutOfRange(OffsetOutOfRangeException outOfRangeException) {
        // Grab the partitions that had errors
        final Set outOfRangePartitions = outOfRangeException.partitions();

        // Grab all partitions our consumer is subscribed too.
        Set<TopicPartition> allAssignedPartitions = getKafkaConsumer().assignment();

        // Loop over all subscribed partitions
        for (TopicPartition assignedTopicPartition : allAssignedPartitions) {
            // If this partition was out of range
            // we simply log an error about data loss, and skip them for now.
            if (outOfRangePartitions.contains(assignedTopicPartition)) {
                final long offset = outOfRangeException.offsetOutOfRangePartitions().get(assignedTopicPartition);
                logger.error("DATA LOSS ERROR - offset {} for partition {} was out of range", offset, assignedTopicPartition);
                continue;
            }

            // This partition did NOT have any errors, but its possible that we "lost" some messages
            // during the poll() call.  This partition needs to seek back to its previous position
            // before the exception was thrown.
            final long offset = partitionStateManagers.get(assignedTopicPartition).lastStartedOffset();
            logger.info("Backtracking {} offset to {}", assignedTopicPartition, offset);
            getKafkaConsumer().seek(assignedTopicPartition, offset);
        }

        // All of the error'd partitions we need to seek to earliest available position.
        resetPartitionsToEarliest(outOfRangePartitions);
    }

    /**
     * Internal method that given a collection of topic partitions will find the earliest
     * offset for that partition, seek the underlying consumer to it, and reset its internal
     * offset tracking to that new position.
     *
     * This should be used when no state exists for a given partition, OR if the offset
     * requested was too old.
     * @param topicPartitions - the collection of offsets to reset offsets for to the earliest position.
     */
    private void resetPartitionsToEarliest(Collection<TopicPartition> topicPartitions) {
        // Seek to earliest for each
        logger.info("Seeking to earliest offset on partitions {}", topicPartitions);
        getKafkaConsumer().seekToBeginning(topicPartitions);

        // Now for each partition
        for (TopicPartition topicPartition: topicPartitions) {
            // Determine the current offset now that we've seeked to earliest
            // We subtract one from this offset and set that as the last "committed" offset.
            final long newOffset = getKafkaConsumer().position(topicPartition) - 1;

            // We need to reset the saved offset to the current value
            // Replace PartitionOffsetManager with new instance from new position.
            logger.info("Partition {} using new earliest offset {}", topicPartition, newOffset);
            partitionStateManagers.put(topicPartition, new PartitionOffsetManager(topicPartition.topic(), topicPartition.partition(), newOffset));
        }
    }

    /**
     * Close out Kafka connections.
     */
    public void close() {
        // If our consumer is already null
        if (kafkaConsumer == null) {
            // Do nothing.
            return;
        }

        // Close out persistence manager.
        persistenceManager.close();

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

    /**
     * Returns what the consumer considers its current "finished" state to be.  This means the highest
     * offsets for all partitions its consuming that it has tracked as having been complete.
     *
     * @return - Returns the Consumer's current state.
     */
    public ConsumerState getCurrentState() {
        final ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();

        for (Map.Entry<TopicPartition, PartitionOffsetManager> entry : partitionStateManagers.entrySet()) {
            builder.withPartition(entry.getKey(), entry.getValue().lastFinishedOffset());
        }
        return builder.build();
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

    /**
     * This will remove all state from the persistence manager.
     * This is typically called when the consumer has finished reading
     * everything that it wants to read, and does not need to be resumed.
     */
    public void removeConsumerState() {
        logger.info("Removing Consumer state for ConsumerId: {}", getConsumerId());
        getPersistenceManager().clearConsumerState(getConsumerId());
    }
}
