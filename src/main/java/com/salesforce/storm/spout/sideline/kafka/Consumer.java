package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.sideline.ConsumerPartition;
import com.salesforce.storm.spout.sideline.PartitionDistributor;
import com.salesforce.storm.spout.sideline.PartitionOffsetManager;
import com.salesforce.storm.spout.sideline.consumer.Record;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Deserializer;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Utf8StringDeserializer;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
// TODO - rename this class
public class Consumer implements com.salesforce.storm.spout.sideline.consumer.Consumer {
    // For logging.
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    // Kafka Consumer Instance and its Config.
    private ConsumerConfig consumerConfig;
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
    private PersistenceAdapter persistenceAdapter;

    /**
     * Our Deserializer, it deserializes messages from Kafka into objects.
     */
    private Deserializer deserializer;

    /**
     * Since offsets are managed on a per partition basis, each namespace/partition has its own ConsumerPartitionStateManagers
     * instance to track its own offset.  The state of these are what gets persisted via the ConsumerStateManager.
     */
    private final Map<ConsumerPartition, PartitionOffsetManager> partitionStateManagers = Maps.newHashMap();

    /**
     * Used to buffers messages read from Kafka.
     */
    private ConsumerRecords<byte[], byte[]> buffer = null;
    private Iterator<ConsumerRecord<byte[], byte[]>> bufferIterator = null;

    /**
     * Used to control how often we flush state using PersistenceAdapter.
     */
    private transient Clock clock = Clock.systemUTC();
    private long lastFlushTime = 0;

    // Default constructor
    public Consumer() {

    }

    public Consumer(final KafkaConsumer<byte[], byte[]> kafkaConsumer) {
        this();
        this.kafkaConsumer = kafkaConsumer;
    }

    /**
     * Constructor.
     */
    // TODO REMOVE CONSTRUCTOR
    public Consumer(final ConsumerConfig consumerConfig, final PersistenceAdapter persistenceAdapter, final Deserializer deserializer) {
        this.consumerConfig = consumerConfig;
        this.persistenceAdapter = persistenceAdapter;
        this.deserializer = deserializer;
    }

    /**
     * Get the kafka consumer, if it has been retried yet, set it up.
     * @return Kafka consumer
     */
    private KafkaConsumer<byte[], byte[]> getKafkaConsumer() {
        // If kafkaConsumer is not null, we'll create one.
        // If it is NOT null, we'll re-use the instance we got passed in from the constructor.
        // Typically you'd pass in an instance for testing.
        if (kafkaConsumer == null) {
            // Construct new consumer
            kafkaConsumer = new KafkaConsumer<>(getConsumerConfig().getKafkaConsumerProperties());
        }

        return kafkaConsumer;
    }

    // TODO REMOVE
    public void open(final ConsumerState startingState) {
        open(getConsumerConfig(), getPersistenceAdapter(), getDeserializer(), startingState);
    }

    /**
     * Handles connecting to the Kafka cluster, determining which partitions to subscribe to,
     * and based on previously saved state from ConsumerStateManager, seek to the last positions processed on
     * each partition.
     * @param consumerConfig Configuration for our consumer
     * @param persistenceAdapter Implementation of PersistenceAdapter to use for storing consumer state.
     * @param deserializer Deserializer instance to use.
     * @param startingState Starting state that the consumer should use.  optional, can be null.
     */
    @Override
    public void open(final ConsumerConfig consumerConfig, final PersistenceAdapter persistenceAdapter, final Deserializer deserializer, final ConsumerState startingState) {
        // Simple state enforcement.
        if (isOpen) {
            throw new IllegalStateException("Cannot call open more than once.");
        }
        isOpen = true;

        // Save references
        this.consumerConfig = consumerConfig;
        this.persistenceAdapter = persistenceAdapter;
        this.deserializer = deserializer;

        // Get partitions
        List<TopicPartition> topicPartitions = getPartitions();
        if (topicPartitions.isEmpty()) {
            throw new RuntimeException("Cannot assign partitions when there are none!");
        }

        logger.info("Assigning namespace and partitions = {}", topicPartitions);

        // Assign our consumer to the given partitions
        getKafkaConsumer().assign(topicPartitions);

        for (TopicPartition topicPartition : topicPartitions) {
            Long startingOffset = null;

            if (startingState != null) {
                startingOffset = startingState.getOffsetForNamespaceAndPartition(topicPartition.topic(), topicPartition.partition());
            }

            // Check to see if we have an existing offset saved for this partition
            Long offset = persistenceAdapter.retrieveConsumerState(getConsumerId(), topicPartition.partition());

            if (offset == null && startingOffset != null) {
                offset = startingOffset;
            }

            if (offset != null) {
                // We have a stored offset, so pick up on the partition where we left off
                logger.info("Resuming namespace {} partition {} at offset {}", topicPartition.topic(), topicPartition.partition(), (offset + 1));
                getKafkaConsumer().seek(topicPartition, (offset + 1));
            } else {
                // We do not have an existing offset saved, so start from the head
                getKafkaConsumer().seekToBeginning(Collections.singletonList(topicPartition));
                offset = getKafkaConsumer().position(topicPartition) - 1;
                logger.info("Starting at the beginning of namespace {} partition {} => offset {}", topicPartition.topic(), topicPartition.partition(), offset);
            }

            partitionStateManagers.put(
                new ConsumerPartition(topicPartition.topic(), topicPartition.partition()),
                new PartitionOffsetManager(
                    topicPartition.topic(),
                    topicPartition.partition(),
                    offset
                )
            );
        }
    }

    /**
     * Ask the consumer for the next message from Kafka.
     * @return The next Record read from kafka, or null if no such msg is available.
     */
    public Record nextRecord() {
        // Fill our buffer if its empty
        fillBuffer();

        // Check our iterator for the next message
        if (!bufferIterator.hasNext()) {
            // Oh no!  No new msg found.
            logger.debug("Unable to fill buffer...nothing new!");
            return null;
        }

        // Iterate to next result
        final ConsumerRecord<byte[], byte[]> nextRecord = bufferIterator.next();

        // Create consumerPartition instance
        final ConsumerPartition consumerPartition = new ConsumerPartition(nextRecord.topic(), nextRecord.partition());

        // Track this new message's state
        partitionStateManagers.get(consumerPartition).startOffset(nextRecord.offset());

        // Deserialize into values
        final Values deserializedValues = getDeserializer().deserialize(nextRecord.topic(), nextRecord.partition(), nextRecord.offset(), nextRecord.key(), nextRecord.value());

        // Handle null
        if (deserializedValues == null) {
            // Failed to deserialize, just ack and return null?
            logger.debug("Deserialization returned null");

            // Mark as completed.
            commitOffset(consumerPartition, nextRecord.offset());

            // return null
            return null;
        }

        // Return the record
        return new Record(nextRecord.topic(), nextRecord.partition(), nextRecord.offset(), deserializedValues);
    }

    /**
     * Mark a particular offset on a Topic/Partition as having been successfully processed.
     * @param consumerPartition The Topic & Partition the offset belongs to
     * @param offset The offset that should be marked as completed.
     */
    public void commitOffset(final ConsumerPartition consumerPartition, final long offset) {
        // Track internally which offsets we've marked completed
        partitionStateManagers.get(consumerPartition).finishOffset(offset);

        // Occasionally flush
        timedFlushConsumerState();
    }

    /**
     * Mark a particular offset on a Topic/Partition as having been successfully processed.
     * @param namespace The topic offset belongs to.
     * @param partition The partition the offset belongs to.
     * @param offset The offset that should be marked as completed.
     */
    public void commitOffset(final String namespace, final int partition, final long offset) {
        commitOffset(new ConsumerPartition(namespace, partition), offset);
    }

    /**
     * Mark a particular message as having been successfully processed.
     * @param record The record to mark as completed.
     */
    public void commitOffset(final Record record) {
        commitOffset(record.getNamespace(), record.getPartition(), record.getOffset());
    }

    /**
     * Conditionally flushes the consumer state to the persistence layer based
     * on a time-out condition.
     *
     * @return True if we flushed state, false if we didn't
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
     * @return A copy of the state that was persisted.
     */
    public ConsumerState flushConsumerState() {
        // Create a consumer state builder.
        ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();

        // Loop through all of our partition managers
        for (Map.Entry<ConsumerPartition, PartitionOffsetManager> entry: partitionStateManagers.entrySet()) {
            final ConsumerPartition topicPartition = entry.getKey();

            final long lastFinishedOffset = entry.getValue().lastFinishedOffset();

            // Set the current offset for each partition.
            builder.withPartition(topicPartition, lastFinishedOffset);

            // Persist it.
            persistenceAdapter.persistConsumerState(
                getConsumerId(),
                topicPartition.partition(),
                lastFinishedOffset
            );
        }

        return builder.build();
    }

    /**
     * Internal method used to fill internal message buffer from kafka.
     * Maybe this should be marked private.
     */
    private void fillBuffer() {
        // If our buffer is null, or our iterator is at the end
        if (buffer == null || !bufferIterator.hasNext()) {
            // Time to refill the buffer
            try {
                buffer = getKafkaConsumer().poll(300);
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
     * This means when we roll back, we may replay some messages :/
     *
     * @param outOfRangeException The exception that was raised by the consumer.
     */
    private void handleOffsetOutOfRange(OffsetOutOfRangeException outOfRangeException) {
        // Grab the partitions that had errors
        final Set outOfRangePartitions = outOfRangeException.partitions();

        // Grab all partitions our consumer is subscribed too.
        Set<ConsumerPartition> allAssignedPartitions = getAssignedPartitions();

        // Loop over all subscribed partitions
        for (ConsumerPartition assignedTopicPartition : allAssignedPartitions) {
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
            getKafkaConsumer().seek(new TopicPartition(assignedTopicPartition.namespace(), assignedTopicPartition.partition()), offset);
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
     * @param topicPartitions The collection of offsets to reset offsets for to the earliest position.
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
            partitionStateManagers.put(new ConsumerPartition(topicPartition.topic(), topicPartition.partition()), new PartitionOffsetManager(topicPartition.topic(), topicPartition.partition(), newOffset));
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
        persistenceAdapter.close();

        // Call close on underlying consumer
        kafkaConsumer.close();
        kafkaConsumer = null;
    }

    /**
     * @return The defined consumer config.
     */
    public ConsumerConfig getConsumerConfig() {
        return consumerConfig;
    }

    /**
     * @return PersistenceAdapter instance.
     */
    public PersistenceAdapter getPersistenceAdapter() {
        return persistenceAdapter;
    }

    /**
     * @return A set of all the partitions currently subscribed to.
     */
    public Set<ConsumerPartition> getAssignedPartitions() {
        // Create our return set using abstracted TopicPartition
        Set<ConsumerPartition> assignedPartitions = new HashSet<>();

        // Loop over resumes from underlying kafka consumer
        for (TopicPartition topicPartition: getKafkaConsumer().assignment()) {
            // Convert object type
            assignedPartitions.add(new ConsumerPartition(topicPartition.topic(), topicPartition.partition()));
        }
        // Return immutable copy of our list.
        return Collections.unmodifiableSet(assignedPartitions);
    }

    /**
     * Unsubscribe the consumer from a specific topic/partition.
     * @param consumerPartitionToUnsubscribe the Topic/Partition to stop consuming from.
     * @return boolean, true if unsubscribed, false if it did not.
     */
    public boolean unsubscribeConsumerPartition(final ConsumerPartition consumerPartitionToUnsubscribe) {
        // Determine what we're currently assigned to,
        // We clone the returned set so we can modify it.
        Set<ConsumerPartition> assignedTopicPartitions = Sets.newHashSet(getAssignedPartitions());

        // If it doesn't contain our namespace partition
        if (!assignedTopicPartitions.contains(consumerPartitionToUnsubscribe)) {
            // For now return false, but maybe we should throw exception?
            return false;
        }
        // Remove it
        assignedTopicPartitions.remove(consumerPartitionToUnsubscribe);

        // Convert to TopicPartitions to interact with underlying kafka consumer.
        Set<TopicPartition> reassignedTopicPartitions = new HashSet<>();
        for (ConsumerPartition consumerPartition : assignedTopicPartitions) {
            reassignedTopicPartitions.add(new TopicPartition(consumerPartition.namespace(), consumerPartition.partition()));
        }

        // Reassign consumer
        kafkaConsumer.assign(reassignedTopicPartitions);

        // return boolean
        return true;
    }

    /**
     * Returns what the consumer considers its current "finished" state to be.  This means the highest
     * offsets for all partitions its consuming that it has tracked as having been complete.
     *
     * @return The Consumer's current state.
     */
    public ConsumerState getCurrentState() {
        final ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();

        for (Map.Entry<ConsumerPartition, PartitionOffsetManager> entry : partitionStateManagers.entrySet()) {
            builder.withPartition(entry.getKey(), entry.getValue().lastFinishedOffset());
        }
        return builder.build();
    }

    /**
     * @return returns our unique consumer identifier.
     */
    public String getConsumerId() {
        return getConsumerConfig().getConsumerId();
    }

    /**
     * @return return our clock implementation.  Useful for testing.
     */
    protected Clock getClock() {
        return clock;
    }

    /**
     * @return Deserializer instance.
     */
    Deserializer getDeserializer() {
        return deserializer;
    }

    /**
     * For injecting a clock implementation.  Useful for testing.
     * @param clock the clock implementation to use.
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

        final Set<ConsumerPartition> topicPartitions = partitionStateManagers.keySet();

        for (ConsumerPartition topicPartition : topicPartitions) {
            getPersistenceAdapter().clearConsumerState(getConsumerId(), topicPartition.partition());
        }
    }

    /**
     * Get the partitions that this particular consumer instance should consume from.
     * @return List of partitions to consume from
     */
    private List<TopicPartition> getPartitions() {
        // Ask Kafka for all of the partitions that are available
        final List<PartitionInfo> allPartitionInfos = getKafkaConsumer().partitionsFor(consumerConfig.getTopic());

        // Convert all of our partition info objects into a primitive list of the partition ids
        final int[] allPartitionIds = allPartitionInfos.stream().map(PartitionInfo::partition).mapToInt(i -> i).toArray();

        // Perform our calculation
        final int[] partitionsIds = PartitionDistributor.calculatePartitionAssignment(
            consumerConfig.getNumberOfConsumers(),
            consumerConfig.getIndexOfConsumer(),
            allPartitionIds
        );

        // Convert our partition ids back to a list of TopicPartition records
        final List<TopicPartition> topicPartitions = Lists.newArrayList();

        for (final int partitonId : partitionsIds) {
            topicPartitions.add(
                new TopicPartition(
                    consumerConfig.getTopic(),
                    partitonId
                )
            );
        }

        // Return TopicPartitions for our assigned partitions
        return topicPartitions;
    }

    private Map<MetricName, ? extends Metric> metrics() {
        return getKafkaConsumer().metrics();
    }

    /**
     * @return The maximum lag of the consumer.
     */
    public double getMaxLag() {
        for (Map.Entry<MetricName, ? extends Metric> entry : metrics().entrySet()) {
            if (entry.getKey().name().equals("records-lag-max")) {
                return entry.getValue().value();
            }
        }
        // Fall thru return value?
        return -1.0;
    }
}
