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

package com.salesforce.storm.spout.dynamic.kafka;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import com.salesforce.storm.spout.dynamic.FactoryManager;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerPeerContext;
import com.salesforce.storm.spout.dynamic.consumer.PartitionDistributor;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerState;
import com.salesforce.storm.spout.dynamic.consumer.PartitionOffsetsManager;
import com.salesforce.storm.spout.dynamic.consumer.Record;
import com.salesforce.storm.spout.dynamic.kafka.deserializer.Deserializer;
import com.salesforce.storm.spout.dynamic.metrics.MetricsRecorder;
import com.salesforce.storm.spout.dynamic.persistence.PersistenceAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
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
import java.util.concurrent.TimeUnit;

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
 * Internally the consumer will recognize that offsets 3 through 5 are all complete, and now mark offset #5 as the last
 * finished offset.
 */
// TODO - rename this class?
public class Consumer implements com.salesforce.storm.spout.dynamic.consumer.Consumer {

    /**
     * Logger for logging logs.
     */
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    /**
     * KafkaConsumer configuration.
     */
    private KafkaConsumerConfig consumerConfig;

    /**
     * KafkaConsumer, for pulling messages off of a Kafka topic.
     */
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
     * MetricsRecorder for reporting metrics from the consumer.
     */
    private MetricsRecorder metricsRecorder;

    /**
     * Our Deserializer, it deserializes messages from Kafka into objects.
     */
    private Deserializer deserializer;

    /**
     * Since offsets are managed on a per partition basis, each namespace/partition has its own ConsumerPartitionStateManagers
     * instance to track its own offset.  The state of these are what gets persisted via the ConsumerStateManager.
     */
    private final PartitionOffsetsManager partitionOffsetsManager = new PartitionOffsetsManager();

    /**
     * Used to buffers messages read from Kafka.
     */
    private ConsumerRecords<byte[], byte[]> buffer = null;

    /**
     * Used to iterate over messages buffered from kafka.
     */
    private Iterator<ConsumerRecord<byte[], byte[]>> bufferIterator = null;

    /**
     * Clock instance, for controlling time based operations.
     */
    private transient Clock clock = Clock.systemUTC();

    /**
     * Last time that status was reported for metrics.
     */
    private Long lastReportedStatusMillis;

    /**
     * Default constructor.
     */
    public Consumer() {
    }

    /**
     * Constructor used for testing, allows for injecting a KafkaConsumer instance.
     */
    Consumer(final KafkaConsumer<byte[], byte[]> kafkaConsumer) {
        this();
        this.kafkaConsumer = kafkaConsumer;
    }

    /**
     * Get the kafka consumer, if it has been retried yet, set it up.
     * @return Kafka consumer
     */
    KafkaConsumer<byte[], byte[]> getKafkaConsumer() {
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
     * @param spoutConfig spout configuration.
     * @param virtualSpoutIdentifier VirtualSpout running this consumer.
     * @param consumerPeerContext defines how many instances in total are running of this consumer.
     * @param persistenceAdapter persistence adapter used to manage state.
     * @param metricsRecorder metrics recorder for reporting metrics from the consumer.
     * @param startingState (optional) if not null, this defines the state at which the consumer should resume from.
     */
    @Override
    public void open(
        final Map<String, Object> spoutConfig,
        final VirtualSpoutIdentifier virtualSpoutIdentifier,
        final ConsumerPeerContext consumerPeerContext,
        final PersistenceAdapter persistenceAdapter,
        final MetricsRecorder metricsRecorder,
        final ConsumerState startingState
    ) {
        // Simple state enforcement.
        if (isOpen) {
            throw new IllegalStateException("Cannot call open more than once.");
        }
        isOpen = true;

        // Build KafkaConsumerConfig from spoutConfig
        final List<String> kafkaBrokers = (List<String>) spoutConfig.get(KafkaConsumerConfig.KAFKA_BROKERS);
        final String topic = (String) spoutConfig.get(KafkaConsumerConfig.KAFKA_TOPIC);

        Preconditions.checkArgument(
            !kafkaBrokers.isEmpty(),
            "Kafka brokers are required"
        );

        Preconditions.checkArgument(
            topic != null && !topic.isEmpty(),
            "Kafka topic is required"
        );

        // TODO ConsumerConfig should use a VirtualSpoutIdentifier
        final KafkaConsumerConfig consumerConfig = new KafkaConsumerConfig(kafkaBrokers, virtualSpoutIdentifier.toString(), topic);

        // Use ConsumerPeerContext to setup how many instances we have.
        consumerConfig.setNumberOfConsumers(
            consumerPeerContext.getTotalInstances()
        );
        consumerConfig.setIndexOfConsumer(
            consumerPeerContext.getInstanceNumber()
        );

        // Create deserializer.
        final Deserializer deserializer = FactoryManager.createNewInstance(
            (String) spoutConfig.get(KafkaConsumerConfig.DESERIALIZER_CLASS)
        );

        // Save references
        this.consumerConfig = consumerConfig;
        this.persistenceAdapter = persistenceAdapter;
        this.metricsRecorder = metricsRecorder;
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

            // If we have a non-null offset
            if (offset != null) {
                // We have a stored offset, so pick up on the partition where we left off
                logger.info(
                    "Resuming namespace {} partition {} at offset {}",
                    topicPartition.topic(),
                    topicPartition.partition(),
                    (offset + 1)
                );
                getKafkaConsumer().seek(topicPartition, (offset + 1));
            } else {
                // We do not have an existing offset saved, so start from the head
                getKafkaConsumer().seekToBeginning(Collections.singletonList(topicPartition));

                // This preserve the 0.10.0.x behavior where a seekToBeginning() call followed by position() on an
                // otherwise empty partition would yield us a -1.  In 0.11.0.x it throws this exception if the
                // partition is empty.
                try {
                    offset = getKafkaConsumer().position(topicPartition) - 1;
                } catch (InvalidOffsetException ex) {
                    logger.info("{} appears to be empty!", topicPartition);
                    offset = -1L;
                }

                logger.info(
                    "Starting at the beginning of namespace {} partition {} => offset {}",
                    topicPartition.topic(), topicPartition.partition(), offset
                );
            }

            // Start tracking offsets on ConsumerPartition
            partitionOffsetsManager.replaceEntry(
                new ConsumerPartition(topicPartition.topic(), topicPartition.partition()),
                offset
            );
        }
    }

    /**
     * Ask the consumer for the next message from Kafka.
     * @return The next Record read from kafka, or null if no such msg is available.
     */
    @Override
    public Record nextRecord() {
        // Fill our buffer if its empty
        fillBuffer();

        reportStatus();

        // Check our iterator for the next message, it's only null at this point if something bad is happening inside
        // of the fileBuffer() method, like it bailed after too much recursion. There will be a lot for that scenario.
        if (bufferIterator == null || !bufferIterator.hasNext()) {
            // Oh no!  No new msg found.
            logger.debug("Unable to fill buffer...nothing new!");
            return null;
        }

        // Iterate to next result
        final ConsumerRecord<byte[], byte[]> nextRecord = bufferIterator.next();

        // Create consumerPartition instance
        final ConsumerPartition consumerPartition = new ConsumerPartition(nextRecord.topic(), nextRecord.partition());

        // Track this new message's state
        partitionOffsetsManager.startOffset(consumerPartition, nextRecord.offset());

        // Deserialize into values
        final Values deserializedValues = getDeserializer().deserialize(
            nextRecord.topic(),
            nextRecord.partition(),
            nextRecord.offset(),
            nextRecord.key(),
            nextRecord.value()
        );

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
     * Reports kafka consumer statistics at an interval.
     * Currently this reports
     *   - "endOffset" or TAIL position of the kafka topic/partition.
     *   - "currentOffset" or the position the consumer is currently per topic/partition.
     *   - "lag" or difference between currentOffset and endOffset position.
     */
    private void reportStatus() {
        // If we've reported status more recently than 30 seconds we skip over this.
        if (lastReportedStatusMillis != null && TimeUnit.MILLISECONDS.toSeconds(clock.millis() - lastReportedStatusMillis) < 30) {
            return;
        }

        // Grab the TAIL offset positions for ONLY the topic partitions this consumer is managing.
        final Map<TopicPartition, Long> endOffsetsMap = getKafkaConsumer().endOffsets(
            getKafkaConsumer().assignment()
        );

        // Grab the consumer's current state for the last offsets it considers completed.
        final ConsumerState consumerState = getCurrentState();

        // Loop through
        for (final Map.Entry<TopicPartition, Long> entry : endOffsetsMap.entrySet()) {
            final TopicPartition topicPartition = entry.getKey();
            final long endOffset = entry.getValue();

            // Assign the value for endOffset for this topic and partition.
            metricsRecorder.assignValue(
                KafkaMetrics.KAFKA_END_OFFSET,
                endOffset,
                topicPartition.topic(), topicPartition.partition()
            );

            // Grab the current offset this consumer is at within this topic and partition.
            final Long currentOffset = consumerState
                .getOffsetForNamespaceAndPartition(topicPartition.topic(), topicPartition.partition());

            // Validate we got a non-null result.
            if (currentOffset == null) {
                // Skip invalid?
                continue;
            }

            // Current offset is the consumers current position.
            metricsRecorder.assignValue(
                KafkaMetrics.KAFKA_CURRENT_OFFSET,
                currentOffset,
                topicPartition.topic(), topicPartition.partition()
            );

            // Calculate "lag" based on (endOffset - currentOffset).
            metricsRecorder.assignValue(
                KafkaMetrics.KAFKA_LAG,
                (endOffset - currentOffset),
                topicPartition.topic(), topicPartition.partition()
            );
        }

        // Update our last reported timestamp to now.
        lastReportedStatusMillis = clock.millis();
    }

    /**
     * Mark a particular offset on a Topic/Partition as having been successfully processed.
     * @param consumerPartition The Topic & Partition the offset belongs to
     * @param offset The offset that should be marked as completed.
     */
    private void commitOffset(final ConsumerPartition consumerPartition, final long offset) {
        // Track internally which offsets we've marked completed
        partitionOffsetsManager.finishOffset(consumerPartition, offset);
    }

    /**
     * Marks a particular offset on a Topic/Partition as having been successfully processed.
     * @param namespace The topic offset belongs to.
     * @param partition The partition the offset belongs to.
     * @param offset The offset that should be marked as completed.
     */
    @Override
    public void commitOffset(final String namespace, final int partition, final long offset) {
        commitOffset(new ConsumerPartition(namespace, partition), offset);
    }

    /**
     * Forces the Consumer's current state to be persisted.
     * @return A copy of the state that was persisted.
     */
    @Override
    public ConsumerState flushConsumerState() {
        // Get the current state
        final ConsumerState consumerState = partitionOffsetsManager.getCurrentState();

        // Persist each partition offset

        for (Map.Entry<ConsumerPartition, Long> entry: consumerState.entrySet()) {
            final ConsumerPartition consumerPartition = entry.getKey();
            final long lastFinishedOffset = entry.getValue();

            // Persist it.
            persistenceAdapter.persistConsumerState(
                getConsumerId(),
                consumerPartition.partition(),
                lastFinishedOffset
            );
        }

        // return the state.
        return consumerState;
    }

    /**
     * Internal method used to fill internal message buffer from kafka.
     */
    private void fillBuffer() {
        // First trip, here we go! (This method recurses and we only let it do that five times, so we start the counter here.)
        fillBuffer(1);
    }

    /**
     * Internal method used to fill internal message buffer from kafka.
     *
     * Limited by the number of trips made. This should only be called from {@link #fillBuffer()}.
     */
    private void fillBuffer(final int trips) {
        // If our buffer is null, or our iterator is at the end
        if (buffer == null || !bufferIterator.hasNext()) {

            // If we have no assigned partitions to consume from, then don't call poll()
            // The underlying consumer call here does NOT make an API call, so this is safe to call within this loop.
            if (getKafkaConsumer().assignment().isEmpty()) {
                // No assigned partitions, nothing to consume :)
                return;
            }

            // Time to refill the buffer
            try {
                buffer = getKafkaConsumer().poll(300);
            } catch (OffsetOutOfRangeException outOfRangeException) {
                // Handle it
                handleOffsetOutOfRange(outOfRangeException);

                // Clear out so we can attempt next time.
                buffer = null;
                bufferIterator = null;

                // Why 5? Because it's less than 6.
                if (trips >= 5) {
                    logger.error(
                        "Attempted to fill the buffer after an OffsetOutOfRangeException, but this was my fifth attempt so I'm bailing."
                    );
                    // nextRecord() will get called by the VirtualSpout instance soon so we're not giving up, just avoiding a StackOverflow
                    // exception on this current run of checks.
                    return;
                }

                fillBuffer(trips + 1);
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
    private void handleOffsetOutOfRange(final OffsetOutOfRangeException outOfRangeException) {
        final Set<TopicPartition> resetPartitions = Sets.newHashSet();

        // Loop over all the partitions in this exception
        for (final TopicPartition topicPartition : outOfRangeException.offsetOutOfRangePartitions().keySet()) {
            // The offset that was in the error
            final long exceptionOffset = outOfRangeException.offsetOutOfRangePartitions().get(topicPartition);
            // What kafka says the last offset is
            final long endingOffset = getKafkaConsumer().endOffsets(Collections.singletonList(topicPartition))
                .get(topicPartition);

            logger.warn("Offset Out of Range for partition {} at offset {}, kafka says last offset in partition is {}",
                topicPartition.partition(), exceptionOffset, endingOffset);

            // We have a hypothesis that the consumer can actually seek past the last message of the topic,
            // this yields this error and we want to catch it and try to back it up just a bit to a place that
            // we can work from.
            if (exceptionOffset >= endingOffset) {
                logger.warn(
                    "OutOfRangeException yielded offset {}, which is past our ending offset of {} for {}",
                    exceptionOffset,
                    endingOffset,
                    topicPartition
                );

                // Seek to the end we found above.  The end may have moved since we last asked, which is why we are not doing seekToEnd()
                getKafkaConsumer().seek(
                    topicPartition,
                    endingOffset
                );

                partitionOffsetsManager.replaceEntry(
                    new ConsumerPartition(topicPartition.topic(), topicPartition.partition()),
                    endingOffset
                );
            } else {
                resetPartitions.add(topicPartition);
            }
        }

        // All of the error'd partitions we need to seek to earliest available position.
        resetPartitionsToEarliest(resetPartitions);
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
        if (topicPartitions.isEmpty()) {
            logger.info("Reset partitions requested with no partitions supplied.");
            return;
        }
        // Seek to earliest for each
        logger.info("Seeking to earliest offset on partitions {}", topicPartitions);
        // If you call this with an empty set it resets everything that the consumer is assigned, which is probably
        // not what you want...
        getKafkaConsumer().seekToBeginning(topicPartitions);

        // Now for each partition
        for (TopicPartition topicPartition: topicPartitions) {
            // Determine the current offset now that we've seeked to earliest
            // We subtract one from this offset and set that as the last "committed" offset.
            final long newOffset = getKafkaConsumer().position(topicPartition) - 1;

            // We need to reset the saved offset to the current value
            // Replace PartitionOffsetManager with new instance from new position.
            logger.info("Partition {} using new earliest offset {}", topicPartition, newOffset);
            partitionOffsetsManager.replaceEntry(
                new ConsumerPartition(topicPartition.topic(), topicPartition.partition()), newOffset
            );
        }
    }

    /**
     * Close out Kafka connections.
     */
    @Override
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
    KafkaConsumerConfig getConsumerConfig() {
        return consumerConfig;
    }

    /**
     * @return PersistenceAdapter instance.
     */
    PersistenceAdapter getPersistenceAdapter() {
        return persistenceAdapter;
    }

    /**
     * @return A set of all the partitions currently subscribed to.
     */
    Set<ConsumerPartition> getAssignedPartitions() {
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
    @Override
    public boolean unsubscribeConsumerPartition(final ConsumerPartition consumerPartitionToUnsubscribe) {
        // Determine what we're currently assigned to,
        // We clone the returned set so we can modify it.
        final Set<ConsumerPartition> assignedTopicPartitions = Sets.newHashSet(getAssignedPartitions());

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
    @Override
    public ConsumerState getCurrentState() {
        return partitionOffsetsManager.getCurrentState();
    }

    /**
     * @return returns our unique consumer identifier.
     */
    String getConsumerId() {
        return getConsumerConfig().getConsumerId();
    }

    /**
     * @return Deserializer instance.
     */
    Deserializer getDeserializer() {
        return deserializer;
    }

    /**
     * This will remove all state from the persistence manager.
     * This is typically called when the consumer has finished reading
     * everything that it wants to read, and does not need to be resumed.
     */
    @Override
    public void removeConsumerState() {
        logger.info("Removing Consumer state for ConsumerId: {}", getConsumerId());

        final Set<ConsumerPartition> consumerPartitions = partitionOffsetsManager.getAllManagedConsumerPartitions();
        for (ConsumerPartition consumerPartition : consumerPartitions) {
            getPersistenceAdapter().clearConsumerState(getConsumerId(), consumerPartition.partition());
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
                new TopicPartition(consumerConfig.getTopic(), partitonId)
            );
        }

        // Return TopicPartitions for our assigned partitions
        return topicPartitions;
    }
}
