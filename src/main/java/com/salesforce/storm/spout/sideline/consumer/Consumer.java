package com.salesforce.storm.spout.sideline.consumer;

import com.salesforce.storm.spout.sideline.ConsumerPartition;
import com.salesforce.storm.spout.sideline.kafka.ConsumerConfig;
import com.salesforce.storm.spout.sideline.kafka.ConsumerState;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Deserializer;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;

/**
 * First pass interface for a Consumer.
 * This was created by looking at our Kafka based consumer and pulling all of the public methods.
 */
public interface Consumer {
    void open(final ConsumerConfig consumerConfig, final PersistenceAdapter persistenceAdapter, final Deserializer deserializer, final ConsumerState startingState);

    // Next entry to process
    public Record nextRecord();

    // Which of these three is best?
    void commitOffset(final ConsumerPartition consumerPartition, final long offset);
    void commitOffset(final String namespace, final int partition, final long offset);
    void commitOffset(final Record record);

    // State related methods
    public ConsumerState getCurrentState();
    public ConsumerState flushConsumerState();

    // This feels like it should be removed as well? Maybe?
    void removeConsumerState();

    // Maybe the logic in VSpout that needs this can be refactored within KafkaConsumer?
    boolean unsubscribeConsumerPartition(final ConsumerPartition consumerPartitionToUnsubscribe);

    // Not sure if this is needed or not.
    String getConsumerId();

    double getMaxLag();

    void close();
}
