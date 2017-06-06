package com.salesforce.storm.spout.sideline.consumer;

import com.salesforce.storm.spout.sideline.ConsumerPartition;
import com.salesforce.storm.spout.sideline.kafka.ConsumerConfig;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Deserializer;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;

/**
 * This defines the interface for Consumers.
 * Implementing the following methods will allow you to build a drop in Consumer that processes data
 * within a VirtualSpout instance.
 */
public interface Consumer {
    /**
     * This method is called once, after your implementation has been constructed.
     * This method should handle all setup and configuration.
     * @param consumerConfig TODO this needs to be replaced with SpoutConfig.
     * @param persistenceAdapter The persistence adapter used to manage any state.
     * @param deserializer TODO this needs to be removed
     * @param startingState (Optional) If not null, This defines the state at which the consumer should resume from.
     */
    void open(final ConsumerConfig consumerConfig, final PersistenceAdapter persistenceAdapter, final Deserializer deserializer, final ConsumerState startingState);

    /**
     * This method is called when a VirtualSpout is shutting down.  It should perform any necessary cleanup.
     */
    void close();

    /**
     * @return The next Record that should be processed.
     */
    Record nextRecord();

    /**
     * Called when a specific Record has completed processing successfully.
     * @param namespace Namespace the record originated from.
     * @param partition Partition the record originated from.
     * @param offset Offset the record originated from.
     */
    void commitOffset(final String namespace, final int partition, final long offset);

    // State related methods

    /**
     * @return The Consumer's current state.
     */
    ConsumerState getCurrentState();

    /**
     * Requests the consumer to persist state to the Persistence adapter.
     * @return The Consumer's current state.
     */
    ConsumerState flushConsumerState();

    /**
     * This is likely to change in signature in the future to return some standardized object instead of a double.
     * @return The consumer's maximum lag.
     */
    double getMaxLag();

    // The following methods are likely to be removed in future refactorings.
    void removeConsumerState();
    PersistenceAdapter getPersistenceAdapter();

    // Maybe the logic in VSpout that needs this can be refactored within KafkaConsumer?
    boolean unsubscribeConsumerPartition(final ConsumerPartition consumerPartitionToUnsubscribe);
}
