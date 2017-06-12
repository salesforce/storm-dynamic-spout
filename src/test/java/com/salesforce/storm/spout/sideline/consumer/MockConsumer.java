package com.salesforce.storm.spout.sideline.consumer;

import com.salesforce.storm.spout.sideline.ConsumerPartition;
import com.salesforce.storm.spout.sideline.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MockConsumer implements Consumer {

    public static String topic = "MyTopic";
    public static List<Integer> partitions = Collections.singletonList(1);

    @Override
    public void open(Map<String, Object> spoutConfig, VirtualSpoutIdentifier virtualSpoutIdentifier, ConsumerPeerContext consumerPeerContext, PersistenceAdapter persistenceAdapter, ConsumerState startingState) {

    }

    @Override
    public void close() {

    }

    @Override
    public Record nextRecord() {
        return null;
    }

    @Override
    public void commitOffset(String namespace, int partition, long offset) {

    }

    @Override
    public ConsumerState getCurrentState() {
        ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();

        for (Integer partition : partitions) {
            builder.withPartition(topic, partition, 1L);
        }

        return builder.build();
    }

    @Override
    public ConsumerState flushConsumerState() {
        return null;
    }

    @Override
    public double getMaxLag() {
        return 0;
    }

    @Override
    public void removeConsumerState() {

    }

    @Override
    public PersistenceAdapter getPersistenceAdapter() {
        return null;
    }

    @Override
    public boolean unsubscribeConsumerPartition(ConsumerPartition consumerPartitionToUnsubscribe) {
        return false;
    }
}
