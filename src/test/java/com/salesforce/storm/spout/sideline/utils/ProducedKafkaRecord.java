package com.salesforce.storm.spout.sideline.utils;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Class that wraps relevant information about data that was published to kafka.
 * Used within our tests.
 *
 * @param <K> - Object type of the Key written to kafka.
 * @param <V> - Object type of the Value written to kafka.
 */
public class ProducedKafkaRecord<K, V> {
    private final String topic;
    private final int partition;
    private final long offset;
    private final K key;
    private final V value;

    public ProducedKafkaRecord(String topic, int partition, long offset, K key, V value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
    }

    public static <K,V> ProducedKafkaRecord<K,V> newInstance(RecordMetadata recordMetadata, ProducerRecord<K,V> producerRecord) {
        return new ProducedKafkaRecord<K,V>(
            recordMetadata.topic(),
            recordMetadata.partition(),
            recordMetadata.offset(),
            producerRecord.key(),
            producerRecord.value()
        );
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}
