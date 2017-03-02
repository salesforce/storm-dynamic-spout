package com.salesforce.storm.spout.sideline.kafka;

import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Wrpaper around Kafka's Consumer Config to abstract it and enforce some requirements.
 */
public class SidelineConsumerConfig {
    private final Properties kafkaConsumerProperties = new Properties();
    private final String topic;
    private final String consumerId;
    private ConsumerState startState = null;
    private ConsumerState stopState = null;

    /**
     * How often we'll flush consumer state to the persistence layer, in milliseconds.
     */
    private long flushStateTimeMS = 15000;  // 15 seconds

    public SidelineConsumerConfig(List<String> brokerHosts, String consumerId, String topic) {
        this.topic = topic;
        this.consumerId = consumerId;

        // Convert list to string
        final String brokerHostsStr = brokerHosts.stream()
                .map(String::toString)
                .collect(Collectors.joining(","));

        // Default settings
        setKafkaConsumerProperty("session.timeout.ms", "1000");
        setKafkaConsumerProperty("enable.auto.commit", "false");
        setKafkaConsumerProperty("auto.commit.interval.ms", "10000");
        setKafkaConsumerProperty("session.timeout.ms", "30000");
        setKafkaConsumerProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        setKafkaConsumerProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // Passed in values
        setKafkaConsumerProperty("bootstrap.servers", brokerHostsStr);
        setKafkaConsumerProperty("group.id", this.consumerId);
    }

    public String getConsumerId() {
        return consumerId;
    }

    public ConsumerState getStartState() {
        return startState;
    }

    public void setStartState(ConsumerState startState) {
        this.startState = startState;
    }

    public ConsumerState getStopState() {
        return stopState;
    }

    public void setStopState(ConsumerState stopState) {
        this.stopState = stopState;
    }

    public String getTopic() {
        return topic;
    }

    public void setKafkaConsumerProperty(String name, String value) {
        kafkaConsumerProperties.put(name, value);
    }

    public String getKafkaConsumerProperty(String name) {
        return (String) kafkaConsumerProperties.get(name);
    }

    public Properties getKafkaConsumerProperties() {
        return kafkaConsumerProperties;
    }

    public void setFlushStateTimeMS(long flushStateTimeMS) {
        this.flushStateTimeMS = flushStateTimeMS;
    }

    public long getFlushStateTimeMS() {
        return flushStateTimeMS;
    }
}
