package com.salesforce.storm.spout.sideline.kafka;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Wrapper around Kafka's Consumer Config to abstract it and enforce some requirements.
 * TODO: should probably be immutable and use the builder pattern.
 */
public class ConsumerConfig {

    private final Properties kafkaConsumerProperties = new Properties();
    private final String topic;
    private final String consumerId;
    private int numberOfConsumers;
    private int indexOfConsumer;

    /**
     * Settings for consumer state auto commit.  Defaulted to off.
     */
    private boolean consumerStateAutoCommit = false;

    /**
     * If autoCommit is enabled, how often we will flush that state, in milliseconds.
     * Defaults to 15 seconds.
     */
    private long consumerStateAutoCommitIntervalMs = 15000;

    /**
     * Constructor.
     * @param brokerHosts - List of Kafka brokers in format of ["host1:9092", "host2:9092", ...]
     * @param consumerId - What consumerId the consumer should use.
     * @param topic - What topic the consumer should consume from.
     */
    public ConsumerConfig(final List<String> brokerHosts, final String consumerId, final String topic) {
        this.topic = topic;
        this.consumerId = consumerId;

        // Convert list to string
        final String brokerHostsStr = brokerHosts.stream()
                .map(String::toString)
                .collect(Collectors.joining(","));

        // Autocommit is disabled, we handle offset tracking.
        setKafkaConsumerProperty(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        setKafkaConsumerProperty(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");

        // We use our own deserializer interface, so force ByteArray deserialization.
        setKafkaConsumerProperty(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
        setKafkaConsumerProperty(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());

        // Other random tunings
        // Default is 65536 bytes, we 4x'd it
        setKafkaConsumerProperty(org.apache.kafka.clients.consumer.ConsumerConfig.RECEIVE_BUFFER_CONFIG, "262144");

        // Default value: 2147483647
        setKafkaConsumerProperty(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2147483647");

        // Default value: true, "This check adds some overhead, so it may be disabled in cases seeking extreme performance."
        setKafkaConsumerProperty(org.apache.kafka.clients.consumer.ConsumerConfig.CHECK_CRCS_CONFIG, "true");

        /**
         * Defines how quickly a session will time out.
         * If this is set too low, and we are unable to process the returned results fast enough
         * this will cause the client to become disconnected.
         * Setting it too high means that the kafka cluster will take longer to determine if the client
         * has disconnected because of some unannounced error.
         *
         * We default this to 30 seconds.
         */
        setKafkaConsumerProperty(org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        /**
         * If an offset is deemed too old and not available how should we handle it?
         * Values:
         * "earliest" - Use the smallest value available (start from head).
         *              Using this value means we missed some messages... but we'll start from
         *              the oldest message available.
         *
         * "latest" - Use the largest offset available (start from tail).
         *             Using this value means we'll miss A LOT of messages... not likely what we want.
         *
         * "none" - Throw an exception if we are unable to get an offset.
         *           Using this value means we'll bubble up an exception... its now the users issue to deal with
         *           and resolve.
         *
         * Note: If we use latest we miss too many messages.
         *
         * Note: If we use earliest the following problem arises:
         * We ack offsets 1,2,3,4,5 so our last completed offset is 5.  Then we determine offset 6
         * is out of range, and jump to offset 10 and start acking 10,11,12,13.  Our largest completed
         * offset will still be 5 because of the hole.  So we'll have issues using smallest.
         *
         * We probably need to bubble up an exception, catch it, log a scary error about
         * missing messages, reset our partition managers acked offset list back to zero.
         */
        setKafkaConsumerProperty(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        // Passed in values
        setKafkaConsumerProperty(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerHostsStr);
        setKafkaConsumerProperty(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, this.consumerId);
    }

    public String getConsumerId() {
        return consumerId;
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

    public boolean isConsumerStateAutoCommit() {
        return consumerStateAutoCommit;
    }

    public void setConsumerStateAutoCommit(boolean consumerStateAutoCommit) {
        this.consumerStateAutoCommit = consumerStateAutoCommit;
    }

    public long getConsumerStateAutoCommitIntervalMs() {
        return consumerStateAutoCommitIntervalMs;
    }

    public void setConsumerStateAutoCommitIntervalMs(long consumerStateAutoCommitIntervalMs) {
        this.consumerStateAutoCommitIntervalMs = consumerStateAutoCommitIntervalMs;
    }

    public int getNumberOfConsumers() {
        return numberOfConsumers;
    }

    public void setNumberOfConsumers(int numberOfConsumers) {
        this.numberOfConsumers = numberOfConsumers;
    }

    public int getIndexOfConsumer() {
        return indexOfConsumer;
    }

    public void setIndexOfConsumer(int indexOfConsumer) {
        this.indexOfConsumer = indexOfConsumer;
    }
}
