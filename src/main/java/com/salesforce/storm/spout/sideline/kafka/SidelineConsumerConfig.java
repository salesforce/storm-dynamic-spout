package com.salesforce.storm.spout.sideline.kafka;

import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Wrapper around Kafka's Consumer Config to abstract it and enforce some requirements.
 * TODO: should probably be immutable and use the builder pattern.
 */
public class SidelineConsumerConfig {
    private final Properties kafkaConsumerProperties = new Properties();
    private final String topic;
    private final String consumerId;
    private ConsumerState startState = null;
    private ConsumerState stopState = null;

    /**
     * Settings for consumer state auto commit.  Defaulted to off.
     */
    private boolean consumerStateAutoCommit = false;

    /**
     * If autoCommit is enabled, how often we will flush that state, in milliseconds.
     * Defaults to 15 seconds.
     */
    private long consumerStateAutoCommitIntervalMs = 15000;

    public SidelineConsumerConfig(List<String> brokerHosts, String consumerId, String topic) {
        this.topic = topic;
        this.consumerId = consumerId;

        // Convert list to string
        final String brokerHostsStr = brokerHosts.stream()
                .map(String::toString)
                .collect(Collectors.joining(","));

        // Autocommit is disabled, we handle offset tracking.
        setKafkaConsumerProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        setKafkaConsumerProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");

        // We use our own deserializer interface, so force ByteArray deserialization.
        setKafkaConsumerProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        setKafkaConsumerProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // Other random tunings
        // Default is 65536 bytes, we 4x'd it
        setKafkaConsumerProperty(ConsumerConfig.RECEIVE_BUFFER_CONFIG, "262144");

        // Default value: 2147483647
        setKafkaConsumerProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2147483647");

        // Default value: true, "This check adds some overhead, so it may be disabled in cases seeking extreme performance."
        setKafkaConsumerProperty(ConsumerConfig.CHECK_CRCS_CONFIG, "true");

        /**
         * Defines how quickly a session will time out.
         * If this is set too low, and we are unable to process the returned results fast enough
         * this will cause the client to become disconnected.
         * Setting it too high means that the kafka cluster will take longer to determine if the client
         * has disconnected because of some unannounced error.
         *
         * We default this to 30 seconds.
         */
        setKafkaConsumerProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

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
         * We're acking along at 1,2,3,4,5 so our last completed offset is 5.  Then we determine offset 6
         * is out of range, and jump to offset 10 and start acking 10,11,12,13.  Our largest completed
         * offset will still be 5 because of the hole.  So we'll have issues using smallest.
         *
         * We probably need to bubble up an exception, catch it, log a scary error about
         * missing messages, reset our partition managers acked offset list back to zero.
         */
        setKafkaConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        // Passed in values
        setKafkaConsumerProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerHostsStr);
        setKafkaConsumerProperty(ConsumerConfig.GROUP_ID_CONFIG, this.consumerId);
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
}
