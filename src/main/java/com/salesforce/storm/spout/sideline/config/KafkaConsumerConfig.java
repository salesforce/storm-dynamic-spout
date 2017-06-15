package com.salesforce.storm.spout.sideline.config;

import com.salesforce.storm.spout.sideline.config.annotation.Documentation;

import java.util.List;

/**
 * Configuration items for Kafka consumer.
 */
public class KafkaConsumerConfig {
///////////////////////////////////
// Kafka Consumer Config
///////////////////////////////////

    /**
     * (String) Defines which Deserializer (Schema?) implementation to use.
     * Should be a full classpath to a class that implements the Deserializer interface.
     */
    @Documentation(
        category = Documentation.Category.KAFKA,
        description = "Defines which Deserializer (Schema?) implementation to use. "
            + "Should be a full classpath to a class that implements the Deserializer interface.",
        type = String.class
    )
    public static final String DESERIALIZER_CLASS = "spout.kafka.deserializer.class";

    /**
     * (String) Defines which Kafka topic we will consume messages from.
     */
    @Documentation(
        category = Documentation.Category.KAFKA,
        description = "Defines which Kafka topic we will consume messages from.",
        type = String.class
    )
    public static final String KAFKA_TOPIC = "spout.kafka.topic";

    /**
     * (List<String>) Holds a list of Kafka Broker hostnames + ports in the following format:
     * ["broker1:9092", "broker2:9092", ...]
     */
    @Documentation(
        category = Documentation.Category.KAFKA,
        description = "Holds a list of Kafka Broker hostnames + ports in the following format: "
            + "[\"broker1:9092\", \"broker2:9092\", ...]",
        type = List.class
    )
    public static final String KAFKA_BROKERS = "spout.kafka.brokers";

    // TODO: Alias for VSpoutIdPrefix
    /**
     * (String) Defines a consumerId prefix to use for all consumers created by the spout.
     * This must be unique to your spout instance, and must not change between deploys.
     */
    @Documentation(
        category = Documentation.Category.KAFKA,
        description = "Defines a consumerId prefix to use for all consumers created by the spout. "
            + "This must be unique to your spout instance, and must not change between deploys.",
        type = String.class
    )
    public static final String CONSUMER_ID_PREFIX = SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX;
}
