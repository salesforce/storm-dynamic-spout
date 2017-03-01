package com.salesforce.storm.spout.sideline.config;

/**
 * Start to define some configuration keys.  This may be all for nothing, but its a first pass.
 */
public class SidelineSpoutConfig {
    /**
     * (String) Holds which topic we should be consuming from.
     */
    public static final String KAFKA_TOPIC = "sideline_spout.kafka.topic";

    /**
     * (List<String>) Holds a list of Kafka Broker Hostnames + Ports in the following format:
     * ["broker1:9092", "broker2:9092", ...]
     */
    public static final String KAFKA_BROKERS = "sideline_spout.kafka.brokers";

    /**
     * (String) Defines a consumerId prefix to use for all consumers created by the spout.
     */
    public static final String CONSUMER_ID_PREFIX = "sideline_spout.consumer_id_prefix";

    /**
     * (String) Defines the output stream id to use on the spout
     */
    public static final String OUTPUT_STREAM_ID = "sideline_spout.output_stream_id";

    /**
     * (List<String>) Holds a list of Zookeeper server Hostnames + Ports in the following format:
     * ["zkhost1:2181", "zkhost2:2181", ...]
     *
     * Optional - Only required if you use the Zookeeper persistence implementation.
     */
    public static final String PERSISTENCE_ZK_SERVERS = "sideline_spout.persistence.zk_servers";

    /**
     * (String) Defines the root path to persist state under.
     * Example: "/sideline-consumer-state"
     *
     * Optional - Only required if you use the Zookeeper persistence implementation.
     */
    public static final String PERSISTENCE_ZK_ROOT = "sideline_spout.persistence.zk_root";
}
