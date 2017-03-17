package com.salesforce.storm.spout.sideline.config;

/**
 * Start to define some configuration keys.  This may be all for nothing, but its a first pass.
 */
public class SidelineSpoutConfig {
///////////////////////////////////
// Spout Config
///////////////////////////////////

    /**
     * (String) Defines the output stream id to use on the spout.
     */
    public static final String OUTPUT_STREAM_ID = "sideline_spout.output_stream_id";

    /**
     * (String) Defines which Deserializer (Schema?) implementation to use.
     * Should be a full classpath to a class that implements the Deserializer interface.
     * Default Value: "com.salesforce.storm.spout.sideline.kafka.deserializer.Utf8StringDeserializer"
     */
    public static final String DESERIALIZER_CLASS = "sideline_spout.deserializer.class";

///////////////////////////////////
// Kafka Consumer Config
///////////////////////////////////

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

///////////////////////////////////
// Persistence Layer Config
///////////////////////////////////

    /**
     * (String) Defines which PersistenceManager implementation to use.
     * Should be a full classpath to a class that implements the PersistenceManager interface.
     * Default Value: "com.salesforce.storm.spout.sideline.persistence.ZookeeperPersistenceManager"
     */
    public static final String PERSISTENCE_MANAGER_CLASS = "sideline_spout.persistence_manager.class";

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

///////////////////////////////////
// Failed Message Retry Config
///////////////////////////////////

    /**
     * (String) Defines which FailedMsgRetryManager implementation to use.
     * Should be a full classpath to a class that implements the FailedMsgRetryManager interface.
     * Default Value: "com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers.DefaultFailedMsgRetryManager"
     */
    public static final String FAILED_MSG_RETRY_MANAGER_CLASS = "sideline_spout.failed_msg_retry_manager.class";

    /**
     * (int) Defines how many times a failed message will be replayed before just being acked.
     * A value of 0 means tuples will never be retried.
     * A negative value means tuples will be retried forever.
     *
     * Default Value: 25
     * Optional - Only required if you use the DefaultFailedMsgRetryManager implementation.
     */
    public static final String FAILED_MSG_RETRY_MANAGER_MAX_RETRIES = "sideline_spout.failed_msg_retry_manager.max_retries";

    /**
     * (long) Defines how long to wait before retry attempts are made on failed tuples, in milliseconds.
     * Each retry attempt will wait for (number_of_times_message_has_failed * min_retry_time_ms).
     *
     * Example: If a tuple fails 5 times, and the min retry time is set to 1000, it will wait at least (5 * 1000) milliseconds
     * before the next retry attempt.
     *
     * Default Value: 1000
     * Optional - Only required if you use the DefaultFailedMsgRetryManager implementation.
     */
    public static final String FAILED_MSG_RETRY_MANAGER_MIN_RETRY_TIME_MS = "sideline_spout.failed_msg_retry_manager.min_retry_time_ms";

///////////////////////////////////
// Metrics Collection
///////////////////////////////////

    /**
     * (String) Defines which MetricsRecorder implementation to use.
     * Should be a full classpath to a class that implements the MetricsRecorder interface.
     * Default Value: "com.salesforce.storm.spout.sideline.metrics.StormRecorder"
     */
    public static final String METRICS_RECORDER_CLASS = "sideline_spout.metrics.class";

///////////////////////////////////
// Internal Coordinator Config
///////////////////////////////////

    /**
     * (Long) How long our monitor thread will sit around and sleep between monitoring
     * if new VirtualSpouts need to be started up, in Milliseconds.
     * Default Value: 2000
     */
    public static final String MONITOR_THREAD_SLEEP_MS = "sideline_spout.coordinator.monitor_thread_sleep_ms";

    /**
     * (Long) How long we'll wait for all VirtualSpout's to cleanly shut down, before we stop
     * them with force, in Milliseconds.
     * Default Value: 10000
     */
    public static final String MAX_SPOUT_STOP_TIME_MS = "sideline_spout.coordinator.max_spout_stop_time_ms";

    /**
     * (Long) How often we'll make sure each VirtualSpout persists its state, in Milliseconds.
     * Default Value: 30000
     */
    public static final String CONSUMER_STATE_FLUSH_INTERVAL_MS = "sideline_spout.coordinator.consumer_state_flush_interval_ms";

    /**
     * (Integer) The size of the thread pool for running virtual spouts for sideline requests.
     * Default Value: 10
     */
    public static final String MAX_CONCURRENT_VIRTUAL_SPOUTS = "sideline_spout.coordinator.max_concurrent_virtual_spouts";
}
