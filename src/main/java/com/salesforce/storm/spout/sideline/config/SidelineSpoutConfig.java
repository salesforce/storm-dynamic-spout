package com.salesforce.storm.spout.sideline.config;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.kafka.retryManagers.DefaultRetryManager;
import com.salesforce.storm.spout.sideline.metrics.LogRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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
     * (List<String>) Holds a list of Kafka Broker hostnames + ports in the following format:
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
     * (String) Defines which RetryManager implementation to use.
     * Should be a full classpath to a class that implements the RetryManager interface.
     * Default Value: "com.salesforce.storm.spout.sideline.kafka.retryManagers.DefaultRetryManager"
     */
    public static final String RETRY_MANAGER_CLASS = "sideline_spout.retry_manager.class";

    /**
     * (int) Defines how many times a failed message will be replayed before just being acked.
     * A value of 0 means tuples will never be retried.
     * A negative value means tuples will be retried forever.
     *
     * Default Value: 25
     * Optional - Only required if you use the DefaultRetryManager implementation.
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
     * Optional - Only required if you use the DefaultRetryManager implementation.
     */
    public static final String FAILED_MSG_RETRY_MANAGER_MIN_RETRY_TIME_MS = "sideline_spout.failed_msg_retry_manager.min_retry_time_ms";

///////////////////////////////////
// Metrics Collection
///////////////////////////////////

    /**
     * (String) Defines which MetricsRecorder implementation to use.
     * Should be a full classpath to a class that implements the MetricsRecorder interface.
     * Default Value: "com.salesforce.storm.spout.sideline.metrics.LogRecorder"
     */
    public static final String METRICS_RECORDER_CLASS = "sideline_spout.metrics.class";

///////////////////////////////////
// Internal Coordinator Config
///////////////////////////////////

    /**
     * (String) Defines which TupleBuffer implementation to use.
     * Should be a full classpath to a class that implements the TupleBuffer interface.
     * Default Value: "com.salesforce.storm.spout.sideline.tupleBuffer.RoundRobinBuffer"
     */
    public static final String TUPLE_BUFFER_CLASS = "sideline_spout.coordinator.tuple_buffer.class";

    /**
     * (int) Defines maximum size of the tuple buffer.  After the buffer reaches this size
     * the internal kafka consumers will be blocked from consuming.
     * Default Value: 2000
     */
    public static final String TUPLE_BUFFER_MAX_SIZE = "sideline_spout.coordinator.tuple_buffer.max_size";

    /**
     * (long) How often our monitor thread will run and watch over its managed virtual spout instances, in milliseconds.
     * Default Value: 2000
     */
    public static final String MONITOR_THREAD_INTERVAL_MS = "sideline_spout.coordinator.monitor_thread_interval_ms";

    /**
     * (long) How long we'll wait for all VirtualSpout's to cleanly shut down, before we stop
     * them with force, in Milliseconds.
     * Default Value: 10000
     */
    public static final String MAX_SPOUT_STOP_TIME_MS = "sideline_spout.coordinator.max_spout_stop_time_ms";

    /**
     * (long) How often we'll make sure each VirtualSpout persists its state, in Milliseconds.
     * Default Value: 30000
     */
    public static final String CONSUMER_STATE_FLUSH_INTERVAL_MS = "sideline_spout.coordinator.consumer_state_flush_interval_ms";

    /**
     * (int) The size of the thread pool for running virtual spouts for sideline requests.
     * Default Value: 10
     */
    public static final String MAX_CONCURRENT_VIRTUAL_SPOUTS = "sideline_spout.coordinator.max_concurrent_virtual_spouts";

///////////////////////////////////
// Utility Methods.
///////////////////////////////////
    private static final Logger logger = LoggerFactory.getLogger(SidelineSpoutConfig.class);

    /**
     * Utility method to add any missing configuration items with their defaults.
     * @param config - the config to update.
     * @return - a cloned copy of the config that is updated.
     */
    public static Map<String, Object> setDefaults(Map config) {
        // Clone the map
        Map<String, Object> clonedConfig = Maps.newHashMap();
        clonedConfig.putAll(config);

        // Add in defaults where needed.
        if (!clonedConfig.containsKey(RETRY_MANAGER_CLASS)) {
            clonedConfig.put(RETRY_MANAGER_CLASS, DefaultRetryManager.class.getName());
            logger.info("Missing configuration {} using default value {}", RETRY_MANAGER_CLASS, clonedConfig.get(RETRY_MANAGER_CLASS));
        }
        if (!clonedConfig.containsKey(FAILED_MSG_RETRY_MANAGER_MAX_RETRIES)) {
            clonedConfig.put(FAILED_MSG_RETRY_MANAGER_MAX_RETRIES, 25);
            logger.info("Missing configuration {} using default value {}", FAILED_MSG_RETRY_MANAGER_MAX_RETRIES, clonedConfig.get(FAILED_MSG_RETRY_MANAGER_MAX_RETRIES));
        }
        if (!clonedConfig.containsKey(FAILED_MSG_RETRY_MANAGER_MIN_RETRY_TIME_MS)) {
            clonedConfig.put(FAILED_MSG_RETRY_MANAGER_MIN_RETRY_TIME_MS, 1000L);
            logger.info("Missing configuration {} using default value {}", FAILED_MSG_RETRY_MANAGER_MIN_RETRY_TIME_MS, clonedConfig.get(FAILED_MSG_RETRY_MANAGER_MIN_RETRY_TIME_MS));
        }
        if (!clonedConfig.containsKey(METRICS_RECORDER_CLASS)) {
            clonedConfig.put(METRICS_RECORDER_CLASS, LogRecorder.class.getName());
            logger.info("Missing configuration {} using default value {}", METRICS_RECORDER_CLASS, clonedConfig.get(METRICS_RECORDER_CLASS));
        }
        if (!clonedConfig.containsKey(MONITOR_THREAD_INTERVAL_MS)) {
            clonedConfig.put(MONITOR_THREAD_INTERVAL_MS, 2000L);
            logger.info("Missing configuration {} using default value {}", MONITOR_THREAD_INTERVAL_MS, clonedConfig.get(MONITOR_THREAD_INTERVAL_MS));
        }
        if (!clonedConfig.containsKey(MAX_SPOUT_STOP_TIME_MS)) {
            clonedConfig.put(MAX_SPOUT_STOP_TIME_MS, 10000L);
            logger.info("Missing configuration {} using default value {}", MAX_SPOUT_STOP_TIME_MS, clonedConfig.get(MAX_SPOUT_STOP_TIME_MS));
        }
        if (!clonedConfig.containsKey(CONSUMER_STATE_FLUSH_INTERVAL_MS)) {
            clonedConfig.put(CONSUMER_STATE_FLUSH_INTERVAL_MS, 30000L);
            logger.info("Missing configuration {} using default value {}", CONSUMER_STATE_FLUSH_INTERVAL_MS, clonedConfig.get(CONSUMER_STATE_FLUSH_INTERVAL_MS));
        }
        if (!clonedConfig.containsKey(MAX_CONCURRENT_VIRTUAL_SPOUTS)) {
            clonedConfig.put(MAX_CONCURRENT_VIRTUAL_SPOUTS, 10);
            logger.info("Missing configuration {} using default value {}", MAX_CONCURRENT_VIRTUAL_SPOUTS, clonedConfig.get(MAX_CONCURRENT_VIRTUAL_SPOUTS));
        }
        if (!clonedConfig.containsKey(TUPLE_BUFFER_CLASS)) {
            clonedConfig.put(TUPLE_BUFFER_CLASS, "com.salesforce.storm.spout.sideline.tupleBuffer.RoundRobinBuffer");
            logger.info("Missing configuration {} using default value {}", TUPLE_BUFFER_CLASS, clonedConfig.get(TUPLE_BUFFER_CLASS));
        }
        if (!clonedConfig.containsKey(TUPLE_BUFFER_MAX_SIZE)) {
            clonedConfig.put(TUPLE_BUFFER_MAX_SIZE, 2000);
            logger.info("Missing configuration {} using default value {}", TUPLE_BUFFER_MAX_SIZE, clonedConfig.get(TUPLE_BUFFER_MAX_SIZE));
        }

        return clonedConfig;
    }
}
