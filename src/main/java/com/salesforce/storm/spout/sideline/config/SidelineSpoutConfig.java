package com.salesforce.storm.spout.sideline.config;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.handler.NoopSpoutHandler;
import com.salesforce.storm.spout.sideline.handler.NoopVirtualSpoutHandler;
import com.salesforce.storm.spout.sideline.kafka.Consumer;
import com.salesforce.storm.spout.sideline.retry.DefaultRetryManager;
import com.salesforce.storm.spout.sideline.metrics.LogRecorder;
import com.salesforce.storm.spout.sideline.buffer.RoundRobinBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

// TODO: Rename this to SpoutConfig and rename keys accordingly

/**
 * Start to define some configuration keys.  This may be all for nothing, but its a first pass.
 */
public class SidelineSpoutConfig {
///////////////////////////////////
// Spout Config
///////////////////////////////////

    /**
     * (String) Defines the name of the output stream tuples will be emitted out of.
     */
    public static final String OUTPUT_STREAM_ID = "sideline_spout.output_stream_id";

    /**
     * (String) Defines which Deserializer (Schema?) implementation to use.
     * Should be a full classpath to a class that implements the Deserializer interface.
     */
    public static final String DESERIALIZER_CLASS = "sideline_spout.deserializer.class";

///////////////////////////////////
// Consumer Config
///////////////////////////////////
    /**
     * (String) Defines which Consumer implementation to use.
     * Should be a full classpath to a class that implements the Consumer interface.
     */
    public static final String CONSUMER_CLASS = "sideline_spout.consumer.class";

///////////////////////////////////
// Kafka Consumer Config
///////////////////////////////////

    /**
     * (String) Defines which Kafka topic we will consume messages from.
     */
    public static final String KAFKA_TOPIC = "sideline_spout.kafka.topic";

    /**
     * (List<String>) Holds a list of Kafka Broker hostnames + ports in the following format:
     * ["broker1:9092", "broker2:9092", ...]
     */
    public static final String KAFKA_BROKERS = "sideline_spout.kafka.brokers";

    /**
     * (String) Defines a consumerId prefix to use for all consumers created by the spout.
     * This must be unique to your spout instance, and must not change between deploys.
     */
    public static final String CONSUMER_ID_PREFIX = "sideline_spout.consumer_id_prefix";

///////////////////////////////////
// Persistence Layer Config
///////////////////////////////////

    /**
     * (String) Defines which PersistenceAdapter implementation to use.
     * Should be a full classpath to a class that implements the PersistenceAdapter interface.
     * Default Value: "com.salesforce.storm.spout.sideline.persistence.ZookeeperPersistenceAdapter"
     */
    public static final String PERSISTENCE_ADAPTER_CLASS = "sideline_spout.persistence_adapter.class";

///////////////////////////////////
// Zookeeper Persistence Config
///////////////////////////////////

    /**
     * (List<String>) Holds a list of Zookeeper server Hostnames + Ports in the following format:
     * ["zkhost1:2181", "zkhost2:2181", ...]
     *
     * Optional - Only required if you use the Zookeeper persistence implementation.
     */
    public static final String PERSISTENCE_ZK_SERVERS = "sideline_spout.persistence.zookeeper.servers";

    /**
     * (String) Defines the root path to persist state under.
     * Example: "/sideline-consumer-state"
     *
     * Optional - Only required if you use the Zookeeper persistence implementation.
     */
    public static final String PERSISTENCE_ZK_ROOT = "sideline_spout.persistence.zookeeper.root";

    /**
     * (Integer) Zookeeper session timeout.
     */
    public static final String PERSISTENCE_ZK_SESSION_TIMEOUT = "sideline_spout.persistence.zookeeper.session_timeout";

    /**
     * (Integer) Zookeeper connection timeout.
     */
    public static final String PERSISTENCE_ZK_CONNECTION_TIMEOUT = "sideline_spout.persistence.zookeeper.connection_timeout";

    /**
     * (Integer) Zookeeper retry attempts.
     */
    public static final String PERSISTENCE_ZK_RETRY_ATTEMPTS = "sideline_spout.persistence.zookeeper.retry_attempts";

    /**
     * (Integer) Zookeeper retry interval.
     */
    public static final String PERSISTENCE_ZK_RETRY_INTERVAL = "sideline_spout.persistence.zookeeper.retry_interval";

///////////////////////////////////
// Failed Message Retry Config
///////////////////////////////////

    /**
     * (String) Defines which RetryManager implementation to use.
     * Should be a full classpath to a class that implements the RetryManager interface.
     * Default Value: "com.salesforce.storm.spout.sideline.kafka.retry.DefaultRetryManager"
     */
    public static final String RETRY_MANAGER_CLASS = "sideline_spout.retry_manager.class";

    /**
     * (int) Defines how many times a failed message will be replayed before just being acked.
     * A negative value means tuples will be retried forever.
     * A value of 0 means tuples will never be retried.
     * A positive value means tuples will be retried up to this limit, then dropped.
     *
     * Default Value: -1
     * Optional - Only required if you use the DefaultRetryManager implementation.
     */
    public static final String RETRY_MANAGER_RETRY_LIMIT = "sideline_spout.retry_manager.retry_limit";

    /**
     * (long) Defines how long to wait before retry attempts are made on failed tuples, in milliseconds.
     * Each retry attempt will wait for (number_of_times_message_has_failed * min_retry_time_ms).
     *
     * Example: If a tuple fails 5 times, and the min retry time is set to 1000, it will wait at least (5 * 1000) milliseconds
     * before the next retry attempt.
     *
     * Default Value: 2000 (2 seconds)
     * Optional - Only required if you use the DefaultRetryManager implementation.
     */
    public static final String RETRY_MANAGER_INITIAL_DELAY_MS = "sideline_spout.retry_manager.initial_delay_ms";

    /**
     * (double) Defines how quickly the delay increases after each failed tuple.
     *
     * Example: A value of 2.0 means the delay between retries doubles.  eg. 4, 8, 16 seconds, etc.
     *
     * Default Value: 2.0
     * Optional - Only required if you use the DefaultRetryManager implementation.
     */
    public static final String RETRY_MANAGER_DELAY_MULTIPLIER = "sideline_spout.retry_manager.delay_multiplier";

    /**
     * (long) Defines an upper bound of the max delay time between retried a failed tuple.
     *
     * Default Value: 900000 (15 minutes)
     * Optional - Only required if you use the DefaultRetryManager implementation.
     */
    public static final String RETRY_MANAGER_MAX_DELAY_MS = "sideline_spout.retry_manager.retry_delay_max_ms";

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
     * (String) Defines which MessageBuffer implementation to use.
     * Should be a full classpath to a class that implements the MessageBuffer interface.
     * Default Value: "com.salesforce.storm.spout.sideline.buffer.RoundRobinBuffer"
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
    public static final String MAX_SPOUT_SHUTDOWN_TIME_MS = "sideline_spout.coordinator.max_spout_shutdown_time_ms";

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

    /**
     * (String) Defines which SpoutHandler implementation to use.
     * Should be a fully qualified class path that implements the SpoutHandler interface.
     * Default value: com.salesforce.storm.spout.sideline.handler.NoopSpoutHandler
     */
    public static final String SPOUT_HANDLER_CLASS = "sideline_spout.spout_handler_class";

    /**
     * (String) Defines which VirtualSpoutHandler implementation to use.
     * Should be a fully qualified class path that implements the VirtualSpoutHandler interface.
     * Default value: com.salesforce.storm.spout.sideline.handler.NoopVirtualSpoutHandler
     */
    public static final String VIRTUAL_SPOUT_HANDLER_CLASS = "sideline_spout.virtual_spout_handler_class";

///////////////////////////////////
// Utility Methods.
///////////////////////////////////
    private static final Logger logger = LoggerFactory.getLogger(SidelineSpoutConfig.class);

    /**
     * Utility method to add any unspecified configuration value for items with their defaults.
     * @param config - the config to update.
     * @return - a cloned copy of the config that is updated.
     */
    public static Map<String, Object> setDefaults(Map config) {
        // Clone the map
        Map<String, Object> clonedConfig = Maps.newHashMap();
        clonedConfig.putAll(config);

        // Add in defaults where needed.
        if (!clonedConfig.containsKey(OUTPUT_STREAM_ID)) {
            clonedConfig.put(OUTPUT_STREAM_ID, "default");
            logger.info("Unspecified configuration value for {} using default value {}", OUTPUT_STREAM_ID, clonedConfig.get(OUTPUT_STREAM_ID));
        }
        if (!clonedConfig.containsKey(CONSUMER_CLASS)) {
            // For now default KafkaConsumer
            clonedConfig.put(CONSUMER_CLASS, Consumer.class);
            logger.info("Unspecified configuration value for {} using default value {}", CONSUMER_CLASS, clonedConfig.get(CONSUMER_CLASS));
        }
        if (!clonedConfig.containsKey(RETRY_MANAGER_CLASS)) {
            clonedConfig.put(RETRY_MANAGER_CLASS, DefaultRetryManager.class.getName());
            logger.info("Unspecified configuration value for {} using default value {}", RETRY_MANAGER_CLASS, clonedConfig.get(RETRY_MANAGER_CLASS));
        }
        if (!clonedConfig.containsKey(RETRY_MANAGER_RETRY_LIMIT)) {
            clonedConfig.put(RETRY_MANAGER_RETRY_LIMIT, 25);
            logger.info("Unspecified configuration value for {} using default value {}", RETRY_MANAGER_RETRY_LIMIT, clonedConfig.get(RETRY_MANAGER_RETRY_LIMIT));
        }
        if (!clonedConfig.containsKey(RETRY_MANAGER_INITIAL_DELAY_MS)) {
            clonedConfig.put(RETRY_MANAGER_INITIAL_DELAY_MS, 1000L);
            logger.info("Unspecified configuration value for {} using default value {}", RETRY_MANAGER_INITIAL_DELAY_MS, clonedConfig.get(RETRY_MANAGER_INITIAL_DELAY_MS));
        }
        if (!clonedConfig.containsKey(METRICS_RECORDER_CLASS)) {
            clonedConfig.put(METRICS_RECORDER_CLASS, LogRecorder.class.getName());
            logger.info("Unspecified configuration value for {} using default value {}", METRICS_RECORDER_CLASS, clonedConfig.get(METRICS_RECORDER_CLASS));
        }
        if (!clonedConfig.containsKey(MONITOR_THREAD_INTERVAL_MS)) {
            clonedConfig.put(MONITOR_THREAD_INTERVAL_MS, 2000L);
            logger.info("Unspecified configuration value for {} using default value {}", MONITOR_THREAD_INTERVAL_MS, clonedConfig.get(MONITOR_THREAD_INTERVAL_MS));
        }
        if (!clonedConfig.containsKey(MAX_SPOUT_SHUTDOWN_TIME_MS)) {
            clonedConfig.put(MAX_SPOUT_SHUTDOWN_TIME_MS, 10000L);
            logger.info("Unspecified configuration value for {} using default value {}", MAX_SPOUT_SHUTDOWN_TIME_MS, clonedConfig.get(MAX_SPOUT_SHUTDOWN_TIME_MS));
        }
        if (!clonedConfig.containsKey(CONSUMER_STATE_FLUSH_INTERVAL_MS)) {
            clonedConfig.put(CONSUMER_STATE_FLUSH_INTERVAL_MS, 30000L);
            logger.info("Unspecified configuration value for {} using default value {}", CONSUMER_STATE_FLUSH_INTERVAL_MS, clonedConfig.get(CONSUMER_STATE_FLUSH_INTERVAL_MS));
        }
        if (!clonedConfig.containsKey(MAX_CONCURRENT_VIRTUAL_SPOUTS)) {
            clonedConfig.put(MAX_CONCURRENT_VIRTUAL_SPOUTS, 10);
            logger.info("Unspecified configuration value for {} using default value {}", MAX_CONCURRENT_VIRTUAL_SPOUTS, clonedConfig.get(MAX_CONCURRENT_VIRTUAL_SPOUTS));
        }
        if (!clonedConfig.containsKey(TUPLE_BUFFER_CLASS)) {
            clonedConfig.put(TUPLE_BUFFER_CLASS, RoundRobinBuffer.class.getName());
            logger.info("Unspecified configuration value for {} using default value {}", TUPLE_BUFFER_CLASS, clonedConfig.get(TUPLE_BUFFER_CLASS));
        }
        if (!clonedConfig.containsKey(TUPLE_BUFFER_MAX_SIZE)) {
            clonedConfig.put(TUPLE_BUFFER_MAX_SIZE, 2000);
            logger.info("Unspecified configuration value for {} using default value {}", TUPLE_BUFFER_MAX_SIZE, clonedConfig.get(TUPLE_BUFFER_MAX_SIZE));
        }

        if (!clonedConfig.containsKey(PERSISTENCE_ZK_SESSION_TIMEOUT)) {
            clonedConfig.put(PERSISTENCE_ZK_SESSION_TIMEOUT, 6000);
            logger.info("Unspecified configuration value for {} using default value {}", PERSISTENCE_ZK_SESSION_TIMEOUT, clonedConfig.get(PERSISTENCE_ZK_SESSION_TIMEOUT));
        }

        if (!clonedConfig.containsKey(PERSISTENCE_ZK_CONNECTION_TIMEOUT)) {
            clonedConfig.put(PERSISTENCE_ZK_CONNECTION_TIMEOUT, 6000);
            logger.info("Unspecified configuration value for {} using default value {}", PERSISTENCE_ZK_CONNECTION_TIMEOUT, clonedConfig.get(PERSISTENCE_ZK_CONNECTION_TIMEOUT));
        }

        if (!clonedConfig.containsKey(PERSISTENCE_ZK_RETRY_ATTEMPTS)) {
            clonedConfig.put(PERSISTENCE_ZK_RETRY_ATTEMPTS, 10);
            logger.info("Unspecified configuration value for {} using default value {}", PERSISTENCE_ZK_RETRY_ATTEMPTS, clonedConfig.get(PERSISTENCE_ZK_RETRY_ATTEMPTS));
        }

        if (!clonedConfig.containsKey(PERSISTENCE_ZK_RETRY_INTERVAL)) {
            clonedConfig.put(PERSISTENCE_ZK_RETRY_INTERVAL, 10);
            logger.info("Unspecified configuration value for {} using default value {}", PERSISTENCE_ZK_RETRY_INTERVAL, clonedConfig.get(PERSISTENCE_ZK_RETRY_INTERVAL));
        }

        if (!clonedConfig.containsKey(SPOUT_HANDLER_CLASS)) {
            clonedConfig.put(SPOUT_HANDLER_CLASS, NoopSpoutHandler.class.getName());
            logger.info("Unspecified configuration value for {} using default value {}", SPOUT_HANDLER_CLASS, clonedConfig.get(SPOUT_HANDLER_CLASS));
        }

        if (!clonedConfig.containsKey(VIRTUAL_SPOUT_HANDLER_CLASS)) {
            clonedConfig.put(VIRTUAL_SPOUT_HANDLER_CLASS, NoopVirtualSpoutHandler.class.getName());
            logger.info("Unspecified configuration value for {} using default value {}", VIRTUAL_SPOUT_HANDLER_CLASS, clonedConfig.get(VIRTUAL_SPOUT_HANDLER_CLASS));
        }

        return clonedConfig;
    }
}
