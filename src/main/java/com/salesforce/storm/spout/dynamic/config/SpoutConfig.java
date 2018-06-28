/**
 * Copyright (c) 2017, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 *   disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * * Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or promote products
 *   derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.salesforce.storm.spout.dynamic.config;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.config.annotation.Documentation;
import com.salesforce.storm.spout.dynamic.handler.NoopSpoutHandler;
import com.salesforce.storm.spout.dynamic.handler.NoopVirtualSpoutHandler;
import com.salesforce.storm.spout.dynamic.kafka.Consumer;
import com.salesforce.storm.spout.dynamic.retry.DefaultRetryManager;
import com.salesforce.storm.spout.dynamic.metrics.LogRecorder;
import com.salesforce.storm.spout.dynamic.buffer.RoundRobinBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Dynamic Spout Configuration Directives.
 */
public class SpoutConfig {

    /**
     * (String) Defines the name of the output stream tuples will be emitted out of.
     */
    @Documentation(
        description = "Defines the name of the output stream tuples will be emitted out of.",
        type = String.class
    )
    public static final String OUTPUT_STREAM_ID = "spout.output_stream_id";

    /**
     * (List[String]) Defines the output fields that the spout will emit as a list of field names.
     * Example: ["field1", "field2", ...]
     *
     * Also supported as a single string of comma separated values: "field1, field2, ..."
     * Or as an explicitly defined Fields object.
     */
    @Documentation(
        description = "Defines the output fields that the spout will emit as a list of field names.",
        type = List.class
    )
    public static final String OUTPUT_FIELDS = "spout.output_fields";

    /**
     * (String) Defines the name of the output stream tuples that have permanently failed will be emitted out of.
     */
    @Documentation(
        description = "Defines the name of the output stream tuples that have permanently failed be emitted out of.",
        type = String.class
    )
    public static final String PERMANENTLY_FAILED_OUTPUT_STREAM_ID = "spout.permanently_failed_output_stream_id";


///////////////////////////////////
// Consumer Config
///////////////////////////////////

    /**
     * (String) Defines which Consumer implementation to use.
     * Should be a full classpath to a class that implements the Consumer interface.
     */
    @Documentation(
        description = "Defines which Consumer implementation to use. "
        + "Should be a full classpath to a class that implements the Consumer interface.",
        type = String.class
    )
    public static final String CONSUMER_CLASS = "spout.consumer.class";

///////////////////////////////////
// Persistence Layer Config
///////////////////////////////////

    /**
     * (String) Defines which PersistenceAdapter implementation to use.
     * Should be a full classpath to a class that implements the PersistenceAdapter interface.
     * Default Value: "com.salesforce.storm.spout.dynamic.persistence.ZookeeperPersistenceAdapter"
     */
    @Documentation(
        category = Documentation.Category.PERSISTENCE,
        description = "Defines which PersistenceAdapter implementation to use. "
        + "Should be a full classpath to a class that implements the PersistenceAdapter interface.",
        type = String.class,
        required = true
    )
    public static final String PERSISTENCE_ADAPTER_CLASS = "spout.persistence_adapter.class";

///////////////////////////////////
// Zookeeper Persistence Config
///////////////////////////////////

    /**
     * (List[String) Holds a list of Zookeeper server Hostnames + Ports in the following format:
     * ["zkhost1:2181", "zkhost2:2181", ...]
     *
     * Optional - Only required if you use the Zookeeper persistence implementation.
     */
    @Documentation(
        category = Documentation.Category.PERSISTENCE_ZOOKEEPER,
        description = "Holds a list of Zookeeper server Hostnames + Ports in the following format: "
        + "[\"zkhost1:2181\", \"zkhost2:2181\", ...]",
        type = List.class
    )
    public static final String PERSISTENCE_ZK_SERVERS = "spout.persistence.zookeeper.servers";

    /**
     * (String) Defines the root path to persist state under.
     * Example: "/consumer-state"
     *
     * Optional - Only required if you use the Zookeeper persistence implementation.
     */
    @Documentation(
        category = Documentation.Category.PERSISTENCE_ZOOKEEPER,
        description = "Defines the root path to persist state under. Example: \"/consumer-state\"",
        type = String.class
    )
    public static final String PERSISTENCE_ZK_ROOT = "spout.persistence.zookeeper.root";

    /**
     * (Integer) Zookeeper session timeout.
     */
    @Documentation(
        category = Documentation.Category.PERSISTENCE_ZOOKEEPER,
        description = "Zookeeper session timeout.",
        type = Integer.class
    )
    public static final String PERSISTENCE_ZK_SESSION_TIMEOUT = "spout.persistence.zookeeper.session_timeout";

    /**
     * (Integer) Zookeeper connection timeout.
     */
    @Documentation(
        category = Documentation.Category.PERSISTENCE_ZOOKEEPER,
        description = "Zookeeper connection timeout.",
        type = Integer.class
    )
    public static final String PERSISTENCE_ZK_CONNECTION_TIMEOUT = "spout.persistence.zookeeper.connection_timeout";

    /**
     * (Integer) Zookeeper retry attempts.
     */
    @Documentation(
        category = Documentation.Category.PERSISTENCE_ZOOKEEPER,
        description = "Zookeeper retry attempts.",
        type = Integer.class
    )
    public static final String PERSISTENCE_ZK_RETRY_ATTEMPTS = "spout.persistence.zookeeper.retry_attempts";

    /**
     * (Integer) Zookeeper retry interval.
     */
    @Documentation(
        category = Documentation.Category.PERSISTENCE_ZOOKEEPER,
        description = "Zookeeper retry interval.",
        type = Integer.class
    )
    public static final String PERSISTENCE_ZK_RETRY_INTERVAL = "spout.persistence.zookeeper.retry_interval";

///////////////////////////////////
// Failed Message Retry Config
///////////////////////////////////
    
    /**
     * (String) Defines which RetryManager implementation to use.
     * Should be a full classpath to a class that implements the RetryManager interface.
     * Default Value: "com.salesforce.storm.spout.dynamic.retry.DefaultRetryManager"
     */
    @Documentation(
        description = "Defines which RetryManager implementation to use. "
        + "Should be a full classpath to a class that implements the RetryManager interface.",
        type = String.class,
        required = true
    )
    public static final String RETRY_MANAGER_CLASS = "spout.retry_manager.class";

    /**
     * (int) Defines how many times a failed message will be replayed before just being acked.
     * A negative value means tuples will be retried forever.
     * A value of 0 means tuples will never be retried.
     * A positive value means tuples will be retried up to this limit, then dropped.
     *
     * Default Value: -1
     * Optional - Only required if you use the DefaultRetryManager implementation.
     */
    @Documentation(
        description = "Defines how many times a failed message will be replayed before just being acked. "
        + "A negative value means tuples will be retried forever. A value of 0 means tuples will never be retried. "
        + "A positive value means tuples will be retried up to this limit, then dropped.",
        type = Integer.class
    )
    public static final String RETRY_MANAGER_RETRY_LIMIT = "spout.retry_manager.retry_limit";

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
    @Documentation(
        description = "Defines how long to wait before retry attempts are made on failed tuples, in milliseconds. "
        + "Each retry attempt will wait for (number_of_times_message_has_failed * min_retry_time_ms). "
        + "Example: If a tuple fails 5 times, and the min retry time is set to 1000, it will wait at least "
        + "(5 * 1000) milliseconds before the next retry attempt.",
        type = Long.class
    )
    public static final String RETRY_MANAGER_INITIAL_DELAY_MS = "spout.retry_manager.initial_delay_ms";

    /**
     * (double) Defines how quickly the delay increases after each failed tuple.
     *
     * Example: A value of 2.0 means the delay between retries doubles.  eg. 4, 8, 16 seconds, etc.
     *
     * Default Value: 2.0
     * Optional - Only required if you use the DefaultRetryManager implementation.
     */
    @Documentation(
        description = "Defines how quickly the delay increases after each failed tuple. "
        + "Example: A value of 2.0 means the delay between retries doubles.  eg. 4, 8, 16 seconds, etc.",
        type = Double.class
    )
    public static final String RETRY_MANAGER_DELAY_MULTIPLIER = "spout.retry_manager.delay_multiplier";

    /**
     * (long) Defines an upper bound of the max delay time between retried a failed tuple.
     *
     * Default Value: 900000 (15 minutes)
     * Optional - Only required if you use the DefaultRetryManager implementation.
     */
    @Documentation(
        description = "Defines an upper bound of the max delay time between retried a failed tuple.",
        type = Long.class
    )
    public static final String RETRY_MANAGER_MAX_DELAY_MS = "spout.retry_manager.retry_delay_max_ms";

///////////////////////////////////
// Metrics Collection
///////////////////////////////////

    /**
     * (String) Defines which MetricsRecorder implementation to use.
     * Should be a full classpath to a class that implements the MetricsRecorder interface.
     * Default Value: "com.salesforce.storm.spout.dynamic.metrics.LogRecorder"
     */
    @Documentation(
        description = "Defines which MetricsRecorder implementation to use. "
        + "Should be a full classpath to a class that implements the MetricsRecorder interface.",
        type = String.class
    )
    public static final String METRICS_RECORDER_CLASS = "spout.metrics.class";

    /**
     * (boolean) Defines the time bucket to group metrics together under.
     * Default Value: 60
     */
    @Documentation(
        description = "Defines the time bucket to group metrics together under.",
        type = Integer.class
    )
    public static final String METRICS_RECORDER_TIME_BUCKET = "spout.metrics.time_bucket";

    /**
     * (boolean) Defines if MetricsRecord instance should include the taskId in the metric key.
     * Default Value: false
     */
    @Documentation(
        description = "Defines if MetricsRecorder instance should include the taskId in the metric key.",
        type = Boolean.class
    )
    public static final String METRICS_RECORDER_ENABLE_TASK_ID_PREFIX = "spout.metrics.enable_task_id_prefix";

///////////////////////////////////
// Internal Coordinator Config
///////////////////////////////////

    /**
     * (String) Defines which MessageBuffer implementation to use.
     * Should be a full classpath to a class that implements the MessageBuffer interface.
     * Default Value: com.salesforce.storm.spout.dynamic.buffer.RoundRobinBuffer
     */
    @Documentation(
        description = "Defines which MessageBuffer implementation to use. "
        + "Should be a full classpath to a class that implements the MessageBuffer interface.",
        type = String.class
    )
    public static final String TUPLE_BUFFER_CLASS = "spout.coordinator.tuple_buffer.class";

    /**
     * (int) Defines maximum size of the tuple buffer.  After the buffer reaches this size
     * the internal kafka consumers will be blocked from consuming.
     * Default Value: 2000
     */
    @Documentation(
        description = "Defines maximum size of the tuple buffer.  After the buffer reaches this size the internal "
        + "kafka consumers will be blocked from consuming.",
        type = Integer.class
    )
    public static final String TUPLE_BUFFER_MAX_SIZE = "spout.coordinator.tuple_buffer.max_size";

    /**
     * (long) How often our monitor thread will run and watch over its managed virtual spout instances, in milliseconds.
     * Default Value: 2000
     */
    @Documentation(
        description = "How often our monitor thread will run and watch over its managed virtual spout instances, in milliseconds.",
        type = Long.class
    )
    public static final String MONITOR_THREAD_INTERVAL_MS = "spout.coordinator.monitor_thread_interval_ms";

    /**
     * (long) How long we'll wait for all VirtualSpout's to cleanly shut down, before we stop
     * them with force, in Milliseconds.
     * Default Value: 10000
     */
    @Documentation(
        description = "How long we'll wait for all VirtualSpout's to cleanly shut down, before we stop them with force, in Milliseconds.",
        type = Long.class
    )
    public static final String MAX_SPOUT_SHUTDOWN_TIME_MS = "spout.coordinator.max_spout_shutdown_time_ms";

    /**
     * (long) How often we'll make sure each VirtualSpout persists its state, in Milliseconds.
     * Default Value: 30000
     */
    @Documentation(
        description = "How often we'll make sure each VirtualSpout persists its state, in Milliseconds.",
        type = Long.class
    )
    public static final String CONSUMER_STATE_FLUSH_INTERVAL_MS = "spout.coordinator.consumer_state_flush_interval_ms";

    // TODO: Category needs to change?
    /**
     * (String) Defines a consumerId prefix to use for all consumers created by the spout.
     * This must be unique to your spout instance, and must not change between deploys.
     */
    @Documentation(
        description = "Defines a VirtualSpoutId prefix to use for all VirtualSpouts created by the spout. "
        + "This must be unique to your spout instance, and must not change between deploys.",
        type = String.class
    )
    public static final String VIRTUAL_SPOUT_ID_PREFIX = "spout.coordinator.virtual_spout_id_prefix";

    /**
     * (int) The size of the thread pool for running virtual spouts.
     * Default Value: 10
     */
    @Documentation(
        description = "The size of the thread pool for running virtual spouts.",
        type = Integer.class
    )
    public static final String MAX_CONCURRENT_VIRTUAL_SPOUTS = "spout.coordinator.max_concurrent_virtual_spouts";

    /**
     * (String) Defines which SpoutHandler implementation to use.
     * Should be a fully qualified class path that implements the SpoutHandler interface.
     * Default value: com.salesforce.storm.spout.dynamic.handler.NoopSpoutHandler
     */
    @Documentation(
        description = "Defines which SpoutHandler implementation to use. "
        + "Should be a fully qualified class path that implements the SpoutHandler interface.",
        type = String.class
    )
    public static final String SPOUT_HANDLER_CLASS = "spout.spout_handler_class";

    /**
     * (String) Defines which VirtualSpoutHandler implementation to use.
     * Should be a fully qualified class path that implements the VirtualSpoutHandler interface.
     * Default value: com.salesforce.storm.spout.dynamic.handler.NoopVirtualSpoutHandler
     */
    @Documentation(
        description = "Defines which VirtualSpoutHandler implementation to use. "
        + "Should be a fully qualified class path that implements the VirtualSpoutHandler interface.",
        type = String.class
    )
    public static final String VIRTUAL_SPOUT_HANDLER_CLASS = "spout.virtual_spout_handler_class";

    /**
     * Logger for logging logs.
     */
    private static final Logger logger = LoggerFactory.getLogger(SpoutConfig.class);

    /**
     * Utility method to add any unspecified configuration value for items with their defaults.
     * @param config config to update.
     * @return cloned copy of the config that is updated.
     */
    public static Map<String, Object> setDefaults(Map config) {
        // Clone the map
        Map<String, Object> clonedConfig = Maps.newHashMap();
        clonedConfig.putAll(config);

        // Add in defaults where needed.
        if (!clonedConfig.containsKey(OUTPUT_STREAM_ID)) {
            clonedConfig.put(OUTPUT_STREAM_ID, "default");
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                OUTPUT_STREAM_ID,
                clonedConfig.get(OUTPUT_STREAM_ID)
            );
        }
        if (!clonedConfig.containsKey(PERMANENTLY_FAILED_OUTPUT_STREAM_ID)) {
            clonedConfig.put(PERMANENTLY_FAILED_OUTPUT_STREAM_ID, "failed");
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                PERMANENTLY_FAILED_OUTPUT_STREAM_ID,
                clonedConfig.get(PERMANENTLY_FAILED_OUTPUT_STREAM_ID)
            );
        }
        if (!clonedConfig.containsKey(CONSUMER_CLASS)) {
            // For now default KafkaConsumer
            clonedConfig.put(CONSUMER_CLASS, Consumer.class.getName());
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                CONSUMER_CLASS,
                clonedConfig.get(CONSUMER_CLASS)
            );
        }
        if (!clonedConfig.containsKey(METRICS_RECORDER_CLASS)) {
            clonedConfig.put(METRICS_RECORDER_CLASS, LogRecorder.class.getName());
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                METRICS_RECORDER_CLASS,
                clonedConfig.get(METRICS_RECORDER_CLASS)
            );
        }
        if (!clonedConfig.containsKey(RETRY_MANAGER_CLASS)) {
            clonedConfig.put(RETRY_MANAGER_CLASS, DefaultRetryManager.class.getName());
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                RETRY_MANAGER_CLASS, clonedConfig.get(RETRY_MANAGER_CLASS));
        }
        if (!clonedConfig.containsKey(RETRY_MANAGER_RETRY_LIMIT)) {
            clonedConfig.put(RETRY_MANAGER_RETRY_LIMIT, 25);
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                RETRY_MANAGER_RETRY_LIMIT,
                clonedConfig.get(RETRY_MANAGER_RETRY_LIMIT)
            );
        }
        if (!clonedConfig.containsKey(RETRY_MANAGER_INITIAL_DELAY_MS)) {
            clonedConfig.put(RETRY_MANAGER_INITIAL_DELAY_MS, 1000L);
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                RETRY_MANAGER_INITIAL_DELAY_MS,
                clonedConfig.get(RETRY_MANAGER_INITIAL_DELAY_MS)
            );
        }
        if (!clonedConfig.containsKey(METRICS_RECORDER_CLASS)) {
            clonedConfig.put(METRICS_RECORDER_CLASS, LogRecorder.class.getName());
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                METRICS_RECORDER_CLASS,
                clonedConfig.get(METRICS_RECORDER_CLASS)
            );
        }
        if (!clonedConfig.containsKey(MONITOR_THREAD_INTERVAL_MS)) {
            clonedConfig.put(MONITOR_THREAD_INTERVAL_MS, 2000L);
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                MONITOR_THREAD_INTERVAL_MS,
                clonedConfig.get(MONITOR_THREAD_INTERVAL_MS)
            );
        }
        if (!clonedConfig.containsKey(MAX_SPOUT_SHUTDOWN_TIME_MS)) {
            clonedConfig.put(MAX_SPOUT_SHUTDOWN_TIME_MS, 10000L);
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                MAX_SPOUT_SHUTDOWN_TIME_MS,
                clonedConfig.get(MAX_SPOUT_SHUTDOWN_TIME_MS)
            );
        }
        if (!clonedConfig.containsKey(CONSUMER_STATE_FLUSH_INTERVAL_MS)) {
            clonedConfig.put(CONSUMER_STATE_FLUSH_INTERVAL_MS, 30000L);
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                CONSUMER_STATE_FLUSH_INTERVAL_MS,
                clonedConfig.get(CONSUMER_STATE_FLUSH_INTERVAL_MS)
            );
        }
        if (!clonedConfig.containsKey(MAX_CONCURRENT_VIRTUAL_SPOUTS)) {
            clonedConfig.put(MAX_CONCURRENT_VIRTUAL_SPOUTS, 10);
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                MAX_CONCURRENT_VIRTUAL_SPOUTS,
                clonedConfig.get(MAX_CONCURRENT_VIRTUAL_SPOUTS)
            );
        }
        if (!clonedConfig.containsKey(TUPLE_BUFFER_CLASS)) {
            clonedConfig.put(TUPLE_BUFFER_CLASS, RoundRobinBuffer.class.getName());
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                TUPLE_BUFFER_CLASS,
                clonedConfig.get(TUPLE_BUFFER_CLASS)
            );
        }
        if (!clonedConfig.containsKey(TUPLE_BUFFER_MAX_SIZE)) {
            clonedConfig.put(TUPLE_BUFFER_MAX_SIZE, 2000);
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                TUPLE_BUFFER_MAX_SIZE,
                clonedConfig.get(TUPLE_BUFFER_MAX_SIZE)
            );
        }

        if (!clonedConfig.containsKey(PERSISTENCE_ZK_SESSION_TIMEOUT)) {
            clonedConfig.put(PERSISTENCE_ZK_SESSION_TIMEOUT, 6000);
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                PERSISTENCE_ZK_SESSION_TIMEOUT,
                clonedConfig.get(PERSISTENCE_ZK_SESSION_TIMEOUT)
            );
        }

        if (!clonedConfig.containsKey(PERSISTENCE_ZK_CONNECTION_TIMEOUT)) {
            clonedConfig.put(PERSISTENCE_ZK_CONNECTION_TIMEOUT, 6000);
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                PERSISTENCE_ZK_CONNECTION_TIMEOUT,
                clonedConfig.get(PERSISTENCE_ZK_CONNECTION_TIMEOUT)
            );
        }

        if (!clonedConfig.containsKey(PERSISTENCE_ZK_RETRY_ATTEMPTS)) {
            clonedConfig.put(PERSISTENCE_ZK_RETRY_ATTEMPTS, 10);
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                PERSISTENCE_ZK_RETRY_ATTEMPTS,
                clonedConfig.get(PERSISTENCE_ZK_RETRY_ATTEMPTS)
            );
        }

        if (!clonedConfig.containsKey(PERSISTENCE_ZK_RETRY_INTERVAL)) {
            clonedConfig.put(PERSISTENCE_ZK_RETRY_INTERVAL, 10);
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                PERSISTENCE_ZK_RETRY_INTERVAL,
                clonedConfig.get(PERSISTENCE_ZK_RETRY_INTERVAL)
            );
        }

        if (!clonedConfig.containsKey(SPOUT_HANDLER_CLASS)) {
            // If we don't have a spout handler configured use the noop one, which does nothing but helps
            // us avoid null pointers in the main spout code.
            clonedConfig.put(SPOUT_HANDLER_CLASS, NoopSpoutHandler.class.getName());
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                SPOUT_HANDLER_CLASS,
                clonedConfig.get(SPOUT_HANDLER_CLASS)
            );
        }

        if (!clonedConfig.containsKey(VIRTUAL_SPOUT_HANDLER_CLASS)) {
            // If we don't have a spout handler configured use the noop one, which does nothing but helps
            // us avoid null pointers in the main spout code.
            clonedConfig.put(VIRTUAL_SPOUT_HANDLER_CLASS, NoopVirtualSpoutHandler.class.getName());
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                VIRTUAL_SPOUT_HANDLER_CLASS,
                clonedConfig.get(VIRTUAL_SPOUT_HANDLER_CLASS)
            );
        }

        return clonedConfig;
    }
}
