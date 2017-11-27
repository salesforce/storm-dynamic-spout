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

import com.salesforce.storm.spout.documentation.ConfigDocumentation;
import com.salesforce.storm.spout.dynamic.VirtualSpoutFactory;
import com.salesforce.storm.spout.dynamic.handler.NoopSpoutHandler;
import com.salesforce.storm.spout.dynamic.handler.NoopVirtualSpoutHandler;
import com.salesforce.storm.spout.dynamic.kafka.Consumer;
import com.salesforce.storm.spout.dynamic.retry.DefaultRetryManager;
import com.salesforce.storm.spout.dynamic.metrics.LogRecorder;
import com.salesforce.storm.spout.dynamic.buffer.RoundRobinBuffer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dynamic Spout Configuration Directives.
 */
public class DynamicSpoutConfig extends SpoutConfig {
    /**
     * Holds our Configuration Definition.
     */
    public static final ConfigDefinition CONFIG;

    /**
     * (String) Defines the name of the output stream tuples will be emitted out of.
     */
    public static final String OUTPUT_STREAM_ID = "spout.output_stream_id";

    /**
     * (List[String]) Defines the output fields that the spout will emit as a list of field names.
     * Example: ["field1", "field2", ...]
     *
     * Also supported as a single string of comma separated values: "field1, field2, ..."
     * Or as an explicitly defined Fields object.
     */
    public static final String OUTPUT_FIELDS = "spout.output_fields";

///////////////////////////////////
// Consumer Config
///////////////////////////////////

    /**
     * (String) Defines which Consumer implementation to use.
     * Should be a full classpath to a class that implements the Consumer interface.
     */
    public static final String CONSUMER_CLASS = "spout.consumer.class";

///////////////////////////////////
// Persistence Layer Config
///////////////////////////////////

    /**
     * (String) Defines which PersistenceAdapter implementation to use.
     * Should be a full classpath to a class that implements the PersistenceAdapter interface.
     * Default Value: "com.salesforce.storm.spout.dynamic.persistence.ZookeeperPersistenceAdapter"
     */
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
    public static final String PERSISTENCE_ZK_SERVERS = "spout.persistence.zookeeper.servers";

    /**
     * (String) Defines the root path to persist state under.
     * Example: "/consumer-state"
     *
     * Optional - Only required if you use the Zookeeper persistence implementation.
     */
    public static final String PERSISTENCE_ZK_ROOT = "spout.persistence.zookeeper.root";

    /**
     * (Integer) Zookeeper session timeout.
     */
    public static final String PERSISTENCE_ZK_SESSION_TIMEOUT = "spout.persistence.zookeeper.session_timeout";

    /**
     * (Integer) Zookeeper connection timeout.
     */
    public static final String PERSISTENCE_ZK_CONNECTION_TIMEOUT = "spout.persistence.zookeeper.connection_timeout";

    /**
     * (Integer) Zookeeper retry attempts.
     */
    public static final String PERSISTENCE_ZK_RETRY_ATTEMPTS = "spout.persistence.zookeeper.retry_attempts";

    /**
     * (Integer) Zookeeper retry interval.
     */
    public static final String PERSISTENCE_ZK_RETRY_INTERVAL = "spout.persistence.zookeeper.retry_interval";

///////////////////////////////////
// Failed Message Retry Config
///////////////////////////////////

    /**
     * (String) Defines which RetryManager implementation to use.
     * Should be a full classpath to a class that implements the RetryManager interface.
     * Default Value: "com.salesforce.storm.spout.dynamic.retry.DefaultRetryManager"
     */
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
    public static final String RETRY_MANAGER_INITIAL_DELAY_MS = "spout.retry_manager.initial_delay_ms";

    /**
     * (double) Defines how quickly the delay increases after each failed tuple.
     *
     * Example: A value of 2.0 means the delay between retries doubles.  eg. 4, 8, 16 seconds, etc.
     *
     * Default Value: 2.0
     * Optional - Only required if you use the DefaultRetryManager implementation.
     */
    public static final String RETRY_MANAGER_DELAY_MULTIPLIER = "spout.retry_manager.delay_multiplier";

    /**
     * (long) Defines an upper bound of the max delay time between retried a failed tuple.
     *
     * Default Value: 900000 (15 minutes)
     * Optional - Only required if you use the DefaultRetryManager implementation.
     */
    public static final String RETRY_MANAGER_MAX_DELAY_MS = "spout.retry_manager.retry_delay_max_ms";

///////////////////////////////////
// Metrics Collection
///////////////////////////////////

    /**
     * (String) Defines which MetricsRecorder implementation to use.
     * Should be a full classpath to a class that implements the MetricsRecorder interface.
     * Default Value: "com.salesforce.storm.spout.dynamic.metrics.LogRecorder"
     */
    public static final String METRICS_RECORDER_CLASS = "spout.metrics.class";

    /**
     * (boolean) Defines the time bucket to group metrics together under.
     * Default Value: 60
     */
    public static final String METRICS_RECORDER_TIME_BUCKET = "spout.metrics.time_bucket";

    /**
     * (boolean) Defines if MetricsRecord instance should include the taskId in the metric key.
     * Default Value: false
     */
    public static final String METRICS_RECORDER_ENABLE_TASK_ID_PREFIX = "spout.metrics.enable_task_id_prefix";

///////////////////////////////////
// Internal Coordinator Config
///////////////////////////////////

    /**
     * (String) Defines which MessageBuffer implementation to use.
     * Should be a full classpath to a class that implements the MessageBuffer interface.
     * Default Value: com.salesforce.storm.spout.dynamic.buffer.RoundRobinBuffer
     */
    public static final String TUPLE_BUFFER_CLASS = "spout.coordinator.tuple_buffer.class";

    /**
     * (int) Defines maximum size of the tuple buffer.  After the buffer reaches this size
     * the internal VirtualSpouts will be blocked from generating additional tuples until they have been emitted into the topology.
     * Default Value: 2000
     */
    public static final String TUPLE_BUFFER_MAX_SIZE = "spout.coordinator.tuple_buffer.max_size";

    /**
     * (long) How often our monitor thread will run and watch over its managed virtual spout instances, in milliseconds.
     * Default Value: 2000
     */
    public static final String MONITOR_THREAD_INTERVAL_MS = "spout.coordinator.monitor_thread_interval_ms";

    /**
     * (long) How long we'll wait for all VirtualSpout's to cleanly shut down, before we stop
     * them with force, in Milliseconds.
     * Default Value: 10000
     */
    public static final String MAX_SPOUT_SHUTDOWN_TIME_MS = "spout.coordinator.max_spout_shutdown_time_ms";

    /**
     * (long) How often we'll make sure each VirtualSpout persists its state, in Milliseconds.
     * Default Value: 30000
     */
    public static final String CONSUMER_STATE_FLUSH_INTERVAL_MS = "spout.coordinator.consumer_state_flush_interval_ms";

    // TODO: Category needs to change?
    /**
     * (String) Defines a consumerId prefix to use for all consumers created by the spout.
     * This must be unique to your spout instance, and must not change between deploys.
     */
    public static final String VIRTUAL_SPOUT_ID_PREFIX = "spout.coordinator.virtual_spout_id_prefix";

    /**
     * (int) The size of the thread pool for running virtual spouts.
     * Default Value: 10
     */
    public static final String MAX_CONCURRENT_VIRTUAL_SPOUTS = "spout.coordinator.max_concurrent_virtual_spouts";

    /**
     * (String) Defines which SpoutHandler implementation to use.
     * Should be a fully qualified class path that implements the SpoutHandler interface.
     * Default value: com.salesforce.storm.spout.dynamic.handler.NoopSpoutHandler
     */
    @ConfigDocumentation(
        category = ConfigDocumentation.Category.DYNAMIC_SPOUT,
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
    public static final String VIRTUAL_SPOUT_HANDLER_CLASS = "spout.virtual_spout_handler_class";

    /**
     * TODO needs a javadoc lemon.
     */
    public static final String VIRTUAL_SPOUT_FACTORY_CLASS = "spout.virtual_spout_factory_class";

    /*
     * Build Configuration definition.
     */
    static {
        CONFIG = new ConfigDefinition()
            .define(
                OUTPUT_STREAM_ID,
                String.class,
                "default",
                ConfigDefinition.Importance.HIGH,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines the name of the output stream tuples will be emitted out of."
            ).define(
                OUTPUT_FIELDS,
                List.class,
                null,
                ConfigDefinition.Importance.HIGH,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines the output fields that the spout will emit as a list of field names."
            ).define(
                CONSUMER_CLASS,
                String.class,
                // TODO We need to no longer default to using KafkaConsumer class here.
                Consumer.class.getName(),
                ConfigDefinition.Importance.HIGH,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines which Consumer implementation to use. Should be a full classpath to a class that "
                + "implements the Consumer interface."
            ).define(
                PERSISTENCE_ADAPTER_CLASS,
                String.class,
                // TODO should this have a default value? Doc says so, but we don't set it. So either update doc, or set default.
                null,
                ConfigDefinition.Importance.MEDIUM,
                ConfigDocumentation.Category.PERSISTENCE.name(),
                "Defines which PersistenceAdapter implementation to use. "
                + "Should be a full classpath to a class that implements the PersistenceAdapter interface."
            ).define(
                PERSISTENCE_ZK_SERVERS,
                List.class,
                null,
                ConfigDefinition.Importance.MEDIUM,
                ConfigDocumentation.Category.PERSISTENCE_ZOOKEEPER.name(),
                "Holds a list of Zookeeper server Hostnames + Ports in the following format: [\"zkhost1:2181\", \"zkhost2:2181\", ...]"
            ).define(
                PERSISTENCE_ZK_ROOT,
                String.class,
                null,
                ConfigDefinition.Importance.MEDIUM,
                ConfigDocumentation.Category.PERSISTENCE_ZOOKEEPER.name(),
                "Defines the root path to persist state under. Example: \"/consumer-state\""
            ).define(
                PERSISTENCE_ZK_SESSION_TIMEOUT,
                Integer.class,
                6000,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.PERSISTENCE_ZOOKEEPER.name(),
                "Zookeeper session timeout."
            ).define(
                PERSISTENCE_ZK_CONNECTION_TIMEOUT,
                Integer.class,
                6000,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.PERSISTENCE_ZOOKEEPER.name(),
                "Zookeeper connection timeout."
            ).define(
                PERSISTENCE_ZK_RETRY_ATTEMPTS,
                Integer.class,
                10,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.PERSISTENCE_ZOOKEEPER.name(),
                "Zookeeper retry attempts."
            ).define(
                PERSISTENCE_ZK_RETRY_INTERVAL,
                Integer.class,
                10,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.PERSISTENCE_ZOOKEEPER.name(),
                "Zookeeper retry interval."
            ).define(
                RETRY_MANAGER_CLASS,
                String.class,
                DefaultRetryManager.class.getName(),
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines which RetryManager implementation to use. "
                + "Should be a full classpath to a class that implements the RetryManager interface."
            ).define(
                RETRY_MANAGER_RETRY_LIMIT,
                Integer.class,
                25,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines how many times a failed message will be replayed before just being acked. "
                + "A negative value means tuples will be retried forever. A value of 0 means tuples will never be retried. "
                + "A positive value means tuples will be retried up to this limit, then dropped."
            ).define(
                RETRY_MANAGER_INITIAL_DELAY_MS,
                Long.class,
                1000L,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines how long to wait before retry attempts are made on failed tuples, in milliseconds. "
                + "Each retry attempt will wait for (number_of_times_message_has_failed * min_retry_time_ms). "
                + "Example: If a tuple fails 5 times, and the min retry time is set to 1000, it will wait at least "
                + "(5 * 1000) milliseconds before the next retry attempt."
            ).define(
                RETRY_MANAGER_DELAY_MULTIPLIER,
                Double.class,
                2.0D,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines how quickly the delay increases after each failed tuple. "
                + "Example: A value of 2.0 means the delay between retries doubles.  eg. 4, 8, 16 seconds, etc."
            ).define(
                RETRY_MANAGER_MAX_DELAY_MS,
                Long.class,
                900000L,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines an upper bound of the max delay time between retried a failed tuple."
            ).define(
                METRICS_RECORDER_CLASS,
                String.class,
                LogRecorder.class.getName(),
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines which MetricsRecorder implementation to use. "
                + "Should be a full classpath to a class that implements the MetricsRecorder interface."
            ).define(
                METRICS_RECORDER_TIME_BUCKET,
                Integer.class,
                60,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines the time bucket to group metrics together under."
            ).define(
                METRICS_RECORDER_ENABLE_TASK_ID_PREFIX,
                Boolean.class,
                false,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines if MetricsRecorder instance should include the taskId in the metric key."
            ).define(
                TUPLE_BUFFER_CLASS,
                String.class,
                RoundRobinBuffer.class.getName(),
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines which MessageBuffer implementation to use. "
                + "Should be a full classpath to a class that implements the MessageBuffer interface."
            ).define(
                TUPLE_BUFFER_MAX_SIZE,
                Integer.class,
                2000,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines maximum size of the tuple buffer.  After the buffer reaches this size the internal "
                + "VirtualSpouts will be blocked from generating additional tuples until they have been emitted into the topology."
            ).define(
                // TODO is this still used?
                MONITOR_THREAD_INTERVAL_MS,
                Long.class,
                2000L,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "How often our monitor thread will run and watch over its managed virtual spout instances, in milliseconds."
            ).define(
                MAX_SPOUT_SHUTDOWN_TIME_MS,
                Long.class,
                10000L,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "How long we'll wait for all VirtualSpout's to cleanly shut down, before we stop them with force, in Milliseconds."
            ).define(
                CONSUMER_STATE_FLUSH_INTERVAL_MS,
                Long.class,
                30000L,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "How often we'll make sure each VirtualSpout persists its state, in Milliseconds."
            ).define(
                VIRTUAL_SPOUT_ID_PREFIX,
                String.class,
                null,
                ConfigDefinition.Importance.HIGH,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines a VirtualSpoutId prefix to use for all VirtualSpouts created by the spout. "
                + "This must be unique to your spout instance, and must not change between deploys."
            ).define(
                MAX_CONCURRENT_VIRTUAL_SPOUTS,
                Integer.class,
                10,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "The size of the thread pool for running virtual spouts."
            ).define(
                SPOUT_HANDLER_CLASS,
                String.class,
                NoopSpoutHandler.class.getName(),
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines which SpoutHandler implementation to use. "
                + "Should be a fully qualified class path that implements the SpoutHandler interface."
            ).define(
                VIRTUAL_SPOUT_HANDLER_CLASS,
                String.class,
                NoopVirtualSpoutHandler.class.getName(),
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines which VirtualSpoutHandler implementation to use. "
                + "Should be a fully qualified class path that implements the VirtualSpoutHandler interface."
            ).define(
                VIRTUAL_SPOUT_FACTORY_CLASS,
                String.class,
                VirtualSpoutFactory.class.getName(),
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.DYNAMIC_SPOUT.name(),
                "Defines which DelegateSpoutFactory implementation to use. "
                + "Should be a fully qualified class path that implements the DelegateSpoutFactory interface."
            ).lock();
    }

    /**
     * Constructor.
     * New Default config.
     */
    public DynamicSpoutConfig() {
        super(CONFIG, new HashMap<>());
    }

    /**
     * Constructor.  Create new instance using values.
     * @param values Configuration values.
     */
    public DynamicSpoutConfig(final Map<String, Object> values) {
        super(CONFIG, values);
    }
}
