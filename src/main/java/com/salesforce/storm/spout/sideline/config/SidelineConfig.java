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

package com.salesforce.storm.spout.sideline.config;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.documentation.ConfigDocumentation;
import com.salesforce.storm.spout.dynamic.Tools;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import com.salesforce.storm.spout.dynamic.config.DynamicSpoutConfig;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.sideline.handler.SidelineSpoutHandler;
import com.salesforce.storm.spout.sideline.handler.SidelineVirtualSpoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sideline Configuration Directives.
 */
public class SidelineConfig extends SpoutConfig {

    /**
     * Holds our Configuration Definition.
     */
    private static final ConfigDefinition CONFIG;

    /**
     * (List|String) Defines one or more sideline trigger(s) (if any) to use.
     * Should be a fully qualified class path that implements thee SidelineTrigger interface.
     */
    public static final String TRIGGER_CLASS = "sideline.trigger_class";

    /**
     * (Integer) Interval (in seconds) to check running sidelines and refresh them if necessary.
     */
    public static final String REFRESH_INTERVAL_SECONDS = "sideline.refresh_interval_seconds";

    /**
     * (String) Defines which PersistenceAdapter implementation to use.
     * Should be a full classpath to a class that implements the PersistenceAdapter interface.
     * Default Value: "com.salesforce.storm.spout.dynamic.persistence.ZookeeperPersistenceAdapter"
     */
    public static final String PERSISTENCE_ADAPTER_CLASS = "sideline.persistence_adapter.class";

    /**
     * (List[String) Holds a list of Zookeeper server Hostnames + Ports in the following format:
     * ["zkhost1:2181", "zkhost2:2181", ...]
     *
     * Optional - Only required if you use the Zookeeper persistence implementation.
     */
    public static final String PERSISTENCE_ZK_SERVERS = "sideline.persistence.zookeeper.servers";

    /**
     * (String) Defines the root path to persist state under.
     * Example: "/consumer-state"
     *
     * Optional - Only required if you use the Zookeeper persistence implementation.
     */
    public static final String PERSISTENCE_ZK_ROOT = "sideline.persistence.zookeeper.root";

    /**
     * (Integer) Zookeeper session timeout.
     */
    public static final String PERSISTENCE_ZK_SESSION_TIMEOUT = "sideline.persistence.zookeeper.session_timeout";

    /**
     * (Integer) Zookeeper connection timeout.
     */
    public static final String PERSISTENCE_ZK_CONNECTION_TIMEOUT = "sideline.persistence.zookeeper.connection_timeout";

    /**
     * (Integer) Zookeeper retry attempts.
     */
    public static final String PERSISTENCE_ZK_RETRY_ATTEMPTS = "sideline.persistence.zookeeper.retry_attempts";

    /**
     * (Integer) Zookeeper retry interval.
     */
    public static final String PERSISTENCE_ZK_RETRY_INTERVAL = "sideline.persistence.zookeeper.retry_interval";

    /*
     * Build Configuration definition.
     */
    static {
        // Extend DynamicSpoutConfig.
        CONFIG = new ConfigDefinition(DynamicSpoutConfig.CONFIG)
            .define(
                TRIGGER_CLASS,
                String.class,
                null,
                ConfigDefinition.Importance.HIGH,
                ConfigDocumentation.Category.SIDELINE.name(),
                "Defines one or more sideline trigger(s) (if any) to use. "
                + "Should be a fully qualified class path that implements thee SidelineTrigger interface."
            ).define(
                REFRESH_INTERVAL_SECONDS,
                Integer.class,
                600,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.SIDELINE.name(),
                "Interval (in seconds) to check running sidelines and refresh them if necessary."
            ).define(
                PERSISTENCE_ADAPTER_CLASS,
                String.class,
                null,
                ConfigDefinition.Importance.HIGH,
                ConfigDocumentation.Category.SIDELINE.name(),
                "Defines which PersistenceAdapter implementation to use. "
                + "Should be a full classpath to a class that implements the PersistenceAdapter interface."
            ).define(
                PERSISTENCE_ZK_SERVERS,
                List.class,
                null,
                ConfigDefinition.Importance.HIGH,
                ConfigDocumentation.Category.SIDELINE.name(),
                "Holds a list of Zookeeper server Hostnames + Ports in the following format: "
                + "[\"zkhost1:2181\", \"zkhost2:2181\", ...]"
            ).define(
                PERSISTENCE_ZK_ROOT,
                String.class,
                null,
                ConfigDefinition.Importance.HIGH,
                ConfigDocumentation.Category.SIDELINE.name(),
                "Defines the root path to persist state under. Example: \"/consumer-state\""
            ).define(
                PERSISTENCE_ZK_SESSION_TIMEOUT,
                Integer.class,
                6000,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.SIDELINE.name(),
                "Zookeeper session timeout."
            ).define(
                PERSISTENCE_ZK_CONNECTION_TIMEOUT,
                Integer.class,
                6000,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.SIDELINE.name(),
                "Zookeeper connection timeout."
            ).define(
                PERSISTENCE_ZK_RETRY_ATTEMPTS,
                Integer.class,
                10,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.SIDELINE.name(),
                "Zookeeper retry attempts."
            ).define(
                PERSISTENCE_ZK_RETRY_INTERVAL,
                Integer.class,
                10,
                ConfigDefinition.Importance.LOW,
                ConfigDocumentation.Category.SIDELINE.name(),
                "Zookeeper retry interval."
            )
            // Re-define some default values.
            .setDefaultValue(DynamicSpoutConfig.SPOUT_HANDLER_CLASS, SidelineSpoutHandler.class.getName())
            .setDefaultValue(DynamicSpoutConfig.VIRTUAL_SPOUT_HANDLER_CLASS, SidelineVirtualSpoutHandler.class.getName())
            .lock();
    }

    /**
     * Constructor.
     * New Default config.
     */
    public SidelineConfig() {
        this(new HashMap<>());
    }

    /**
     * Constructor.  Create new instance using values.
     * @param values Configuration values.
     */
    public SidelineConfig(final Map<String, Object> values) {
        super(CONFIG, values);
    }
}
