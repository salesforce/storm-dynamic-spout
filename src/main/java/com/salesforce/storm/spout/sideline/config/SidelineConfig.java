/*
 * Copyright (c) 2018, Salesforce.com, Inc.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Sideline Configuration Directives.
 */
public class SidelineConfig {

    /**
     * (List|String) Defines one or more sideline trigger(s) (if any) to use.
     * Should be a fully qualified class path that implements thee SidelineTrigger interface.
     */
    @ConfigDocumentation(
        category = ConfigDocumentation.Category.SIDELINE,
        description = "Defines one or more sideline trigger(s) (if any) to use. "
        + "Should be a fully qualified class path that implements thee SidelineTrigger interface.",
        type = String.class
    )
    public static final String TRIGGER_CLASS = "sideline.trigger_class";

    /**
     * (Integer) Interval (in seconds) to check running sidelines and refresh them if necessary.
     */
    @ConfigDocumentation(
        category = ConfigDocumentation.Category.SIDELINE,
        description = "Interval (in seconds) to check running sidelines and refresh them if necessary.",
        type = Integer.class
    )
    public static final String REFRESH_INTERVAL_SECONDS = "sideline.refresh_interval_seconds";

    /**
     * (String) Defines which PersistenceAdapter implementation to use.
     * Should be a full classpath to a class that implements the PersistenceAdapter interface.
     * Default Value: "com.salesforce.storm.spout.dynamic.persistence.ZookeeperPersistenceAdapter"
     */
    @ConfigDocumentation(
        category = ConfigDocumentation.Category.SIDELINE,
        description = "Defines which PersistenceAdapter implementation to use. "
        + "Should be a full classpath to a class that implements the PersistenceAdapter interface.",
        type = String.class,
        required = true
    )
    public static final String PERSISTENCE_ADAPTER_CLASS = "sideline.persistence_adapter.class";

    /**
     * (List[String) Holds a list of Zookeeper server Hostnames + Ports in the following format:
     * ["zkhost1:2181", "zkhost2:2181", ...]
     *
     * Optional - Only required if you use the Zookeeper persistence implementation.
     */
    @ConfigDocumentation(
        category = ConfigDocumentation.Category.SIDELINE,
        description = "Holds a list of Zookeeper server Hostnames + Ports in the following format: "
        + "[\"zkhost1:2181\", \"zkhost2:2181\", ...]",
        type = List.class
    )
    public static final String PERSISTENCE_ZK_SERVERS = "sideline.persistence.zookeeper.servers";

    /**
     * (String) Defines the root path to persist state under.
     * Example: "/consumer-state"
     *
     * Optional - Only required if you use the Zookeeper persistence implementation.
     */
    @ConfigDocumentation(
        category = ConfigDocumentation.Category.SIDELINE,
        description = "Defines the root path to persist state under. Example: \"/consumer-state\"",
        type = String.class
    )
    public static final String PERSISTENCE_ZK_ROOT = "sideline.persistence.zookeeper.root";

    /**
     * (Integer) Zookeeper session timeout.
     */
    @ConfigDocumentation(
        category = ConfigDocumentation.Category.SIDELINE,
        description = "Zookeeper session timeout.",
        type = Integer.class
    )
    public static final String PERSISTENCE_ZK_SESSION_TIMEOUT = "sideline.persistence.zookeeper.session_timeout";

    /**
     * (Integer) Zookeeper connection timeout.
     */
    @ConfigDocumentation(
        category = ConfigDocumentation.Category.SIDELINE,
        description = "Zookeeper connection timeout.",
        type = Integer.class
    )
    public static final String PERSISTENCE_ZK_CONNECTION_TIMEOUT = "sideline.persistence.zookeeper.connection_timeout";

    /**
     * (Integer) Zookeeper retry attempts.
     */
    @ConfigDocumentation(
        category = ConfigDocumentation.Category.SIDELINE,
        description = "Zookeeper retry attempts.",
        type = Integer.class
    )
    public static final String PERSISTENCE_ZK_RETRY_ATTEMPTS = "sideline.persistence.zookeeper.retry_attempts";

    /**
     * (Integer) Zookeeper retry interval.
     */
    @ConfigDocumentation(
        category = ConfigDocumentation.Category.SIDELINE,
        description = "Zookeeper retry interval.",
        type = Integer.class
    )
    public static final String PERSISTENCE_ZK_RETRY_INTERVAL = "sideline.persistence.zookeeper.retry_interval";

    /**
     * Logger for logging logs.
     */
    private static final Logger logger = LoggerFactory.getLogger(SidelineConfig.class);

    /**
     * Utility method to add any unspecified configuration value for items with their defaults.
     * @param config config to update.
     * @return cloned copy of the config that is updated.
     */
    public static Map<String, Object> setDefaults(Map config) {
        // Clone the map
        Map<String, Object> clonedConfig = Maps.newHashMap();
        clonedConfig.putAll(config);

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

        if (!clonedConfig.containsKey(REFRESH_INTERVAL_SECONDS)) {
            // Default to 10 minutes
            clonedConfig.put(REFRESH_INTERVAL_SECONDS, 600);
            logger.info(
                "Unspecified configuration value for {} using default value {}",
                REFRESH_INTERVAL_SECONDS,
                clonedConfig.get(REFRESH_INTERVAL_SECONDS)
            );
        }

        return clonedConfig;
    }
}
