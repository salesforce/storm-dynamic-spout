/*
 * Copyright (c) 2017, 2018, Salesforce.com, Inc.
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

package com.salesforce.storm.spout.sideline.recipes.trigger.zookeeper;

import com.salesforce.storm.spout.documentation.ConfigDocumentation;
import com.salesforce.storm.spout.sideline.recipes.trigger.FilterChainStepBuilder;

import java.util.List;

/**
 * Configuration for the {@link ZookeeperWatchTrigger}.
 */
public class Config {

    /**
     * Prefix for this set of configuration directives.
     */
    public static final String PREFIX = "sideline.zookeeper_watch_trigger.";

    /**
     * (String) Class name for the class of the {@link FilterChainStepBuilder} instance.
     */
    @ConfigDocumentation(
        description = "Class name for the class of the FilterChainStepBuilder instance.",
        type = String.class
    )
    public static final String FILTER_CHAIN_STEP_BUILDER_CLASS = PREFIX + "filter_chain_step_builder_class";

    /**
     * (List[String]) Holds a list of Zookeeper server Hostnames + Ports in the following format:
     * ["zkhost1:2181", "zkhost2:2181", ...]
     */
    @ConfigDocumentation(
        description = "Holds a list of Zookeeper server Hostnames + Ports in the following format: "
        + "[\"zkhost1:2181\", \"zkhost2:2181\", ...]",
        type = List.class
    )
    public static final String ZK_SERVERS = PREFIX + "servers";

    /**
     * (String) Defines the root path to watch for events under.
     * It is recommended that you scope your root to your topology and kafka topic.
     * Example: /sideline-trigger/my-topology/my-topic
     */
    @ConfigDocumentation(
        description = "Defines the root path to watch for events under. "
            + "It is recommended that you scope your root to your topology and kafka topic."
            + "Example: \"/sideline-trigger/my-topology/my-topic\"",
        type = String.class
    )
    public static final String ZK_ROOT = PREFIX + "root";

    /**
     * (Integer) Zookeeper session timeout.
     */
    @ConfigDocumentation(
        description = "Zookeeper session timeout.",
        type = Integer.class
    )
    public static final String ZK_SESSION_TIMEOUT = PREFIX + "session_timeout";

    /**
     * (Integer) Zookeeper connection timeout.
     */
    @ConfigDocumentation(
        description = "Zookeeper connection timeout.",
        type = Integer.class
    )
    public static final String ZK_CONNECTION_TIMEOUT = PREFIX + "connection_timeout";

    /**
     * (Integer) Zookeeper retry attempts.
     */
    @ConfigDocumentation(
        description = "Zookeeper retry attempts.",
        type = Integer.class
    )
    public static final String ZK_RETRY_ATTEMPTS = PREFIX + "retry_attempts";

    /**
     * (Integer) Zookeeper retry interval.
     */
    @ConfigDocumentation(
        description = "Zookeeper retry interval.",
        type = Integer.class
    )
    public static final String ZK_RETRY_INTERVAL = PREFIX + "retry_interval";
}
