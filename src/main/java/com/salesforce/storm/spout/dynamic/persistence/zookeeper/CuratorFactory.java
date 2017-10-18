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

package com.salesforce.storm.spout.dynamic.persistence.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Factory for creating a new curator instance.
 */
public class CuratorFactory {

    /**
     * (List of Strings) Configuration for hosts, in the following format:
     * ["zkhost1:2181", "zkhost2:2181", ...]
     */
    private static final String CONFIG_SERVERS = "servers";

    /**
     * (Integer) Configuration for session timeout.
     */
    private static final String CONFIG_SESSION_TIMEOUT = "session_timeout";

    /**
     * (Integer) Configuration for  connection timeout.
     */
    private static final String CONFIG_CONNECTION_TIMEOUT = "connection_timeout";

    /**
     * (Integer) Configuration for retry attempts.
     */
    private static final String CONFIG_RETRY_ATTEMPTS = "retry_attempts";

    /**
     * (Integer) Configuration for  retry interval.
     */
    private static final String CONFIG_RETRY_INTERVAL = "retry_interval";

    /**
     * Create new curator instance based upon the provided config.
     * @param config configuration object.
     * @return curator instance.
     */
    public static CuratorFramework createNewCuratorInstance(
        final Map<String, Object> config
    ) {
        // List of zookeeper hosts in the format of ["host1:2182", "host2:2181",..].
        final List<String> zkServers = (List<String>) config.get(CONFIG_SERVERS);

        // Build out our bits and pieces.
        final StringBuilder stringBuilder = new StringBuilder();
        for (final String server : zkServers) {
            stringBuilder.append(server).append(",");
        }
        String serverPorts = stringBuilder.toString();
        serverPorts = serverPorts.substring(0, serverPorts.length() - 1);

        final String zkConnectionString = serverPorts;

        final int connectionTimeoutMs = ((Number) config.get(CONFIG_CONNECTION_TIMEOUT)).intValue();
        final int sessionTimeoutMs = ((Number) config.get(CONFIG_SESSION_TIMEOUT)).intValue();
        final int retryAttempts = ((Number) config.get(CONFIG_RETRY_ATTEMPTS)).intValue();
        final int retryIntervalMs = ((Number) config.get(CONFIG_RETRY_INTERVAL)).intValue();

        try {
            // Use builder to create new curator
            CuratorFramework curator = CuratorFrameworkFactory
                .builder()
                .connectString(zkConnectionString)
                .connectionTimeoutMs(connectionTimeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs)
                .retryPolicy(new RetryNTimes(retryAttempts, retryIntervalMs))
                .build();

            // Call start
            curator.start();

            // Block until connected
            curator.blockUntilConnected(
                ((Number) config.get(CONFIG_CONNECTION_TIMEOUT)).intValue(),
                TimeUnit.MILLISECONDS
            );

            return curator;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
