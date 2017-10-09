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

package com.salesforce.storm.spout.dynamic.persistence;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.sideline.filter.FilterChainStep;
import com.salesforce.storm.spout.sideline.filter.Serializer;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Persistence layer implemented using Zookeeper.
 * Why Zookeeper?  Because its easy, and you most likely have it around.
 */
public class ZookeeperPersistenceAdapter implements PersistenceAdapter, Serializable {
    // Logger
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperPersistenceAdapter.class);

    // Config
    private String zkConnectionString;
    private String zkRoot;

    // Zookeeper connection
    private CuratorFramework curator;

    /**
     * Loads in configuration and sets up zookeeper/curator connection.
     * @param spoutConfig - The storm topology config map.
     */
    public void open(Map spoutConfig) {
         // List of zookeeper hosts in the format of ["host1:2182", "host2:2181",..].
        final List<String> zkServers = (List<String>) spoutConfig.get(SpoutConfig.PERSISTENCE_ZK_SERVERS);

        // Root node / prefix to write entries under.
        String zkRoot = (String) spoutConfig.get(SpoutConfig.PERSISTENCE_ZK_ROOT);
        if (Strings.isNullOrEmpty(zkRoot)) {
            throw new IllegalStateException("Missing required configuration: " + SpoutConfig.PERSISTENCE_ZK_ROOT);
        }

        // We append the consumerId onto the zkRootNode
        final String consumerId = (String) spoutConfig.get(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX);

        // Save this concatenated prefix
        this.zkRoot = zkRoot + "/" + consumerId;

        // Build out our bits and pieces.
        StringBuilder stringBuilder = new StringBuilder();
        for (String server : zkServers) {
            stringBuilder.append(server).append(",");
        }
        String serverPorts = stringBuilder.toString();
        serverPorts = serverPorts.substring(0, serverPorts.length() - 1);
        this.zkConnectionString = serverPorts;


        try {
            // Create new curator framework
            curator = newCurator(spoutConfig);

            // Call start
            curator.start();

            // Block until connected
            curator.blockUntilConnected(
                ((Number) spoutConfig.get(SpoutConfig.PERSISTENCE_ZK_CONNECTION_TIMEOUT)).intValue(),
                TimeUnit.MILLISECONDS
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Close up shop, shut down zookeeper/curator connection.
     */
    @Override
    public void close() {
        if (curator == null) {
            return;
        }
        curator.close();
        curator = null;
    }

    /**
     * Pass in the consumer state that you'd like persisted.
     * @param consumerId The consumer's id.
     * @param partitionId The partition id
     * @param offset Offset for the partition to be persisted
     */
    @Override
    public void persistConsumerState(final String consumerId, final int partitionId, final long offset) {
        // Validate we're in a state that can be used.
        verifyHasBeenOpened();

        // Persist!
        writeBytes(getZkConsumerStatePath(consumerId, partitionId), String.valueOf(offset).getBytes(Charsets.UTF_8));
    }

    /**
     * Retrieves the consumer state from the persistence layer.
     * @param consumerId The consumer's id.
     * @param partitionId The partition id
     * @return ConsumerState Consumer state that was persisted
     */
    @Override
    public Long retrieveConsumerState(final String consumerId, final int partitionId) {
        // Validate we're in a state that can be used.
        verifyHasBeenOpened();

        // Read!
        final String path = getZkConsumerStatePath(consumerId, partitionId);

        final byte[] bytes = readBytes(path);

        if (bytes == null) {
            return null;
        }
        return Long.valueOf(new String(bytes, Charsets.UTF_8));
    }

    /**
     * Removes consumer state.
     * @param consumerId The consumer's id
     * @param partitionId The partition id
     */
    @Override
    public void clearConsumerState(final String consumerId, final int partitionId) {
        // Validate we're in a state that can be used.
        verifyHasBeenOpened();

        // Delete!
        final String path = getZkConsumerStatePath(consumerId, partitionId);
        logger.info("Delete state from Zookeeper at {}", path);
        deleteNode(path);

        // Attempt to delete the parent path.
        // This is a noop if the parent path is not empty.
        final String parentPath = path.substring(0, path.lastIndexOf('/'));
        deleteNodeIfNoChildren(parentPath);
    }

    /**
     * Persist a sideline request
     * @param type Sideline Type (Start/Stop)
     * @param id Unique identifier for the sideline request.
     * @param request Sideline Request
     * @param partitionId Partition id
     * @param startingOffset Ending offset
     * @param endingOffset Starting offset
     */
    @Override
    public void persistSidelineRequestState(
        SidelineType type,
        SidelineRequestIdentifier id,
        SidelineRequest request,
        int partitionId,
        Long startingOffset,
        Long endingOffset
    ) {
        // Validate we're in a state that can be used.
        verifyHasBeenOpened();

        Map<String, Object> data = Maps.newHashMap();
        data.put("type", type.toString());
        data.put("startingOffset", startingOffset);
        if (endingOffset != null) { // Optional
            data.put("endingOffset", endingOffset);
        }
        data.put("filterChainStep", Serializer.serialize(request.step));

        // Persist!
        writeJson(getZkRequestStatePath(id.toString(), partitionId), data);
    }

    @Override
    public SidelinePayload retrieveSidelineRequest(SidelineRequestIdentifier id, int partitionId) {
        // Validate we're in a state that can be used.
        verifyHasBeenOpened();

        // Read!
        final String path = getZkRequestStatePath(id.toString(), partitionId);
        Map<Object, Object> json = readJson(path);
        logger.debug("Read request state from Zookeeper at {}: {}", path, json);

        if (json == null) {
            return null;
        }

        final String typeString = (String) json.get("type");

        final SidelineType type = typeString.equals(SidelineType.STOP.toString()) ? SidelineType.STOP : SidelineType.START;

        final FilterChainStep step = parseJsonToFilterChainSteps(json);

        final Long startingOffset = (Long) json.get("startingOffset");
        final Long endingOffset = (Long) json.get("endingOffset");

        return new SidelinePayload(
            type,
            id,
            new SidelineRequest(id, step),
            startingOffset,
            endingOffset
        );
    }

    /**
     * Removes a sideline request from the persistence layer.
     * @param id SidelineRequestIdentifier you want to clear.
     * @param partitionId PartitionId to clear.
     */
    @Override
    public void clearSidelineRequest(SidelineRequestIdentifier id, int partitionId) {
        // Validate we're in a state that can be used.
        verifyHasBeenOpened();

        // Delete!
        final String path = getZkRequestStatePath(id.toString(), partitionId);
        logger.info("Delete request from Zookeeper at {}", path);
        deleteNode(path);

        // Attempt to delete the parent path.
        // This is a noop if the parent path is not empty.
        final String parentPath = path.substring(0, path.lastIndexOf('/'));
        deleteNodeIfNoChildren(parentPath);
    }

    /**
     * Lists out a unique list of current sideline requests.
     * @return List of sideline request identifier objects
     */
    public List<SidelineRequestIdentifier> listSidelineRequests() {
        verifyHasBeenOpened();

        final List<SidelineRequestIdentifier> ids = Lists.newArrayList();

        try {
            final String path = getZkRequestStateRoot();

            if (curator.checkExists().forPath(path) == null) {
                return ids;
            }

            final List<String> requests = curator.getChildren().forPath(path);

            for (String request : requests) {
                ids.add(new SidelineRequestIdentifier(request));
            }

            logger.debug("Existing sideline request identifiers = {}", ids);
        } catch (Exception ex) {
            logger.error("{}", ex);
        }

        return ids;
    }

    /**
     * List the partitions for the given sideline request.
     * @param id Identifier for the sideline request that you want the partitions for
     * @return A list of the partitions for the sideline request
     */
    @Override
    public Set<Integer> listSidelineRequestPartitions(final SidelineRequestIdentifier id) {
        verifyHasBeenOpened();

        final Set<Integer> partitions = Sets.newHashSet();

        try {
            final String path = getZkRequestStatePath(id.toString());

            if (curator.checkExists().forPath(path) == null) {
                return partitions;
            }

            final List<String> partitionNodes = curator.getChildren().forPath(path);

            for (String partition : partitionNodes) {
                partitions.add(Integer.valueOf(partition));
            }

            logger.debug("Partitions for sideline request {} = {}", id, partitions);
        } catch (Exception ex) {
            logger.error("{}", ex);
        }

        return Collections.unmodifiableSet(partitions);
    }

    private FilterChainStep parseJsonToFilterChainSteps(final Map<Object, Object> json) {
        if (json == null) {
            return null;
        }

        final String chainStepData = (String) json.get("filterChainStep");

        return Serializer.deserialize(chainStepData);
    }

    /**
     * Internal method to create a new Curator connection.
     */
    private CuratorFramework newCurator(final Map spoutConfig) {
        final int connectionTimeoutMs = ((Number) spoutConfig.get(SpoutConfig.PERSISTENCE_ZK_CONNECTION_TIMEOUT)).intValue();
        final int sessionTimeoutMs = ((Number) spoutConfig.get(SpoutConfig.PERSISTENCE_ZK_SESSION_TIMEOUT)).intValue();
        final int retryAttempts = ((Number) spoutConfig.get(SpoutConfig.PERSISTENCE_ZK_RETRY_ATTEMPTS)).intValue();
        final int retryIntervalMs = ((Number) spoutConfig.get(SpoutConfig.PERSISTENCE_ZK_RETRY_INTERVAL)).intValue();

        // Use builder to create new curator
        return CuratorFrameworkFactory
            .builder()
            .connectString(zkConnectionString)
            .connectionTimeoutMs(connectionTimeoutMs)
            .sessionTimeoutMs(sessionTimeoutMs)
            .retryPolicy(new RetryNTimes(retryAttempts, retryIntervalMs))
            .build();
    }

    /**
     * Internal method to write JSON structured data into a zookeeper node.
     * @param path - the node to write the JSON data into.
     * @param data - Map representation of JSON data to write.
     */
    private void writeJson(String path, Map data) {
        logger.debug("Zookeeper Writing {} the data {}", path, data.toString());
        writeBytes(path, JSONValue.toJSONString(data).getBytes(Charsets.UTF_8));
    }

    /**
     * Internal method for reading JSON from a zookeeper node.
     * @param path - the node containing JSON to read from.
     * @return - Map representing the JSON stored within the zookeeper node.
     */
    private Map<Object, Object> readJson(String path) {
        try {
            byte[] bytes = readBytes(path);
            if (bytes == null) {
                return null;
            }
            return (Map<Object, Object>) JSONValue.parse(new String(bytes, Charsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Internal method to read a node out of zookeeper.
     * @param path - the node's path to read from zookeeper.
     * @return - the bytes representing that node.
     */
    private byte[] readBytes(String path) {
        try {
            if (curator.checkExists().forPath(path) != null) {
                return curator.getData().forPath(path);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Internal method used to write a byte array into a zookeeper node.
     * @param path - the path to write data into.
     * @param bytes - the data to write into the node.
     */
    private void writeBytes(String path, byte[] bytes) {
        try {
            if (curator.checkExists().forPath(path) == null) {
                curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, bytes);
            } else {
                curator.setData().forPath(path, bytes);
            }
        } catch (KeeperException.NodeExistsException ex) {
            logger.warn("Tried creating node {} that already exists, cowardly refusing to overwrite it.", path);
        } catch (Exception ex) {
            logger.error("Unable to write bytes to Zookeeper {} {}", ex, ex.getStackTrace());
            throw new RuntimeException(ex);
        }
    }

    /**
     * Internal method to delete a node from Zookeeper.
     * @param path - the node to delete.
     */
    private void deleteNode(final String path) {
        try {
            // If it doesn't exist,
            if (curator.checkExists().forPath(path) == null) {
                // Nothing to do!
                logger.warn("Tried to delete {}, but it does not exist.", path);
                return;
            }

            // Delete.
            curator.delete().forPath(path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Removes a path only if it has no children.
     * @param path - path to remove.
     */
    private void deleteNodeIfNoChildren(final String path) {
        try {
            // If it doesn't exist,
            if (curator.checkExists().forPath(path) == null) {
                // Nothing to do!
                return;
            }

            // Delete.
            List<String> children = curator.getChildren().forPath(path);
            if (children.isEmpty()) {
                logger.info("Removing empty path {}", path);
                curator.delete().forPath(path);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return - The full zookeeper path to where our consumer state is stored.
     */
    String getZkConsumerStatePath(final String consumerId, final int partitionId) {
        return getZkRoot() + "/consumers/" + consumerId + "/" + String.valueOf(partitionId);
    }

    String getZkRequestStatePath(final String sidelineIdentifierStr) {
        return getZkRoot() + "/requests/" + sidelineIdentifierStr;
    }

    /**
     * @return - The full zookeeper path to where our consumer state is stored.
     */
    String getZkRequestStatePath(final String sidelineIdentifierStr, final int partitionId) {
        return getZkRequestStatePath(sidelineIdentifierStr) + "/" + partitionId;
    }

    /**
     * @return - The full zookeeper root to where our request state is stored.
     */
    String getZkRequestStateRoot() {
        return getZkRoot() + "/requests";
    }

    /**
     * @return - configured zookeeper root path.
     */
    String getZkRoot() {
        return zkRoot;
    }

    /**
     * @return - configured zookeeper connection string.
     */
    String getZkConnectionString() {
        return zkConnectionString;
    }

    /**
     * Makes sure we don't try to interact w/ this instance unless its been properly opened.
     */
    private void verifyHasBeenOpened() {
        if (curator == null) {
            throw new IllegalStateException("Instance has not been initialized via open() call yet!");
        }
    }
}
