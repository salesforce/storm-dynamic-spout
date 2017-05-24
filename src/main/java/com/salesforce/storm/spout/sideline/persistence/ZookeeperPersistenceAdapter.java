package com.salesforce.storm.spout.sideline.persistence;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.filter.FilterChainStep;
import com.salesforce.storm.spout.sideline.filter.Serializer;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
        final List<String> zkServers = (List<String>) spoutConfig.get(SidelineSpoutConfig.PERSISTENCE_ZK_SERVERS);

        // Root node / prefix to write entries under.
        final String zkRoot = (String) spoutConfig.get(SidelineSpoutConfig.PERSISTENCE_ZK_ROOT);
        if (Strings.isNullOrEmpty(zkRoot)) {
            throw new IllegalStateException("Missing required configuration: " + SidelineSpoutConfig.PERSISTENCE_ZK_ROOT);
        }

        // Build out our bits and pieces.
        StringBuilder stringBuilder = new StringBuilder();
        for (String server : zkServers) {
            stringBuilder.append(server).append(",");
        }
        String serverPorts = stringBuilder.toString();
        serverPorts = serverPorts.substring(0, serverPorts.length() - 1);
        this.zkConnectionString = serverPorts;
        this.zkRoot = zkRoot;

        try {
            curator = newCurator(spoutConfig);
            curator.start();
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
            new SidelineRequest(step),
            startingOffset,
            endingOffset
        );
    }

    /**
     * Removes a sideline request from the persistence layer.
     * @param id - SidelineRequestIdentifier you want to clear.
     * @param partitionId
     */
    @Override
    public void clearSidelineRequest(SidelineRequestIdentifier id, int partitionId) {
        // Validate we're in a state that can be used.
        verifyHasBeenOpened();

        // Delete!
        final String path = getZkRequestStatePath(id.toString(), partitionId);
        logger.info("Delete request from Zookeeper at {}", path);
        deleteNode(path);
    }

    /**
     * Lists out a unique list of current sideline requests
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
        return CuratorFrameworkFactory.newClient(
            zkConnectionString,
            ((Number) spoutConfig.get(SidelineSpoutConfig.PERSISTENCE_ZK_SESSION_TIMEOUT)).intValue(),
            ((Number) spoutConfig.get(SidelineSpoutConfig.PERSISTENCE_ZK_CONNECTION_TIMEOUT)).intValue(),
            new RetryNTimes(
                ((Number) spoutConfig.get(SidelineSpoutConfig.PERSISTENCE_ZK_RETRY_ATTEMPTS)).intValue(),
                ((Number) spoutConfig.get(SidelineSpoutConfig.PERSISTENCE_ZK_RETRY_INTERVAL)).intValue()
            )
        );
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
        } catch (Exception e) {
            throw new RuntimeException(e);
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

    /**
     * @return - The full zookeeper path to where our consumer state is stored.
     */
    String getZkRequestStatePath(final String sidelineIdentifierStr, final int partitionId) {
        return getZkRoot() + "/requests/" + sidelineIdentifierStr + "/" + partitionId;
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
