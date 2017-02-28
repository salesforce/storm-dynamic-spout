package com.salesforce.storm.spout.sideline.persistence;

import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import com.salesforce.storm.spout.sideline.trigger.SidelineIdentifier;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.CreateMode;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * Persistence layer implemented using Zookeeper.
 * Why Zookeeper?  Because its easy, and you most likely have it around.
 */
public class ZookeeperPersistenceManager implements PersistenceManager {
    // Logger
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperPersistenceManager.class);

    // Config
    private final String zkConnectionString;
    private final String zkRoot;

    // Additional Config
    // TODO - Move into some kind of config/properties class/map/thing.
    private int zkSessionTimeout = 6000;
    private int zkConnectionTimeout = 6000;
    private int zkRetryAttempts = 10;
    private int zkRetryInterval = 10;

    // Zookeeper connection
    private CuratorFramework curator;

    /**
     * Constructor.
     *
     * @param zkServers - List of zookeeper hosts in the format of ["host1:2182", "host2:2181",..]
     * @param zkRoot - Root node / prefix to write entries under.
     */
    public ZookeeperPersistenceManager(List<String> zkServers, String zkRoot) {
        String serverPorts = "";
        for (String server : zkServers) {
            serverPorts = serverPorts + server + ",";
        }
        serverPorts = serverPorts.substring(0, serverPorts.length() - 1);
        this.zkConnectionString = serverPorts;
        this.zkRoot = zkRoot;
    }

    /**
     * Constructor.
     *
     * @param zkConnectionStr - Comma deliminated list of zookeeper hosts in the format of: "host1:2181,host2:2181,.."
     * @param zkRoot - Root node / prefix to write entries under.
     */
    public ZookeeperPersistenceManager(String zkConnectionStr, String zkRoot) {
        this.zkConnectionString = zkConnectionStr;
        this.zkRoot = zkRoot;
    }

    /**
     * Initializes the ConsumerStateManager.
     * In this particular implementation it connects to our zookeeper hosts using the Curator framework.
     */
    public void init() {
        try {
            curator = newCurator();
            curator.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (curator == null) {
            return;
        }
        curator.close();
        curator = null;
    }

    @Override
    public void persistConsumerState(final String consumerId, final ConsumerState consumerState) {
        writeJSON(getZkConsumerStatePath(consumerId), consumerState.getState());
    }

    @Override
    public ConsumerState retrieveConsumerState(final String consumerId) {
        Map<Object, Object> json = readJSON(getZkConsumerStatePath(consumerId));
        logger.info("Read state from Zookeeper: {}", json);

        // Parse to ConsumerState
        return parseJsonToConsumerState(json);
    }

    @Override
    public void persistSidelineRequestState(SidelineIdentifier id, ConsumerState state) {
        writeJSON(getZkRequestStatePath(id.toString()), state.getState());
    }

    @Override
    public ConsumerState retrieveSidelineRequestState(SidelineIdentifier id) {
        Map<Object, Object> json = readJSON(getZkRequestStatePath(id.toString()));
        logger.info("Read request state from Zookeeper: {}", json);

        // Parse to ConsumerState
        return parseJsonToConsumerState(json);
    }

    private ConsumerState parseJsonToConsumerState(final Map<Object, Object> json) {
        // Create new ConsumerState instance.
        final ConsumerState consumerState = new ConsumerState();

        // If no state is stored yet.
        if (json == null) {
            // Return empty consumerState
            return consumerState;
        }

        // Otherwise parse the stored json
        for (Object key: json.keySet()) {
            String[] bits = ((String)key).split("-");

            // Populate consumerState.
            consumerState.setOffset(new TopicPartition(bits[0], Integer.valueOf(bits[1])), (Long)json.get(key));
        }
        return consumerState;
    }

    private CuratorFramework newCurator() throws Exception {
        return CuratorFrameworkFactory.newClient(zkConnectionString, zkSessionTimeout, zkConnectionTimeout, new RetryNTimes(zkRetryAttempts, zkRetryInterval));
    }

    /**
     * @return - The full zookeeper path to where our consumer state is stored.
     */
    protected String getZkConsumerStatePath(final String consumerId) {
        return new StringBuilder(getZkRoot()).append("/consumers/").append(consumerId).toString();
    }

    /**
     * @return - The full zookeeper path to where our consumer state is stored.
     */
    protected String getZkRequestStatePath(final String sidelineIdentifierStr) {
        return new StringBuilder(getZkRoot()).append("/requests/").append(sidelineIdentifierStr).toString();
    }

    private void writeJSON(String path, Map data) {
        logger.info("Zookeeper Writing {} the data {}", path, data.toString());
        writeBytes(path, JSONValue.toJSONString(data).getBytes(Charset.forName("UTF-8")));
    }

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

    private Map<Object, Object> readJSON(String path) {
        try {
            byte[] bytes = readBytes(path);
            if (bytes == null) {
                return null;
            }
            return (Map<Object, Object>) JSONValue.parse(new String(bytes, "UTF-8"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

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
     * @return - our configured zookeeper root path.
     */
    public String getZkRoot() {
        return zkRoot;
    }

    protected String getZkConnectionString() {
        return zkConnectionString;
    }
}
