package com.salesforce.storm.spout.sideline.kafka.consumerState;

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
 *
 */
public class ZookeeperConsumerStateManager implements ConsumerStateManager {
    // Logger
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperConsumerStateManager.class);

    // Config
    private final String zkConnectionString;
    private final String zkRoot;
    private final String consumerId;

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
    public ZookeeperConsumerStateManager(List<String> zkServers, String zkRoot, String consumerId) {
        String serverPorts = "";
        for (String server : zkServers) {
            serverPorts = serverPorts + server + ",";
        }
        serverPorts = serverPorts.substring(0, serverPorts.length() - 1);
        this.zkConnectionString = serverPorts;
        this.zkRoot = zkRoot;
        this.consumerId = consumerId;
    }

    /**
     * Constructor.
     *
     * @param zkConnectionStr - Comma deliminated list of zookeeper hosts in the format of: "host1:2181,host2:2181,.."
     * @param zkRoot - Root node / prefix to write entries under.
     */
    public ZookeeperConsumerStateManager(String zkConnectionStr, String zkRoot, String consumerId) {
        this.zkConnectionString = zkConnectionStr;
        this.zkRoot = zkRoot;
        this.consumerId = consumerId;
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

    private CuratorFramework newCurator() throws Exception {
        return CuratorFrameworkFactory.newClient(zkConnectionString, zkSessionTimeout, zkConnectionTimeout, new RetryNTimes(zkRetryAttempts, zkRetryInterval));
    }

    /**
     * Pass in the consumer state that you'd like persisted.
     * In this implementation it will write the state in JSON format into zookeeper.
     * @param consumerState - ConsumerState to be persisted.
     */
    @Override
    public void persistState(ConsumerState consumerState) {
        writeJSON(getZkStatePath(), consumerState.getState());
    }

    /**
     * Retrieves the consumer state from the persistence layer.
     * @return
     */
    @Override
    public ConsumerState getState() {
        Map<Object, Object> json = readJSON(getZkStatePath());
        logger.info("Read state from Zookeeper: {}", json);

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


    public void writeJSON(String path, Map data) {
        logger.info("Zookeeper Writing {} the data {}", path, data.toString());
        writeBytes(path, JSONValue.toJSONString(data).getBytes(Charset.forName("UTF-8")));
    }

    public void writeBytes(String path, byte[] bytes) {
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

    public Map<Object, Object> readJSON(String path) {
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

    public byte[] readBytes(String path) {
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

    public void close() {
        if (curator == null) {
            return;
        }
        curator.close();
        curator = null;
    }

    public String getZkConnectionString() {
        return zkConnectionString;
    }

    public String getZkRoot() {
        return zkRoot;
    }

    public String getConsumerId() {
        return consumerId;
    }

    /**
     * @return - The full zookeeper path to where our consumer state is stored.
     */
    public String getZkStatePath() {
        return new StringBuilder(getZkRoot()).append("/").append(getConsumerId()).toString();
    }
}
