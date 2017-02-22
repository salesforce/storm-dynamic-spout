package com.salesforce.storm.spout.sideline.trigger;

import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * A class that handles persisting starting and stopping offsets by some "identifier."
 */
public class RequestDataStore {
    private static final Logger logger = LoggerFactory.getLogger(RequestDataStore.class);

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
    public RequestDataStore(List<String> zkServers, String zkRoot) {
        String serverPorts = "";
        for (String server : zkServers) {
            serverPorts = serverPorts + server + ",";
        }
        serverPorts = serverPorts.substring(0, serverPorts.length() - 1);
        this.zkConnectionString = serverPorts;
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

    private CuratorFramework newCurator() throws Exception {
        return CuratorFrameworkFactory.newClient(zkConnectionString, zkSessionTimeout, zkConnectionTimeout, new RetryNTimes(zkRetryAttempts, zkRetryInterval));
    }

    public void writeStartRequest(StartRequest startRequest, ConsumerState startingState) {
        final String identifier = "1";
        //final String topic = startRequest.getTopic();
        final String topic = "poop";
        final String path = zkRoot + "/startRequests/" + topic + "/" + identifier;
        writeJSON(path, startingState.getState());
    }

    public StartRequest readStartRequest(final String identifier, final String topic) {
        final String path = zkRoot + "/startRequests/" + topic + "/" + identifier;
        readJSON(path);
        return null;
    }

    public void writeStopRequest(StopRequest stopRequest, ConsumerState endingState) {
        final String identifier = "1";
        //final String topic = stopRequest.getTopic();
        final String topic = "poop";

        final String path = zkRoot + "/stopRequests/" + topic + "/" + identifier;
        writeJSON(path, endingState.getState());
    }

    public StopRequest readStopRequest(final String identifier, final String topic) {
        final String path = zkRoot + "/stopRequests/" + topic + "/" + identifier;
        readJSON(path);
        return null;
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
            byte[] b = readBytes(path);
            if (b == null) {
                return null;
            }
            return (Map<Object, Object>) JSONValue.parse(new String(b, "UTF-8"));
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

    public void close() {
        if (curator == null) {
            return;
        }
        curator.close();
        curator = null;
    }

}
