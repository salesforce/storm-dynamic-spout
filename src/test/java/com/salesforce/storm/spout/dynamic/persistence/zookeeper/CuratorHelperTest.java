package com.salesforce.storm.spout.dynamic.persistence.zookeeper;

import com.google.common.base.Charsets;
import com.salesforce.storm.spout.dynamic.utils.SharedZookeeperTestResource;
import org.apache.curator.framework.CuratorFramework;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class CuratorHelperTest {

    /**
     * Start embedded ZK server.
     */
    @ClassRule
    public static SharedZookeeperTestResource sharedZookeeperTestResource = new SharedZookeeperTestResource();

    /**
     * Test reading and writing bytes.
     */
    @Test
    public void testReadAndWriteBytes() {
        final String path = "/test";
        final String valueStr = "My String";
        final byte[] valueBytes = valueStr.getBytes(Charsets.UTF_8);

        try (final CuratorFramework curator = createCurator()) {
            // Write
            final CuratorHelper curatorHelper = new CuratorHelper(curator);
            curatorHelper.writeBytes(path, valueBytes);

            // Read
            final byte[] resultBytes = curatorHelper.readBytes(path);
            final String resultStr = new String(resultBytes, Charsets.UTF_8);

            assertEquals("Has correct value", valueStr, resultStr);
        }
    }

    /**
     * Test reading and writing bytes.
     */
    @Test
    public void testReadAndWriteJson() {
        final String path = "/jsonTest";

        // Define input map
        final Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("key1", "value1");
        jsonMap.put("key2", 2L);
        jsonMap.put("key3", true);

        try (final CuratorFramework curator = createCurator()) {
            // Write
            final CuratorHelper curatorHelper = new CuratorHelper(curator);
            curatorHelper.writeJson(path, jsonMap);

            // Read
            final Map<String, Object> resultMap = curatorHelper.readJson(path);

            // Validate
            assertNotNull("Not null result", resultMap);
            assertEquals("Has 3 keys", 3, resultMap.size());
            assertEquals("Has key 1", "value1", resultMap.get("key1"));
            assertEquals("Has key 2", 2L, resultMap.get("key2"));
            assertEquals("Has key 3", true, resultMap.get("key3"));
        }
    }

    /**
     * Uses CuratorFactory to create a curator instance.
     */
    private CuratorFramework createCurator() {
        // Create list of Servers
        final String serverStr = sharedZookeeperTestResource.getZookeeperConnectString();
        final List<String> serverList = Arrays.asList(serverStr.split(","));

        // Create config map
        final Map<String, Object> config = new HashMap<>();
        config.put("servers", serverList);

        return CuratorFactory.createNewCuratorInstance(config);
    }
}