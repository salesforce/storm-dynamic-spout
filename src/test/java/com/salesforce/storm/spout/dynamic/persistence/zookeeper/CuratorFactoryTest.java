package com.salesforce.storm.spout.dynamic.persistence.zookeeper;

import com.salesforce.storm.spout.dynamic.utils.SharedZookeeperTestResource;
import org.apache.curator.framework.CuratorFramework;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * This aims to be a simple smoke test doing minimal validation.
 */
public class CuratorFactoryTest {

    /**
     * Standup a test server.
     */
    @ClassRule
    public static SharedZookeeperTestResource sharedZookeeperTestResource = new SharedZookeeperTestResource();

    /**
     * Smoke test over factory.
     */
    @Test
    public void smokeTestFactory() throws Exception {
        // Create list of Servers
        final String serverStr = sharedZookeeperTestResource.getZookeeperConnectString();
        final List<String> serverList = Arrays.asList(serverStr.split(","));

        // Create config map
        final Map<String, Object> config = new HashMap<>();
        config.put("servers", serverList);

        try (final CuratorFramework curatorFramework = CuratorFactory.createNewCuratorInstance(config)) {
            assertNotNull(curatorFramework);

            // Execute a command
            curatorFramework.getChildren().forPath("/");
        }
    }
}