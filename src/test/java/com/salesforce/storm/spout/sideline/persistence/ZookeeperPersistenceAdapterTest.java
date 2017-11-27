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

package com.salesforce.storm.spout.sideline.persistence;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.salesforce.kafka.test.junit.SharedZookeeperTestResource;
import com.google.gson.GsonBuilder;
import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import com.salesforce.storm.spout.dynamic.Tools;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerState;
import com.salesforce.storm.spout.dynamic.filter.FilterChainStep;
import com.salesforce.storm.spout.dynamic.filter.StaticMessageFilter;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests our Zookeeper Persistence layer.
 */
public class ZookeeperPersistenceAdapterTest {

    // For logging within test.
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperPersistenceAdapterTest.class);

    /**
     * Create shared zookeeper test server.
     */
    @ClassRule
    public static final SharedZookeeperTestResource sharedZookeeperTestResource = new SharedZookeeperTestResource();

    /**
     * By default no expected exceptions.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Tests that if you're missing the configuration item for ZkRootNode it will throw
     * an IllegalStateException.
     */
    @Test
    public void testOpenMissingConfigForZkRootNode() {
        final List<String> inputHosts = Lists.newArrayList("localhost:2181", "localhost2:2183");

        // Create our config
        final AbstractConfig spoutConfig = createDefaultConfig(inputHosts, null, null);

        // Create instance and open it.
        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("root is required");
        persistenceAdapter.open(spoutConfig);
    }

    /**
     * Tests that the constructor does what we think.
     */
    @Test
    public void testOpen() {
        final String namespace = "Test";
        final int partitionId = 1;
        final String configuredConsumerPrefix = "consumerIdPrefix";
        final String configuredZkRoot = getRandomZkRootNode();
        final String expectedZkRoot = configuredZkRoot + "/" + configuredConsumerPrefix;
        final String expectedConsumerId = configuredConsumerPrefix + ":MyConsumerId";
        final String expectedZkRequestStatePath = expectedZkRoot + "/requests/" + expectedConsumerId + "/" + namespace + "/"
            + String.valueOf(partitionId);

        // Create our config
        final AbstractConfig spoutConfig = createDefaultConfig(
            getZkServer().getConnectString(),
            configuredZkRoot,
            configuredConsumerPrefix);

        // Create instance and open it.
        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();
        persistenceAdapter.open(spoutConfig);

        // Validate
        assertEquals("Unexpected zk root string", expectedZkRoot, persistenceAdapter.getZkRoot());

        assertEquals(
            "Unexpected zkRequestStatePath returned",
            expectedZkRequestStatePath,
            persistenceAdapter.getZkRequestStatePathForConsumerPartition(expectedConsumerId, new ConsumerPartition(namespace, partitionId))
        );

        // Close everyone out
        persistenceAdapter.close();
    }

    /**
     * Does an end to end test of this persistence layer for storing/retrieving request state.
     * 1 - Sets up an internal Zk server
     * 2 - Connects to it
     * 3 - writes state data to it
     * 4 - reads state data from it
     * 5 - compares that its valid.
     */
    @Test
    public void testEndToEndRequestStatePersistence() throws InterruptedException {
        final String configuredConsumerPrefix = "consumerIdPrefix";
        final String configuredZkRoot = getRandomZkRootNode();

        final String topicName = "MyTopic1";
        final String zkRootPath = configuredZkRoot + "/" + configuredConsumerPrefix;
        final SidelineRequestIdentifier sidelineRequestIdentifier = new SidelineRequestIdentifier("test");
        final FilterChainStep filterChainStep =  new StaticMessageFilter();
        final SidelineRequest sidelineRequest = new SidelineRequest(sidelineRequestIdentifier, filterChainStep);

        // Create our config
        final AbstractConfig spoutConfig = createDefaultConfig(
            getZkServer().getConnectString(),
            configuredZkRoot,
            configuredConsumerPrefix);

        // Create instance and open it.
        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();
        persistenceAdapter.open(spoutConfig);

        // Create state
        final ConsumerState consumerState = ConsumerState.builder()
            .withPartition(new ConsumerPartition(topicName, 0), 10L)
            .withPartition(new ConsumerPartition(topicName, 1), 1000L)
            .withPartition(new ConsumerPartition(topicName, 3), 3000L)
            .build();

        // Persist it
        logger.info("Persisting {}", consumerState);

        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            sidelineRequestIdentifier,
            sidelineRequest,
            new ConsumerPartition(topicName, 0),
            10L,
            11L
        );
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            sidelineRequestIdentifier,
            sidelineRequest,
            new ConsumerPartition(topicName, 1),
            100L,
            101L
        );
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            sidelineRequestIdentifier,
            sidelineRequest,
            new ConsumerPartition(topicName, 2),
            1000L,
            1001L
        );

        // Attempt to read it?
        SidelinePayload result1 = persistenceAdapter.retrieveSidelineRequest(
            sidelineRequestIdentifier,
            new ConsumerPartition(topicName, 0)
        );
        SidelinePayload result2 = persistenceAdapter.retrieveSidelineRequest(
            sidelineRequestIdentifier,
            new ConsumerPartition(topicName, 1)
        );
        SidelinePayload result3 = persistenceAdapter.retrieveSidelineRequest(
            sidelineRequestIdentifier,
            new ConsumerPartition(topicName, 2)
        );

        logger.info("Result {} {} {}", result1, result2, result3);

        assertNotNull("Got an object back", result1);
        assertEquals("Starting offset matches", Long.valueOf(10L), result1.startingOffset);
        assertEquals("Ending offset matches", Long.valueOf(11L), result1.endingOffset);

        assertNotNull("Got an object back", result2);
        assertEquals("Starting offset matches", Long.valueOf(100L), result2.startingOffset);
        assertEquals("Ending offset matches", Long.valueOf(101L), result2.endingOffset);

        assertNotNull("Got an object back", result3);
        assertEquals("Starting offset matches", Long.valueOf(1000L), result3.startingOffset);
        assertEquals("Ending offset matches", Long.valueOf(1001L), result3.endingOffset);

        // Close outs
        persistenceAdapter.close();

        // Create new instance, reconnect to ZK, make sure we can still read it out with our new instance.
        persistenceAdapter = new ZookeeperPersistenceAdapter();
        persistenceAdapter.open(spoutConfig);

        // Re-retrieve, should still be there.
        // Attempt to read it?
        // Attempt to read it?
        result1 = persistenceAdapter.retrieveSidelineRequest(sidelineRequestIdentifier, new ConsumerPartition(topicName, 0));
        result2 = persistenceAdapter.retrieveSidelineRequest(sidelineRequestIdentifier, new ConsumerPartition(topicName, 1));
        result3 = persistenceAdapter.retrieveSidelineRequest(sidelineRequestIdentifier, new ConsumerPartition(topicName, 2));

        logger.info("Result {} {} {}", result1, result2, result3);

        assertNotNull("Got an object back", result1);
        assertEquals("Starting offset matches", Long.valueOf(10L), result1.startingOffset);
        assertEquals("Ending offset matches", Long.valueOf(11L), result1.endingOffset);

        assertNotNull("Got an object back", result2);
        assertEquals("Starting offset matches", Long.valueOf(100L), result2.startingOffset);
        assertEquals("Ending offset matches", Long.valueOf(101L), result2.endingOffset);

        assertNotNull("Got an object back", result3);
        assertEquals("Starting offset matches", Long.valueOf(1000L), result3.startingOffset);
        assertEquals("Ending offset matches", Long.valueOf(1001L), result3.endingOffset);

        // Clear out hose requests
        persistenceAdapter.clearSidelineRequest(sidelineRequestIdentifier, new ConsumerPartition(topicName, 0));
        persistenceAdapter.clearSidelineRequest(sidelineRequestIdentifier, new ConsumerPartition(topicName, 1));
        persistenceAdapter.clearSidelineRequest(sidelineRequestIdentifier, new ConsumerPartition(topicName, 2));

        // Attempt to retrieve those sideline requests, they should come back null
        result1 = persistenceAdapter.retrieveSidelineRequest(sidelineRequestIdentifier, new ConsumerPartition(topicName, 0));
        result2 = persistenceAdapter.retrieveSidelineRequest(sidelineRequestIdentifier, new ConsumerPartition(topicName, 1));
        result3 = persistenceAdapter.retrieveSidelineRequest(sidelineRequestIdentifier, new ConsumerPartition(topicName, 2));

        logger.info("Result {} {} {}", result1, result2, result3);

        assertNull("Sideline request was cleared", result1);
        assertNull("Sideline request was cleared", result2);
        assertNull("Sideline request was cleared", result3);

        // Close outs
        persistenceAdapter.close();
    }

    /**
     * Tests end to end persistence of Consumer state, using an independent ZK client to verify things are written
     * into zookeeper as we expect.
     *
     * We do the following:
     * 1 - Connect to ZK and ensure that the zkRootNode path does NOT exist in Zookeeper yet
     *     If it does, we'll clean it up.
     * 2 - Create an instance of our state manager passing an expected root node
     * 3 - Attempt to persist some state
     * 4 - Go into zookeeper directly and verify the state got written under the appropriate prefix path (zkRootNode).
     * 5 - Read the stored value directly out of zookeeper and verify the right thing got written.
     */
    @Test
    public void testEndToEndRequestStatePersistenceWithValidationWithIndependentZkClient()
        throws IOException, KeeperException, InterruptedException {
        final String configuredConsumerPrefix = "consumerIdPrefix";
        final String configuredZkRoot = getRandomZkRootNode();

        // Define our ZK Root Node
        final String zkRootNodePath = configuredZkRoot + "/" + configuredConsumerPrefix;
        final String zkRequestsRootNodePath = zkRootNodePath + "/requests";
        final SidelineRequestIdentifier sidelineRequestIdentifier = new SidelineRequestIdentifier("test");
        final FilterChainStep filterChainStep = new StaticMessageFilter("test");
        final SidelineRequest sidelineRequest = new SidelineRequest(sidelineRequestIdentifier, filterChainStep);

        // 1 - Connect to ZK directly
        ZooKeeper zookeeperClient = new ZooKeeper(getZkServer().getConnectString(), 6000, event -> logger.info("Got event {}", event));

        // Ensure that our node does not exist before we run test,
        // Validate that our assumption that this node does not exist!
        Stat doesNodeExist = zookeeperClient.exists(zkRootNodePath, false);
        // We need to clean up
        if (doesNodeExist != null) {
            zookeeperClient.delete(zkRootNodePath, doesNodeExist.getVersion());

            // Check again
            doesNodeExist = zookeeperClient.exists(zkRootNodePath, false);
            if (doesNodeExist != null) {
                throw new RuntimeException("Failed to ensure zookeeper was clean before running test");
            }
        }

        // 2. Create our instance and open it
        final AbstractConfig spoutConfig = createDefaultConfig(
            getZkServer().getConnectString(),
            configuredZkRoot,
            configuredConsumerPrefix);

        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();
        persistenceAdapter.open(spoutConfig);

        // 3. Attempt to persist some state.
        final String topicName = "MyTopic";

        // Define our expected result that will be stored in zookeeper
        final Map<String,String> idMap = new HashMap<>();
        idMap.put("id", "test");

        final Map<String,Object> requestMap = new HashMap<>();
        requestMap.put("id", idMap);
        requestMap.put(
            "step",
            "rO0ABXNyAD1jb20uc2FsZXNmb3JjZS5zdG9ybS5zcG91dC5keW5hbWljLmZpbHRlci5TdGF0aWNNZXN"
            + "zYWdlRmlsdGVy3eauq5nDVrUCAAFMAAJpZHQAEkxqYXZhL2xhbmcvU3RyaW5nO3hwdAAEdGVzdA=="
        );

        final Map<String,Object> expectedJsonMap = Maps.newHashMap();
        expectedJsonMap.put("request", requestMap);
        expectedJsonMap.put("type", SidelineType.START.toString());
        expectedJsonMap.put("id", idMap);
        expectedJsonMap.put("startingOffset", 1L);
        expectedJsonMap.put("endingOffset", 2L);

        Gson gson = new GsonBuilder().create();
        final String expectedStoredState = gson.toJson(expectedJsonMap);

        logger.info("expectedStoredState = {}", expectedStoredState);

        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            sidelineRequestIdentifier,
            sidelineRequest,
            new ConsumerPartition(topicName, 0),
            1L,
            2L
        );

        // Since this is an async operation, use await() to watch for the change
        await()
            .atMost(6, TimeUnit.SECONDS)
            .until(() -> zookeeperClient.exists(zkRequestsRootNodePath, false), notNullValue());

        // 4. Go into zookeeper and see where data got written
        doesNodeExist = zookeeperClient.exists(zkRequestsRootNodePath, false);
        logger.debug("Result {}", doesNodeExist);
        assertNotNull("Our root node should now exist", doesNodeExist);

        // Now attempt to read our state
        List<String> childrenNodes = zookeeperClient.getChildren(zkRequestsRootNodePath, false);
        logger.debug("Children Node Names {}", childrenNodes);

        // We should have a single child
        assertEquals("Should have a single filter", 1, childrenNodes.size());

        // Grab the child node node
        final String childNodeName = childrenNodes.get(0);
        assertNotNull("Child Node Name should not be null", childNodeName);
        assertEquals("Child Node name not correct", sidelineRequestIdentifier.toString(), childNodeName);

        // 5. Grab the value and validate it
        final byte[] storedDataBytes = zookeeperClient.getData(
            zkRequestsRootNodePath + "/" + sidelineRequestIdentifier.toString() + "/" + topicName + "/" + 0,
            false,
            null
        );
        logger.debug("Stored data bytes {}", storedDataBytes);
        assertNotEquals("Stored bytes should be non-zero", 0, storedDataBytes.length);

        // Convert to a string
        final String storedDataStr = new String(storedDataBytes, Charsets.UTF_8);
        logger.info("Stored data string {}", storedDataStr);
        assertNotNull("Stored data string should be non-null", storedDataStr);

        Map expectedMap = gson.fromJson(expectedStoredState, HashMap.class);
        Map actualMap = gson.fromJson(storedDataStr, HashMap.class);

        assertEquals(expectedMap, actualMap);

        assertEquals(expectedMap.get("id"), actualMap.get("id"));
        assertEquals(expectedMap.get("type"), actualMap.get("type"));
        assertEquals(expectedMap.get("startingOffset"), actualMap.get("startingOffset"));
        assertEquals(expectedMap.get("endingOffset"), actualMap.get("endingOffset"));
        assertEquals(
            ((Map<String,Object>) expectedMap.get("request")).get("id"),
            ((Map<String,Object>) actualMap.get("request")).get("id")
        );
        assertEquals(
            ((Map<String,Object>) expectedMap.get("request")).get("step"),
            ((Map<String,Object>) actualMap.get("request")).get("step")
        );

        // Now test clearing
        persistenceAdapter.clearSidelineRequest(sidelineRequestIdentifier, new ConsumerPartition(topicName, 0));

        // Validate in the Zk Client.
        doesNodeExist = zookeeperClient.exists(
            zkRequestsRootNodePath + "/" + sidelineRequestIdentifier.toString() + "/" + topicName + "/" + 0,
            false
        );

        logger.debug("Result {}", doesNodeExist);
        assertNull("Our partition node should No longer exist", doesNodeExist);

        doesNodeExist = zookeeperClient.exists(
            zkRequestsRootNodePath + "/" + sidelineRequestIdentifier.toString(),
            false
        );
        assertNull("Our partition node should No longer exist", doesNodeExist);

        // Close everyone out
        persistenceAdapter.close();
        zookeeperClient.close();
    }

    @Rule
    public ExpectedException expectedExceptionPersistSidelineRequestStateBeforeBeingOpened = ExpectedException.none();

    /**
     * Verify we get an exception if you try to persist before calling open().
     */
    @Test
    public void testPersistSidelineRequestStateBeforeBeingOpened() {
        // Create our instance
        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();

        final SidelineRequest sidelineRequest = new SidelineRequest(new SidelineRequestIdentifier("test"), null);

        // Call method and watch for exception
        expectedExceptionPersistSidelineRequestStateBeforeBeingOpened.expect(IllegalStateException.class);
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            new SidelineRequestIdentifier("test"),
            sidelineRequest,
            new ConsumerPartition("Foobar", 0),
            1L,
            2L
        );
    }

    @Rule
    public ExpectedException expectedExceptionRetrieveSidelineRequestStateBeforeBeingOpened = ExpectedException.none();

    /**
     * Verify we get an exception if you try to retrieve before calling open().
     */
    @Test
    public void testRetrieveSidelineRequestStateBeforeBeingOpened() {
        // Create our instance
        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();

        // Call method and watch for exception
        expectedExceptionRetrieveSidelineRequestStateBeforeBeingOpened.expect(IllegalStateException.class);
        persistenceAdapter.retrieveSidelineRequest(new SidelineRequestIdentifier("test"), new ConsumerPartition("Foobar", 0));
    }

    @Rule
    public ExpectedException expectedExceptionClearSidelineRequestBeforeBeingOpened = ExpectedException.none();

    /**
     * Verify we get an exception if you try to persist before calling open().
     */
    @Test
    public void testClearSidelineRequestBeforeBeingOpened() {
        // Create our instance
        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();

        // Call method and watch for exception
        expectedExceptionClearSidelineRequestBeforeBeingOpened.expect(IllegalStateException.class);
        persistenceAdapter.clearSidelineRequest(new SidelineRequestIdentifier("test"), new ConsumerPartition("Foobar", 0));
    }

    @Test
    public void testListSidelineRequests() {
        final String topicName = "MyTopic";
        final String configuredConsumerPrefix = "consumerIdPrefix";
        final String configuredZkRoot = getRandomZkRootNode();

        final AbstractConfig spoutConfig = createDefaultConfig(
            getZkServer().getConnectString(),
            configuredZkRoot,
            configuredConsumerPrefix);

        // Create adapter and open
        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();
        persistenceAdapter.open(spoutConfig);

        final SidelineRequestIdentifier sidelineRequestIdentifier1 = new SidelineRequestIdentifier("test1");
        final SidelineRequest sidelineRequest1 = new SidelineRequest(sidelineRequestIdentifier1, null);

        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            sidelineRequestIdentifier1,
            sidelineRequest1,
            new ConsumerPartition(topicName, 0),
            10L,
            11L
        );
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            sidelineRequestIdentifier1,
            sidelineRequest1,
            new ConsumerPartition(topicName, 1),
            10L,
            11L
        );

        final SidelineRequestIdentifier sidelineRequestIdentifier2 = new SidelineRequestIdentifier("test2");
        final SidelineRequest sidelineRequest2 = new SidelineRequest(sidelineRequestIdentifier2, null);

        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            sidelineRequestIdentifier2,
            sidelineRequest2,
            new ConsumerPartition(topicName, 0),
            100L,
            101L
        );

        final SidelineRequestIdentifier sidelineRequestIdentifier3 = new SidelineRequestIdentifier("test3");
        final SidelineRequest sidelineRequest3 = new SidelineRequest(sidelineRequestIdentifier3, null);

        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            sidelineRequestIdentifier3,
            sidelineRequest3,
            new ConsumerPartition(topicName, 0),
            1000L,
            1001L
        );
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            sidelineRequestIdentifier3,
            sidelineRequest3,
            new ConsumerPartition(topicName, 1),
            1000L,
            1001L
        );
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            sidelineRequestIdentifier3,
            sidelineRequest3,
            new ConsumerPartition(topicName, 2),
            1000L,
            1001L
        );

        final List<SidelineRequestIdentifier> ids = persistenceAdapter.listSidelineRequests();

        assertNotNull(ids);
        assertTrue(ids.size() == 3);
        assertTrue(ids.contains(sidelineRequestIdentifier1));
        assertTrue(ids.contains(sidelineRequestIdentifier2));
        assertTrue(ids.contains(sidelineRequestIdentifier3));

        // Close adapter
        persistenceAdapter.close();
    }

    /**
     * Test that given a sideline request we receive a set of partition ids for it.
     */
    @Test
    public void testListSidelineRequestPartitions() {
        final String configuredConsumerPrefix = "consumerIdPrefix";
        final String configuredZkRoot = getRandomZkRootNode();
        final String topicName = "MyTopic";

        final AbstractConfig spoutConfig = createDefaultConfig(
            getZkServer().getConnectString(),
            configuredZkRoot,
            configuredConsumerPrefix);

        // Create adapter and open
        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();
        persistenceAdapter.open(spoutConfig);

        final SidelineRequestIdentifier sidelineRequestIdentifier1 = new SidelineRequestIdentifier("test1");
        final SidelineRequest sidelineRequest1 = new SidelineRequest(sidelineRequestIdentifier1, null);

        final SidelineRequestIdentifier sidelineRequestIdentifier2 = new SidelineRequestIdentifier("test2");
        final SidelineRequest sidelineRequest2 = new SidelineRequest(sidelineRequestIdentifier2, null);

        // Two partitions for sideline request 1
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            sidelineRequestIdentifier1,
            sidelineRequest1,
            new ConsumerPartition(topicName, 0),
            10L,
            11L
        );
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            sidelineRequestIdentifier1,
            sidelineRequest1,
            new ConsumerPartition(topicName, 1),
            10L,
            11L
        );
        // One partition for sideline request 2
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            sidelineRequestIdentifier2,
            sidelineRequest1,
            new ConsumerPartition(topicName, 0),
            10L,
            11L
        );

        Set<ConsumerPartition> partitionsForSidelineRequest1 = persistenceAdapter.listSidelineRequestPartitions(sidelineRequestIdentifier1);

        assertEquals(
            Sets.newHashSet(new ConsumerPartition(topicName, 0), new ConsumerPartition(topicName, 1)),
            partitionsForSidelineRequest1
        );

        Set<ConsumerPartition> partitionsForSidelineRequest2 = persistenceAdapter.listSidelineRequestPartitions(sidelineRequestIdentifier2);

        assertEquals(
            Sets.newHashSet(new ConsumerPartition(topicName, 0)),
            partitionsForSidelineRequest2
        );

        // Close adapter
        persistenceAdapter.close();
    }

    /**
     * Helper method.
     */
    private AbstractConfig createDefaultConfig(List<String> zkServers, String zkRootNode, String consumerIdPrefix) {
        final Map<String, Object> config = new HashMap<>();
        config.put(SidelineConfig.PERSISTENCE_ZK_SERVERS, zkServers);
        config.put(SidelineConfig.PERSISTENCE_ZK_ROOT, zkRootNode);
        config.put(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX, consumerIdPrefix);

        return new AbstractConfig(new ConfigDefinition(), config);
    }

    /**
     * Helper method.
     */
    private AbstractConfig createDefaultConfig(String zkServers, String zkRootNode, String consumerIdPrefix) {
        return createDefaultConfig(Lists.newArrayList(Tools.splitAndTrim(zkServers)), zkRootNode, consumerIdPrefix);
    }

    /**
     * Helper method to generate a random zkRootNode path to use.
     */
    private String getRandomZkRootNode() {
        return "/testRoot" + System.currentTimeMillis();
    }

    /**
     * Simple accessor.
     */
    private TestingServer getZkServer() {
        return sharedZookeeperTestResource.getZookeeperTestServer();
    }
}