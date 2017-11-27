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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.kafka.test.junit.SharedZookeeperTestResource;
import com.salesforce.storm.spout.dynamic.Tools;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
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
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
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
        final SpoutConfig spoutConfig = createDefaultConfig(inputHosts, null, null);

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
        final int partitionId = 1;
        final String configuredConsumerPrefix = "consumerIdPrefix";
        final String configuredZkRoot = getRandomZkRootNode();
        final String expectedZkRoot = configuredZkRoot + "/" + configuredConsumerPrefix;
        final String expectedConsumerId = configuredConsumerPrefix + ":MyConsumerId";
        final String expectedZkConsumerStatePath = expectedZkRoot + "/consumers/" + expectedConsumerId + "/" + String.valueOf(partitionId);

        // Create our config
        final SpoutConfig spoutConfig = createDefaultConfig(getZkServer().getConnectString(), configuredZkRoot, configuredConsumerPrefix);

        // Create instance and open it.
        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();
        persistenceAdapter.open(spoutConfig);

        // Validate
        assertEquals("Unexpected zk root string", expectedZkRoot, persistenceAdapter.getZkRoot());

        // Validate that getZkXXXXStatePath returns the expected value
        assertEquals(
            "Unexpected zkConsumerStatePath returned",
            expectedZkConsumerStatePath, persistenceAdapter.getZkConsumerStatePathForPartition(expectedConsumerId, partitionId)
        );

        // Close everyone out
        persistenceAdapter.close();
    }

    /**
     * Does an end to end test of this persistence layer for storing/retrieving Consumer state.
     * 1 - Sets up an internal Zk server
     * 2 - Connects to it
     * 3 - writes state data to it
     * 4 - reads state data from it
     * 5 - compares that its valid.
     */
    @Test
    public void testEndToEndConsumerStatePersistence() throws InterruptedException {
        final String configuredConsumerPrefix = "consumerIdPrefix";
        final String configuredZkRoot = getRandomZkRootNode();

        final String consumerId = "myConsumer" + Clock.systemUTC().millis();
        final int partitionId1 = 1;
        final int partitionId2 = 2;

        // Create our config
        final SpoutConfig spoutConfig = createDefaultConfig(getZkServer().getConnectString(), configuredZkRoot, configuredConsumerPrefix);

        // Create instance and open it.
        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();
        persistenceAdapter.open(spoutConfig);

        final Long offset1 = 100L;

        persistenceAdapter.persistConsumerState(consumerId, partitionId1, offset1);

        final Long actual1 = persistenceAdapter.retrieveConsumerState(consumerId, partitionId1);

        // Validate result
        assertNotNull("Got an object back", actual1);
        assertEquals(offset1, actual1);

        // Close outs
        persistenceAdapter.close();

        // Create new instance, reconnect to ZK, make sure we can still read it out with our new instance.
        persistenceAdapter = new ZookeeperPersistenceAdapter();
        persistenceAdapter.open(spoutConfig);

        // Re-retrieve, should still be there.
        final Long actual2 = persistenceAdapter.retrieveConsumerState(consumerId, partitionId1);

        assertNotNull("Got an object back", actual2);
        assertEquals(offset1, actual2);

        final Long offset2 = 101L;

        // Update our existing state
        persistenceAdapter.persistConsumerState(consumerId, partitionId1, offset2);

        // Re-retrieve, should still be there.
        final Long actual3 = persistenceAdapter.retrieveConsumerState(consumerId, partitionId1);

        assertNotNull("Got an object back", actual3);
        assertEquals(offset2, actual3);

        final Long actual4 = persistenceAdapter.retrieveConsumerState(consumerId, partitionId2);

        assertNull("Partition hasn't been set yet", actual4);

        final Long offset3 = 102L;

        persistenceAdapter.persistConsumerState(consumerId, partitionId2, offset3);

        final Long actual5 = persistenceAdapter.retrieveConsumerState(consumerId, partitionId2);

        assertNotNull("Got an object back", actual5);
        assertEquals(offset3, actual5);

        // Re-retrieve, should still be there.
        final Long actual6 = persistenceAdapter.retrieveConsumerState(consumerId, partitionId1);

        assertNotNull("Got an object back", actual3);
        assertEquals(offset2, actual6);

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
    public void testEndToEndConsumerStatePersistenceWithValidationWithIndependentZkClient()
        throws IOException, KeeperException, InterruptedException {
        final String configuredConsumerPrefix = "consumerIdPrefix";
        final String configuredZkRoot = getRandomZkRootNode();

        // Define our ZK Root Node
        final String zkRootNodePath = configuredZkRoot + "/" + configuredConsumerPrefix;
        final String zkConsumersRootNodePath = zkRootNodePath + "/consumers";
        final String consumerId = "MyConsumer" + Clock.systemUTC().millis();
        final int partitionId = 1;

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
        final SpoutConfig spoutConfig = createDefaultConfig(getZkServer().getConnectString(), configuredZkRoot, configuredConsumerPrefix);
        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();
        persistenceAdapter.open(spoutConfig);

        // Define the offset we are storing
        final long offset = 100L;

        // Persist it
        logger.info("Persisting {}", offset);
        persistenceAdapter.persistConsumerState(consumerId, partitionId, offset);

        // Since this is an async operation, use await() to watch for the change
        await()
            .atMost(6, TimeUnit.SECONDS)
            .until(() -> zookeeperClient.exists(zkConsumersRootNodePath, false), notNullValue());

        // 4. Go into zookeeper and see where data got written
        doesNodeExist = zookeeperClient.exists(zkConsumersRootNodePath, false);
        logger.debug("Result {}", doesNodeExist);
        assertNotNull("Our root node should now exist", doesNodeExist);

        // Now attempt to read our state
        List<String> childrenNodes = zookeeperClient.getChildren(zkConsumersRootNodePath, false);
        logger.debug("Children Node Names {}", childrenNodes);

        // We should have a single child
        assertEquals("Should have a single filter", 1, childrenNodes.size());

        // Grab the child node node
        final String childNodeName = childrenNodes.get(0);
        assertNotNull("Child Node Name should not be null", childNodeName);
        assertEquals("Child Node name not correct", consumerId, childNodeName);

        // 5. Grab the value and validate it
        final byte[] storedDataBytes = zookeeperClient.getData(
            zkConsumersRootNodePath + "/" + consumerId + "/" + String.valueOf(partitionId),
            false,
            null
        );
        logger.debug("Stored data bytes {}", storedDataBytes);
        assertNotEquals("Stored bytes should be non-zero", 0, storedDataBytes.length);

        // Convert to a string
        final Long storedData = Long.valueOf(new String(storedDataBytes, Charsets.UTF_8));
        logger.info("Stored data {}", storedData);
        assertNotNull("Stored data should be non-null", storedData);
        assertEquals("Got unexpected state", offset, (long) storedData);

        // Test clearing state actually clears state.
        persistenceAdapter.clearConsumerState(consumerId, partitionId);

        // Validate the node no longer exists
        // Since this is an async operation, use await() to watch for the change
        await()
            .atMost(6, TimeUnit.SECONDS)
            .until(() -> zookeeperClient.exists(zkConsumersRootNodePath + "/" + consumerId + "/" + partitionId, false), nullValue());

        // Make sure the top level key no longer exists
        await()
            .atMost(6, TimeUnit.SECONDS)
            .until(() -> zookeeperClient.exists(zkConsumersRootNodePath + "/" + consumerId , false), nullValue());

        // Close everyone out
        persistenceAdapter.close();
        zookeeperClient.close();
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
    public void testEndToEndConsumerStatePersistenceMultipleValuesWithValidationWithIndependentZkClient()
        throws IOException, KeeperException, InterruptedException {
        final String configuredConsumerPrefix = "consumerIdPrefix";
        final String configuredZkRoot = getRandomZkRootNode();

        // Define our ZK Root Node
        final String zkRootNodePath = configuredZkRoot + "/" + configuredConsumerPrefix;
        final String zkConsumersRootNodePath = zkRootNodePath + "/consumers";
        final String virtualSpoutId = "MyConsumer" + Clock.systemUTC().millis();
        final String zkVirtualSpoutIdNodePath = zkConsumersRootNodePath + "/" + virtualSpoutId;

        // Define partitionIds
        final int partition0 = 0;
        final int partition1 = 1;
        final int partition2 = 2;

        // Define the offset we are storing
        final long partition0Offset = 100L;
        final long partition1Offset = 200L;
        final long partition2Offset = 300L;

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
        final SpoutConfig spoutConfig = createDefaultConfig(getZkServer().getConnectString(), configuredZkRoot, configuredConsumerPrefix);
        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();
        persistenceAdapter.open(spoutConfig);

        // Persist it
        persistenceAdapter.persistConsumerState(virtualSpoutId, partition0, partition0Offset);
        persistenceAdapter.persistConsumerState(virtualSpoutId, partition1, partition1Offset);
        persistenceAdapter.persistConsumerState(virtualSpoutId, partition2, partition2Offset);

        // Since this is an async operation, use await() to watch for the change
        await()
            .atMost(6, TimeUnit.SECONDS)
            .until(() -> zookeeperClient.exists(zkConsumersRootNodePath, false), notNullValue());

        // 4. Go into zookeeper and see where data got written
        doesNodeExist = zookeeperClient.exists(zkConsumersRootNodePath, false);
        logger.debug("Result {}", doesNodeExist);
        assertNotNull("Our root node should now exist", doesNodeExist);

        // Now attempt to read our state
        List<String> virtualSpoutIdNodes = zookeeperClient.getChildren(zkConsumersRootNodePath, false);
        logger.debug("VirtualSpoutId Node Names {}", virtualSpoutIdNodes);

        // We should have a single child
        assertEquals("Should have a single VirtualSpoutId", 1, virtualSpoutIdNodes.size());

        // Grab the virtualSpoutId
        final String foundVirtualSpoutIdNode = virtualSpoutIdNodes.get(0);
        assertNotNull("foundVirtualSpoutIdNode entry should not be null", foundVirtualSpoutIdNode);
        assertEquals("foundVirtualSpoutIdNode name not correct", virtualSpoutId, foundVirtualSpoutIdNode);

        // Grab entries under that virtualSpoutId
        List<String> partitionIdNodes = zookeeperClient.getChildren(zkVirtualSpoutIdNodePath, false);

        // We should have a 3 children
        logger.info("PartitionId nodes: {}", partitionIdNodes);
        assertEquals("Should have 3 partitions", 3, partitionIdNodes.size());
        assertTrue("Should contain partition 0", partitionIdNodes.contains(String.valueOf(partition0)));
        assertTrue("Should contain partition 1", partitionIdNodes.contains(String.valueOf(partition1)));
        assertTrue("Should contain partition 2", partitionIdNodes.contains(String.valueOf(partition2)));

        // Grab each partition and validate it
        byte[] storedDataBytes = zookeeperClient.getData(zkVirtualSpoutIdNodePath + "/" + partition0, false, null);
        logger.debug("Stored data bytes {}", storedDataBytes);
        assertNotEquals("Stored bytes should be non-zero", 0, storedDataBytes.length);

        // Convert to a string
        Long storedData = Long.valueOf(new String(storedDataBytes, Charsets.UTF_8));
        logger.info("Stored data {}", storedData);
        assertNotNull("Stored data should be non-null", storedData);
        assertEquals("Got unexpected state", partition0Offset, (long) storedData);

        // Now remove partition0 from persistence
        persistenceAdapter.clearConsumerState(virtualSpoutId, partition0);

        // Validate the node no longer exists
        // Since this is an async operation, use await() to watch for the change
        await()
            .atMost(6, TimeUnit.SECONDS)
            .until(() -> zookeeperClient.exists(zkVirtualSpoutIdNodePath + "/" + partition0, false), nullValue());

        // Validation partition1
        storedDataBytes = zookeeperClient.getData(zkVirtualSpoutIdNodePath + "/" + partition1, false, null);
        logger.debug("Stored data bytes {}", storedDataBytes);
        assertNotEquals("Stored bytes should be non-zero", 0, storedDataBytes.length);

        // Convert to a string
        storedData = Long.valueOf(new String(storedDataBytes, Charsets.UTF_8));
        logger.info("Stored data {}", storedData);
        assertNotNull("Stored data should be non-null", storedData);
        assertEquals("Got unexpected state", partition1Offset, (long) storedData);

        // Now remove partition1 from persistence
        persistenceAdapter.clearConsumerState(virtualSpoutId, partition1);

        // Validate the node no longer exists
        // Since this is an async operation, use await() to watch for the change
        await()
            .atMost(6, TimeUnit.SECONDS)
            .until(() -> zookeeperClient.exists(zkVirtualSpoutIdNodePath + "/" + partition1, false), nullValue());

        // Validation partition2
        storedDataBytes = zookeeperClient.getData(zkVirtualSpoutIdNodePath + "/" + partition2, false, null);
        logger.debug("Stored data bytes {}", storedDataBytes);
        assertNotEquals("Stored bytes should be non-zero", 0, storedDataBytes.length);

        // Convert to a string
        storedData = Long.valueOf(new String(storedDataBytes, Charsets.UTF_8));
        logger.info("Stored data {}", storedData);
        assertNotNull("Stored data should be non-null", storedData);
        assertEquals("Got unexpected state", partition2Offset, (long) storedData);

        // Now remove partition1 from persistence
        persistenceAdapter.clearConsumerState(virtualSpoutId, partition2);

        // Validate the node no longer exists
        // Since this is an async operation, use await() to watch for the change
        await()
            .atMost(6, TimeUnit.SECONDS)
            .until(() -> zookeeperClient.exists(zkVirtualSpoutIdNodePath + "/" + partition1, false), nullValue());

        // Make sure the top level key no longer exists
        await()
            .atMost(6, TimeUnit.SECONDS)
            .until(() -> zookeeperClient.exists(zkConsumersRootNodePath + "/" + virtualSpoutId , false), nullValue());

        // Close everyone out
        persistenceAdapter.close();
        zookeeperClient.close();
    }

    @Rule
    public ExpectedException expectedExceptionPersistConsumerStateBeforeBeingOpened = ExpectedException.none();

    /**
     * Verify we get an exception if you try to persist before calling open().
     */
    @Test
    public void testPersistConsumerStateBeforeBeingOpened() {
        final int partitionId = 1;

        // Create our instance
        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();

        // Call method and watch for exception
        expectedExceptionPersistConsumerStateBeforeBeingOpened.expect(IllegalStateException.class);
        persistenceAdapter.persistConsumerState("MyConsumerId", partitionId, 100L);
    }

    @Rule
    public ExpectedException expectedExceptionRetrieveConsumerStateBeforeBeingOpened = ExpectedException.none();

    /**
     * Verify we get an exception if you try to retrieve before calling open().
     */
    @Test
    public void testRetrieveConsumerStateBeforeBeingOpened() {
        final int partitionId = 1;

        // Create our instance
        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();

        // Call method and watch for exception
        expectedExceptionRetrieveConsumerStateBeforeBeingOpened.expect(IllegalStateException.class);
        persistenceAdapter.retrieveConsumerState("MyConsumerId", partitionId);
    }

    @Rule
    public ExpectedException expectedExceptionClearConsumerStateBeforeBeingOpened = ExpectedException.none();

    /**
     * Verify we get an exception if you try to persist before calling open().
     */
    @Test
    public void testClearConsumerStateBeforeBeingOpened() {
        final int partitionId = 1;

        // Create our instance
        ZookeeperPersistenceAdapter persistenceAdapter = new ZookeeperPersistenceAdapter();

        // Call method and watch for exception
        expectedExceptionClearConsumerStateBeforeBeingOpened.expect(IllegalStateException.class);
        persistenceAdapter.clearConsumerState("MyConsumerId", partitionId);
    }

    /**
     * Helper method.
     */
    private SpoutConfig createDefaultConfig(List<String> zkServers, String zkRootNode, String consumerIdPrefix) {
        Map<String, Object> config = new HashMap<>();
        config.put(SpoutConfig.PERSISTENCE_ZK_SERVERS, zkServers);
        config.put(SpoutConfig.PERSISTENCE_ZK_ROOT, zkRootNode);
        config.put(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX, consumerIdPrefix);

        return new SpoutConfig(config);
    }

    /**
     * Helper method.
     */
    private SpoutConfig createDefaultConfig(String zkServers, String zkRootNode, String consumerIdPrefix) {
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