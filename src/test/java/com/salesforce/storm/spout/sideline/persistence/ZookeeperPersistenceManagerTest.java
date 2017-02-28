package com.salesforce.storm.spout.sideline.persistence;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.shade.org.joda.time.DateTime;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.*;

/**
 * Tests our Zookeeper Persistence layer.
 */
public class ZookeeperPersistenceManagerTest {
    // For logging within test.
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperPersistenceManagerTest.class);

    // An internal zookeeper server used for testing.
    private TestingServer zkServer;

    /**
     * Before running any tests, we stand up an internal zookeeper server we test against.
     */
    @Before
    public void setup() throws Exception {
        InstanceSpec zkInstanceSpec = new InstanceSpec(null, -1, -1, -1, true, -1, -1, 1000);
        zkServer = new TestingServer(zkInstanceSpec, true);
    }

    /**
     * After running any tests, we shut down the internal zookeeper server instance.
     */
    @After
    public void shutdown() throws Exception {
        zkServer.stop();
        zkServer.close();
    }

    /**
     * Tests that the constructor does what we think.
     */
    @Test
    public void testConstructor() {
        final String expectedZkConnectionString = "localhost:2181,localhost2:2183";
        final String expectedZkRoot = "/myRoot";
        final String expectedConsumerId = "MyConsumerId";

        ZookeeperPersistenceManager persistenceManager = new ZookeeperPersistenceManager(expectedZkConnectionString, expectedZkRoot);
        assertEquals("Unexpected zk connection string", expectedZkConnectionString, persistenceManager.getZkConnectionString());
        assertEquals("Unexpected zk root string", expectedZkRoot, persistenceManager.getZkRoot());

        // Validate that getZkStatePath returns the expected value
        assertEquals("Unexpected zkStatePath returned", expectedZkRoot + "/" + expectedConsumerId, persistenceManager.getZkStatePath(expectedConsumerId));
    }

    /**
     * Tests that the constructor does what we think.
     */
    @Test
    public void testListConstructor() {
        final String expectedZkConnectionString = "localhost:2181,localhost2:2183";
        final List<String> inputHosts = Lists.newArrayList("localhost:2181", "localhost2:2183");
        final String expectedZkRoot = "/myRoot";
        final String expectedConsumerId = "PoopyId";

        ZookeeperPersistenceManager persistenceManager = new ZookeeperPersistenceManager(inputHosts, expectedZkRoot);
        assertEquals("Unexpected zk connection string", expectedZkConnectionString, persistenceManager.getZkConnectionString());
        assertEquals("Unexpected zk root string", expectedZkRoot, persistenceManager.getZkRoot());

        // Validate that getZkStatePath returns the expected value
        assertEquals("Unexpected zkStatePath returned", expectedZkRoot + "/" + expectedConsumerId, persistenceManager.getZkStatePath(expectedConsumerId));
    }

    /**
     * Does an end to end test of this persistence layer.
     * 1 - Sets up an internal Zk server
     * 2 - Connects to it
     * 3 - writes state data to it
     * 4 - reads state data from it
     * 5 - compares that its valid.
     */
    @Test
    public void testEndToEndStatePersistence() throws InterruptedException {
        final String zkRootPath = "/poop";
        final String consumerId = "myConsumer" + DateTime.now().getMillis();

        // Create our instance
        ZookeeperPersistenceManager persistenceManager = new ZookeeperPersistenceManager(zkServer.getConnectString(), zkRootPath);
        persistenceManager.init();

        // Create state
        final ConsumerState consumerState = new ConsumerState();
        consumerState.setOffset(new TopicPartition("MyTopic", 0), 0L);
        consumerState.setOffset(new TopicPartition("MyTopic", 1), 100L);
        consumerState.setOffset(new TopicPartition("MyTopic", 3), 300L);

        // Persist it
        logger.info("Persisting {}", consumerState);
        persistenceManager.persistConsumerState(consumerId, consumerState);

        // Attempt to read it?
        final ConsumerState result = persistenceManager.retrieveConsumerState(consumerId);
        logger.info("Result {}", result);

        // Validate result
        assertNotNull("Got an object back", result);

        // Should have 3 entries
        assertEquals("Should have 3 entries", 3, result.getState().size());
        assertTrue("Contains Partition 0", result.getState().containsKey(new TopicPartition("MyTopic", 0)));
        assertEquals("Contains Partition 0 with value 0L", 0L, (long) result.getState().get(new TopicPartition("MyTopic", 0)));
        assertTrue("Contains Partition 1", result.getState().containsKey(new TopicPartition("MyTopic", 1)));
        assertEquals("Contains Partition 1 with value 100L", 100L, (long) result.getState().get(new TopicPartition("MyTopic", 1)));
        assertTrue("Contains Partition 3", result.getState().containsKey(new TopicPartition("MyTopic", 3)));
        assertEquals("Contains Partition 3 with value 300L", 300L, (long) result.getState().get(new TopicPartition("MyTopic", 3)));

        // Close outs
        persistenceManager.close();
    }

    /**
     * Tests that the zkRootNode constructor parameter works as we expect.
     * We do the following:
     * 1 - Connect to ZK and ensure that the zkRootNode path does NOT exist in Zookeeper yet
     *     If it does, we'll clean it up.
     * 2 - Create an instance of our state manager passing an expected root node
     * 3 - Attempt to persist some state
     * 4 - Go into zookeeper directly and verify the state got written under the appropriate prefix path (zkRootNode).
     * 5 - Read the stored value directly out of zookeeper and verify the right thing got written.
     */
    @Test
    public void testEndToEndStatePersistenceWithValidationWithIndependentZkClient() throws IOException, KeeperException, InterruptedException {
        // Define our ZK Root Node
        final String zkRootNodePath = "/TestRootPath";
        final String consumerId = "MyConsumer" + DateTime.now().getMillis();

        // 1 - Connect to ZK directly
        ZooKeeper zookeeperClient = new ZooKeeper(zkServer.getConnectString(), 6000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                logger.info("Got event {}", event);
            }
        });

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

        // 2. Create our instance
        ZookeeperPersistenceManager persistenceManager = new ZookeeperPersistenceManager(zkServer.getConnectString(), zkRootNodePath);
        persistenceManager.init();

        // 3. Attempt to persist some state.
        final String topicName = "MyTopic";

        // Define our expected result that will be stored in zookeeper
        final String expectedStoredState = "{\""+topicName+"-0\":0,\""+topicName+"-1\":100,\""+topicName+"-3\":300}";

        final ConsumerState consumerState = new ConsumerState();
        consumerState.setOffset(new TopicPartition(topicName, 0), 0L);
        consumerState.setOffset(new TopicPartition(topicName, 1), 100L);
        consumerState.setOffset(new TopicPartition(topicName, 3), 300L);

        // Persist it
        logger.info("Persisting {}", consumerState);
        persistenceManager.persistConsumerState(consumerId, consumerState);

        // Since this is an async operation, use await() to watch for the change
        await()
                .atMost(6, TimeUnit.SECONDS)
                .until(() -> {
                    return zookeeperClient.exists(zkRootNodePath, false);
                }, notNullValue());

        // 4. Go into zookeeper and see where data got written
        doesNodeExist = zookeeperClient.exists(zkRootNodePath, false);
        logger.debug("Result {}", doesNodeExist);
        assertNotNull("Our root node should now exist", doesNodeExist);

        // Now attempt to read our state
        List<String> childrenNodes = zookeeperClient.getChildren(zkRootNodePath, false);
        logger.debug("Children Node Names {}", childrenNodes);

        // We should have a single child
        assertEquals("Should have a single filter", 1, childrenNodes.size());

        // Grab the child node node
        final String childNodeName = childrenNodes.get(0);
        assertNotNull("Child Node Name shouldnt be null", childNodeName);
        assertEquals("Child Node name not correct", consumerId, childNodeName);

        // 5. Grab the value and validate it
        final byte[] storedDataBytes = zookeeperClient.getData(zkRootNodePath + "/" + consumerId, false, null);
        logger.debug("Stored data bytes {}", storedDataBytes);
        assertNotEquals("Stored bytes should be non-zero", 0, storedDataBytes.length);

        // Convert to a string
        final String storedDataStr = new String(storedDataBytes, Charsets.UTF_8);
        logger.info("Stored data string {}", storedDataStr);
        assertNotNull("Stored data string should be non-null", storedDataStr);
        assertEquals("Got unexpected state", expectedStoredState, storedDataStr);
    }
}