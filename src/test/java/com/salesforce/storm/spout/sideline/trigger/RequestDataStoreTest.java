package com.salesforce.storm.spout.sideline.trigger;

import com.salesforce.storm.spout.sideline.kafka.ZookeeperConsumerStateManagerTest;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RequestDataStoreTest {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperConsumerStateManagerTest.class);
    private TestingServer zkServer;

    @Before
    public void setup() throws Exception {
        InstanceSpec zkInstanceSpec = new InstanceSpec(null, -1, -1, -1, true, -1, -1, 1000);
        zkServer = new TestingServer(zkInstanceSpec, true);
    }

    @After
    public void shutdown() throws Exception {
        zkServer.stop();
        zkServer.close();
    }

    /**
     * Test some shiz.
     */
//    @Test
//    public void doTest() throws InterruptedException {
//        final String zkRootPath = "/poop";
//
//        // Create our instance
//        logger.info("ZK Connection String {}", zkServer.getConnectString());
//        RequestDataStore dataStore = new RequestDataStore(Lists.newArrayList(zkServer.getConnectString()), zkRootPath);
//        dataStore.init();
//
//        // Create Start Request
//        final String topic = "MyTopic";
//        final long timeStamp = DateTime.now().getMillis();
//        final StartRequest startRequest = new StartRequest(timeStamp, topic, "payload");
//
//        // Create ConsumerState
//        final ConsumerState consumerState = new ConsumerState();
//        consumerState.setOffset(new TopicPartition(topic, 0), 0L);
//        consumerState.setOffset(new TopicPartition(topic, 1), 1000L);
//        consumerState.setOffset(new TopicPartition(topic, 2), 2000L);
//        consumerState.setOffset(new TopicPartition(topic, 3), 3000L);
//
//        // Write it
//        dataStore.writeStartRequest(startRequest, consumerState);
//
//        Thread.sleep(60000L);
//    }
}