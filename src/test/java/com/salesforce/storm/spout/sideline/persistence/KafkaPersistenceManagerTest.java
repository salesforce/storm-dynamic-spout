package com.salesforce.storm.spout.sideline.persistence;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.kafka.ConsumerState;
import com.salesforce.storm.spout.sideline.kafka.KafkaTestServer;
import com.salesforce.storm.spout.sideline.kafka.SidelineConsumerTest;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Clock;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class KafkaPersistenceManagerTest {

    // Our internal Kafka and Zookeeper Server, used to test against.
    private static KafkaTestServer kafkaTestServer;

    // Gets set to our randomly generated topic created for the test.
    private String topicName;

    private String consumerIdPrefix = "TestConsumerId";

    /**
     * Here we stand up an internal test kafka and zookeeper service.
     * Once for all methods in this class.
     */
    @BeforeClass
    public static void setupKafkaServer() throws Exception {
        // Setup kafka test server
        kafkaTestServer = new KafkaTestServer();
        kafkaTestServer.start();
    }

    /**
     * This happens once before every test method.
     * Create a new empty topic with randomly generated name.
     */
    @Before
    public void setup() throws Exception {
        // Generate topic name
        topicName = SidelineConsumerTest.class.getSimpleName() + Clock.systemUTC().millis();

        // Create topic with 3 partitions
        kafkaTestServer.createTopic(topicName, 3);
    }

    /**
     * Here we shut down the internal test kafka and zookeeper services.
     */
    @AfterClass
    public static void destroyKafkaServer() {
        // Close out kafka test server if needed
        if (kafkaTestServer == null) {
            return;
        }
        try {
            kafkaTestServer.shutdown();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        kafkaTestServer = null;
    }

    /**
     * Here we shut down the internal test kafka and zookeeper services.
     */
    @After
    public void tearDown() {
        // Close out kafka test server if needed
        if (kafkaTestServer == null) {
            return;
        }
        try {
            kafkaTestServer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        kafkaTestServer = null;
    }

    @Test
    public void doSimpleEndToEndTest() throws InterruptedException {
        // Create our config
        final Map topologyConfig = getDefaultConfig(consumerIdPrefix);

        KafkaPersistenceManager persistenceManager = new KafkaPersistenceManager();
        persistenceManager.open(topologyConfig);

        // Create state
        final ConsumerState consumerState = ConsumerState
            .builder()
            .withPartition(new TopicPartition(topicName, 0), 100L)
            .withPartition(new TopicPartition(topicName, 1), 200L)
            .withPartition(new TopicPartition(topicName, 2), 300L)
            .build();

        // Persist it
        // TODO: Fix partition id
        //persistenceManager.persistConsumerState(consumerIdPrefix, 1, consumerState);

        // Now attempt to retrieve it
        // TODO: Fix partition id
        final Long result = persistenceManager.retrieveConsumerState(consumerIdPrefix, 1);

        // TODO: Validate it
    }

    private Map<String, Object> getDefaultConfig(final String consumerIdPrefix) {
        final Map<String, Object> config = Maps.newHashMap();
        config.put(SidelineSpoutConfig.KAFKA_TOPIC, topicName);
        config.put(SidelineSpoutConfig.CONSUMER_ID_PREFIX, consumerIdPrefix);
        config.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort()));
        config.put(SidelineSpoutConfig.PERSISTENCE_MANAGER_CLASS, "com.salesforce.storm.spout.sideline.persistence.KafkaPersistenceManager");

        return config;
    }
}