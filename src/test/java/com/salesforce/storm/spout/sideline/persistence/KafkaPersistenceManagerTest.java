package com.salesforce.storm.spout.sideline.persistence;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.kafka.ConsumerState;
import com.salesforce.storm.spout.sideline.kafka.KafkaTestServer;
import com.salesforce.storm.spout.sideline.kafka.SidelineConsumerTest;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class KafkaPersistenceManagerTest {

    // Our internal Kafka and Zookeeper Server, used to test against.
    private KafkaTestServer kafkaTestServer;

    // Gets set to our randomly generated topic created for the test.
    private String topicName;

    private String consumerIdPrefix = "TestConsumerId";

    /**
     * Here we stand up an internal test kafka and zookeeper service.
     */
    @Before
    public void setup() throws Exception {
        // ensure we're in a clean state
        tearDown();

        // Setup kafka test server
        kafkaTestServer = new KafkaTestServer();
        kafkaTestServer.start();

        // Generate topic name
        topicName = SidelineConsumerTest.class.getSimpleName() + Clock.systemUTC().millis();

        // Create topic with 3 partitions
        kafkaTestServer.createTopic(topicName, 3);
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
        persistenceManager.persistConsumerState(consumerIdPrefix, consumerState);

        // Now attempt to retrieve it
        final ConsumerState results = persistenceManager.retrieveConsumerState(consumerIdPrefix);

        // Validate it
        assertNotNull("Should be non-null", results);
        assertNotNull("should be non-null", results.getTopicPartitions());
        assertEquals("Should have 3 entries", 3, results.size());
        assertEquals("Should have 3 entries", 3, results.getTopicPartitions().size());
        assertEquals("Should have correct value", 100L, (long) results.getOffsetForTopicAndPartition(new TopicPartition(topicName, 0)));
        assertEquals("Should have correct value", 200L, (long) results.getOffsetForTopicAndPartition(new TopicPartition(topicName, 1)));
        assertEquals("Should have correct value", 300L, (long) results.getOffsetForTopicAndPartition(new TopicPartition(topicName, 2)));
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