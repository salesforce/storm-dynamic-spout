package com.salesforce.storm.spout.sideline.persistence;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.kafka.KafkaTestServer;
import com.salesforce.storm.spout.sideline.kafka.SidelineConsumerTest;
import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.shade.com.google.common.base.Charsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 *
 */
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

        // Create topic
        kafkaTestServer.createTopic(topicName);
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
    public void doTest() throws InterruptedException {
        // Create our config
        final Map topologyConfig = getDefaultConfig(consumerIdPrefix);

        KafkaPersistenceManager persistenceManager = new KafkaPersistenceManager();
        persistenceManager.open(topologyConfig);

        // Create state
        final ConsumerState consumerState = new ConsumerState();
        consumerState.setOffset(new TopicPartition(topicName, 0), 100L);

        // Persist it
        //logger.info("Persisting {}", consumerState);
        persistenceManager.persistConsumerState(consumerIdPrefix, consumerState);
        persistenceManager.retrieveConsumerState(consumerIdPrefix);
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