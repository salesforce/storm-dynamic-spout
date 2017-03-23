package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Maps;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Properties;

/**
 * This will spin up a ZooKeeper and Kafka server for use in integration tests. Simply
 * create an instance of KafkaTestServer and call start() and you can publish to Kafka
 * topics in an integration test. Be sure to call shutdown() when the test is complete
 * or use the AutoCloseable interface.
 */
public class KafkaTestServer implements AutoCloseable {


    /**
     * (String[]) Array of zookeeper servers supporting Kafka.  In format of "server.hostname:port"
     * Example: [zkkafka1-1-dfw:2181, zkkafka1-2-dfw:2181, ...]
     */
    public static final String KAFKA_ZOOKEEPER = "environment.kafka.zookeeper";

    /**
     * (String[]) Array of zookeeper servers supporting Storm.  In format of "server.hostname:port"
     * Example: [zkstorm1-1-dfw:2181, zkstorm1-2-dfw:2181, ...]
     */
    public static final String STORM_ZOOKEEPER = "environment.storm.zookeeper";


    private static final Logger logger = LoggerFactory.getLogger(KafkaTestServer.class);
    private TestingServer zkServer;
    private KafkaServerStartable kafka;
    private CuratorFramework cli;

    public TestingServer getZkServer() {
        return this.zkServer;
    }

    public KafkaServerStartable getKafkaServer() {
        return this.kafka;
    }

    public CuratorFramework getCli() {
        return this.cli;
    }

    /**
     * Creates and starts ZooKeeper and Kafka server instances.
     * @throws Exception
     */
    public void start() throws Exception {
        InstanceSpec zkInstanceSpec = new InstanceSpec(null, 21811, -1, -1, true, -1, -1, 1000);
        zkServer = new TestingServer(zkInstanceSpec, true);
        String connectionString = getZkServer().getConnectString();

        File logDir = new File("/tmp/kafka-logs-" + Double.toHexString(Math.random()));
        String kafkaPort = String.valueOf(InstanceSpec.getRandomPort());

        Properties p = new Properties();
        p.setProperty("zookeeper.connect", connectionString);
        p.setProperty("port", kafkaPort);
        p.setProperty("log.dir", logDir.getAbsolutePath());
        p.setProperty("host.name", "127.0.0.1");
        p.setProperty("advertised.host.name", "127.0.0.1");
        p.setProperty("advertised.port", kafkaPort);
        p.setProperty("auto.create.topics.enable", "true");
        p.setProperty("zookeeper.session.timeout.ms", "30000");
        p.setProperty("broker.id", "1");

        KafkaConfig config = new KafkaConfig(p);
        kafka = new KafkaServerStartable(config);
        getKafkaServer().startup();

        cli = CuratorFrameworkFactory.newClient(getZkServer().getConnectString(), new RetryOneTime(2000));
    }

    /**
     * Creates a topic in Kafka. If the topic already exists this does nothing.
     * Will create a topic with exactly 1 partition.
     * @param topicName - the topic name to create.
     */
    public void createTopic(String topicName) {
        createTopic(topicName, 1);
    }

    /**
     * Creates a topic in Kafka. If the topic already exists this does nothing.
     * @param topicName - the topic name to create.
     * @param partitions - the number of partitions to create.
     */
    public void createTopic(final String topicName, final int partitions) {
        int sessionTimeoutInMs = 10000;
        int connectionTimeoutInMs = 10000;

        /**
         * Note: You must initialize the ZkClient with ZKStringSerializer. If you don't then createTopic()
         * will only seem to work (it will return without error). The topic will exist only in ZooKeeper
         * and will be returned when listing topics, but Kafka itself does not create the topic.
         */
        ZkClient zkClient = new ZkClient(getZkServer().getConnectString(), sessionTimeoutInMs, connectionTimeoutInMs, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);
        if (!AdminUtils.topicExists(zkUtils, topicName)) {
            int replicationFactor = 1;
            AdminUtils.createTopic(zkUtils, topicName, partitions, replicationFactor, new Properties(), new RackAwareMode.Disabled$());
        }
    }

    /**
     * Shuts down the ZooKeeper and Kafka server instances. This *must* be called before the integration
     * test completes in order to clean up any running processes and data that was created.
     * @throws Exception
     */
    public void shutdown() throws Exception {
        close();
    }

    /**
     * Creates a kafka producer that is connected to our test server.
     */
    public KafkaProducer getKafkaProducer() {
        return getKafkaProducer("org.apache.kafka.common.serialization.StringSerializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    }

    /**
     * Creates a kafka producer that is connected to our test server.
     */
    public KafkaProducer getKafkaProducer(String keySerializer, String valueSerializer) {
        // Create producer
        Map<String, Object> kafkaProducerConfig = Maps.newHashMap();
        kafkaProducerConfig.put("bootstrap.servers", "127.0.0.1:" + getKafkaServer().serverConfig().advertisedPort());
        kafkaProducerConfig.put("key.serializer", keySerializer);
        kafkaProducerConfig.put("value.serializer", valueSerializer);
        kafkaProducerConfig.put("batch.size", 0);

        // Return our producer
        return new KafkaProducer(kafkaProducerConfig);
    }

    /**
     * Old 0.8.2.x consumer.
     */
    public ConsumerConnector getKafkaConsumerConnector() {
        Properties consumerProperties = new Properties();
        consumerProperties.put("zookeeper.connect", getZkServer().getConnectString());
        consumerProperties.put("group.id", "test-group");

        // Start from the head of the topic by default
        consumerProperties.put("auto.offset.reset", "smallest");

        // Don't commit offsets anywhere for our consumerId
        consumerProperties.put("auto.commit.enable", "false");
        logger.info("Consumer properties {}", consumerProperties);
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));
    }

    /**
     * Depending on your version of Kafka, this may or may not be implemented.
     * 0.8.2.2 this does NOT work!.
     * @return
     */
    public KafkaConsumer getKafkaConsumer() {
        Map<String, Object> kafkaConsumerConfig = Maps.newHashMap();
        kafkaConsumerConfig.put("bootstrap.servers", "127.0.0.1:" + getKafkaServer().serverConfig().advertisedPort());
        kafkaConsumerConfig.put("group.id", "test-consumer-id");
        kafkaConsumerConfig.put("key.deserializer", StringDeserializer.class.getName());
        kafkaConsumerConfig.put("value.deserializer", ByteArrayDeserializer.class.getName());
        kafkaConsumerConfig.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor");

        return new KafkaConsumer(kafkaConsumerConfig);
    }

    @Override
    public void close() throws Exception {
        if (getCli() != null) {
            getCli().close();
            cli = null;
        }
        if (getKafkaServer() != null) {
            getKafkaServer().shutdown();
            kafka = null;
        }
        if (getZkServer() != null) {
            getZkServer().close();
            zkServer = null;
        }
    }
}
