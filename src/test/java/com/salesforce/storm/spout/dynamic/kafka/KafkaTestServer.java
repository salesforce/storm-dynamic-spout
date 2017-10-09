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

package com.salesforce.storm.spout.dynamic.kafka;

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
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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

    private static final Logger logger = LoggerFactory.getLogger(KafkaTestServer.class);
    private TestingServer zkServer;
    private KafkaServerStartable kafka;

    public TestingServer getZkServer() {
        return this.zkServer;
    }

    public KafkaServerStartable getKafkaServer() {
        return this.kafka;
    }

    /**
     * Creates and starts ZooKeeper and Kafka server instances.
     * @throws Exception Something went wrong.
     */
    public void start() throws Exception {
        InstanceSpec zkInstanceSpec = new InstanceSpec(null, 21811, -1, -1, true, -1, -1, 1000);
        zkServer = new TestingServer(zkInstanceSpec, true);
        String connectionString = getZkServer().getConnectString();

        File logDir = new File("/tmp/kafka-logs-" + Double.toHexString(Math.random()));
        logDir.deleteOnExit();
        String kafkaPort = String.valueOf(InstanceSpec.getRandomPort());

        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", connectionString);
        properties.setProperty("port", kafkaPort);
        properties.setProperty("log.dir", logDir.getAbsolutePath());
        properties.setProperty("host.name", "127.0.0.1");
        properties.setProperty("advertised.host.name", "127.0.0.1");
        properties.setProperty("advertised.port", kafkaPort);
        properties.setProperty("auto.create.topics.enable", "true");
        properties.setProperty("zookeeper.session.timeout.ms", "30000");
        properties.setProperty("broker.id", "1");

        KafkaConfig config = new KafkaConfig(properties);
        kafka = new KafkaServerStartable(config);
        getKafkaServer().startup();
    }

    /**
     * Creates a namespace in Kafka. If the namespace already exists this does nothing.
     * Will create a namespace with exactly 1 partition.
     * @param topicName - the namespace name to create.
     */
    public void createTopic(String topicName) {
        createTopic(topicName, 1);
    }

    /**
     * Creates a namespace in Kafka. If the namespace already exists this does nothing.
     * @param topicName - the namespace name to create.
     * @param partitions - the number of partitions to create.
     */
    public void createTopic(final String topicName, final int partitions) {
        int sessionTimeoutInMs = 10000;
        int connectionTimeoutInMs = 10000;

        /**
         * Note: You must initialize the ZkClient with ZKStringSerializer. If you don't then createTopic()
         * will only seem to work (it will return without error). The namespace will exist only in ZooKeeper
         * and will be returned when listing topics, but Kafka itself does not create the namespace.
         */
        ZkClient zkClient = new ZkClient(
            getZkServer().getConnectString(),
            sessionTimeoutInMs,
            connectionTimeoutInMs,
            ZKStringSerializer$.MODULE$
        );
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);
        if (!AdminUtils.topicExists(zkUtils, topicName)) {
            int replicationFactor = 1;
            AdminUtils.createTopic(zkUtils, topicName, partitions, replicationFactor, new Properties(), new RackAwareMode.Disabled$());
        }

        // Close connection
        zkClient.close();
    }

    /**
     * Shuts down the ZooKeeper and Kafka server instances. This *must* be called before the integration
     * test completes in order to clean up any running processes and data that was created.
     * @throws Exception Something went wrong.
     */
    public void shutdown() throws Exception {
        close();
    }

    /**
     * Creates a kafka producer that is connected to our test server.
     */
    public KafkaProducer getKafkaProducer() {
        return getKafkaProducer(
            StringSerializer.class.getName(),
            ByteArraySerializer.class.getName()
        );
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

        // Start from the head of the namespace by default
        consumerProperties.put("auto.offset.reset", "smallest");

        // Don't commit offsets anywhere for our consumerId
        consumerProperties.put("auto.commit.enable", "false");
        logger.info("Consumer properties {}", consumerProperties);
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));
    }

    /**
     * Depending on your version of Kafka, this may or may not be implemented.
     * 0.8.2.2 this does NOT work!.
     * @return Kafka consumer instance.
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
