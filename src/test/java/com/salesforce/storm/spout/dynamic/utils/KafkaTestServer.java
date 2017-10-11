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

package com.salesforce.storm.spout.dynamic.utils;

import com.google.common.collect.Maps;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

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

    private TestingServer zkServer;
    private KafkaServerStartable kafka;

    /**
     * @return Internal Zookeeper Server.
     */
    public TestingServer getZookeeperServer() {
        return this.zkServer;
    }

    /**
     * @return Internal Kafka Server.
     */
    public KafkaServerStartable getKafkaServer() {
        return this.kafka;
    }

    /**
     * @return The proper connect string to use for Kafka.
     */
    public String getKafkaConnectString() {
        return "127.0.0.1:" + getKafkaServer().serverConfig().advertisedPort();
    }

    /**
     * @return The proper connect string to use for Zookeeper.
     */
    public String getZookeeperConnectString() {
        return "127.0.0.1:" + getZookeeperServer().getPort();
    }

    /**
     * Creates and starts ZooKeeper and Kafka server instances.
     */
    public void start() throws Exception {
        // Start zookeeper
        final InstanceSpec zkInstanceSpec = new InstanceSpec(null, -1, -1, -1, true, -1, -1, 1000);
        zkServer = new TestingServer(zkInstanceSpec, true);
        final String zkConnectionString = getZookeeperServer().getConnectString();

        // Create temp path to store logs
        final File logDir = new File("/tmp/kafka-logs-" + Double.toHexString(Math.random()));
        logDir.deleteOnExit();

        // Determine what port to run kafka on
        final String kafkaPort = String.valueOf(InstanceSpec.getRandomPort());

        // Build properties
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("zookeeper.connect", zkConnectionString);
        kafkaProperties.setProperty("port", kafkaPort);
        kafkaProperties.setProperty("log.dir", logDir.getAbsolutePath());
        kafkaProperties.setProperty("host.name", "127.0.0.1");
        kafkaProperties.setProperty("advertised.host.name", "127.0.0.1");
        kafkaProperties.setProperty("advertised.port", kafkaPort);
        kafkaProperties.setProperty("auto.create.topics.enable", "true");
        kafkaProperties.setProperty("zookeeper.session.timeout.ms", "30000");
        kafkaProperties.setProperty("broker.id", "1");

        final KafkaConfig config = new KafkaConfig(kafkaProperties);
        kafka = new KafkaServerStartable(config);
        getKafkaServer().startup();
    }

    /**
     * Creates a namespace in Kafka. If the namespace already exists this does nothing.
     * Will create a namespace with exactly 1 partition.
     * @param topicName - the namespace name to create.
     */
    public void createTopic(final String topicName) {
        createTopic(topicName, 1);
    }

    /**
     * Creates a namespace in Kafka. If the namespace already exists this does nothing.
     * @param topicName - the namespace name to create.
     * @param partitions - the number of partitions to create.
     */
    public void createTopic(final String topicName, final int partitions) {
        final int sessionTimeoutInMs = 10000;
        final int connectionTimeoutInMs = 10000;

        /*
         * Note: You must initialize the ZkClient with ZKStringSerializer. If you don't then createTopic()
         * will only seem to work (it will return without error). The namespace will exist only in ZooKeeper
         * and will be returned when listing topics, but Kafka itself does not create the namespace.
         */
        ZkClient zkClient = new ZkClient(
            getZookeeperServer().getConnectString(),
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
     */
    public void shutdown() throws Exception {
        close();
    }

    /**
     * Creates a kafka producer that is connected to our test server.
     */
    public KafkaProducer getKafkaProducer(final String keySerializer, final String valueSerializer) {
        // Create producer
        final Map<String, Object> kafkaProducerConfig = Maps.newHashMap();
        kafkaProducerConfig.put("bootstrap.servers", getKafkaConnectString());
        kafkaProducerConfig.put("key.serializer", keySerializer);
        kafkaProducerConfig.put("value.serializer", valueSerializer);
        kafkaProducerConfig.put("batch.size", 0);

        // Return our producer
        return new KafkaProducer(kafkaProducerConfig);
    }

    /**
     * Return Kafka Consumer configured to consume from internal Kafka Server.
     * @param keyDeserializer which deserializer to use for key
     * @param valueDeserializer which deserializer to use for value
     */
    public KafkaConsumer getKafkaConsumer(final String keyDeserializer, final String valueDeserializer) {
        Map<String, Object> kafkaConsumerConfig = Maps.newHashMap();
        kafkaConsumerConfig.put("bootstrap.servers", getKafkaConnectString());
        kafkaConsumerConfig.put("group.id", "test-consumer-id");
        kafkaConsumerConfig.put("key.deserializer", keyDeserializer);
        kafkaConsumerConfig.put("value.deserializer", valueDeserializer);
        kafkaConsumerConfig.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor");

        return new KafkaConsumer(kafkaConsumerConfig);
    }

    /**
     * Closes the internal servers. Failing to call this at the end of your tests will likely
     * result in leaking instances.
     */
    @Override
    public void close() throws Exception {
        if (getKafkaServer() != null) {
            getKafkaServer().shutdown();
            kafka = null;
        }
        if (getZookeeperServer() != null) {
            getZookeeperServer().close();
            zkServer = null;
        }
    }
}
