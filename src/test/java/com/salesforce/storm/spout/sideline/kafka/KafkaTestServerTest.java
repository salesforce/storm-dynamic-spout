package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Maps;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class KafkaTestServerTest {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTestServer.class);

    /**
     * Tests producer and old style consumer.
     */
    @Test
    public void testProducerAndConsumer() throws Exception {
        final String topicName = "dummy-topic";
        final int partitionId = 0;

        // Start server
        KafkaTestServer server = new KafkaTestServer();
        server.start();

        // Create a dummy topic
        server.createTopic(topicName);

        // Define our message
        final String expectedKey = "my-key";
        final String expectedValue = "my test message";

        // Publish a msg
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(topicName, partitionId, expectedKey, expectedValue.getBytes("utf8"));
        KafkaProducer<String, byte[]> producer = server.getKafkaProducer();
        Future<RecordMetadata> future = producer.send(producerRecord);
        producer.flush();
        while (!future.isDone()) {
            Thread.sleep(500L);
        }
        logger.info("Produce completed");
        producer.close();


        Map<String, Integer> topicCountMap = Maps.newHashMap();
        topicCountMap.put(topicName, 1);

        // Consume message
        logger.info("Starting to get Consumer...");
        ConsumerConnector consumer = server.getKafkaConsumerConnector();
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

        KafkaStream<byte[], byte[]> stream =  consumerMap.get(topicName).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        for (int x=0; x<5; x++) {
            if (it.hasNext()) {
                break;
            }
            Thread.sleep(1000L);
        }
        MessageAndMetadata<byte[], byte[]> kafkaMessage = it.next();
        String foundMessage = new String(kafkaMessage.message(), "utf8");
        String foundKey = new String(kafkaMessage.key(), "utf8");

        assertEquals("Should have message", expectedValue, foundMessage);
        assertEquals("Should have key", expectedKey, foundKey);
        consumer.shutdown();

        // Shutdown server
        server.shutdown();
    }
}