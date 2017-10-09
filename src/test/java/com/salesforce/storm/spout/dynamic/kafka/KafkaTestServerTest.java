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
 * Tests producer and old style consumer.
 */
public class KafkaTestServerTest {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTestServerTest.class);

    /**
     * Tests producer and old style consumer.
     */
    @Test
    public void testProducerAndConsumer() throws Exception {
        final String topicName = "dummy-namespace";
        final int partitionId = 0;

        // Start server
        KafkaTestServer server = new KafkaTestServer();
        server.start();

        // Create a dummy namespace
        server.createTopic(topicName);

        // Define our message
        final String expectedKey = "my-key";
        final String expectedValue = "my test message";

        // Publish a msg
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<>(
            topicName,
            partitionId,
            expectedKey,
            expectedValue.getBytes("utf8")
        );
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
        for (int x = 0; x < 5; x++) {
            if (it.hasNext()) {
                break;
            }
            Thread.sleep(1000L);
        }
        MessageAndMetadata<byte[], byte[]> message = it.next();
        String foundMessage = new String(message.message(), "utf8");
        String foundKey = new String(message.key(), "utf8");

        assertEquals("Should have message", expectedValue, foundMessage);
        assertEquals("Should have key", expectedKey, foundKey);
        consumer.shutdown();

        // Shutdown server
        server.shutdown();
    }
}