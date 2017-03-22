package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 *
 */
public class SidelineConsumerConfigTest {

    /**
     * Validates that we have some sane default settings.
     */
    @Test
    public void testForSaneDefaultKafkaConsumerSettings() {
        final List<String> brokerHosts = Lists.newArrayList(
                "broker1:9092", "broker2:9093"
        );
        final String consumerId = "myConsumerId";
        final String topic = "myTopic";

        // Create config
        final SidelineConsumerConfig config = new SidelineConsumerConfig(brokerHosts, consumerId, topic);

        // now do validation on constructor arguments
        assertEquals("Topic set", topic, config.getTopic());
        assertEquals("ConsumerId set", consumerId, config.getConsumerId());
        assertEquals("BrokerHosts set", "broker1:9092,broker2:9093", config.getKafkaConsumerProperty("bootstrap.servers"));

        // Check defaults are sane.
        assertFalse("Consumer Autocommit defaults to false", config.isConsumerStateAutoCommit());
        assertEquals("autocommit set to false", "false", config.getKafkaConsumerProperty("enable.auto.commit"));
        assertEquals("group.id set", consumerId, config.getKafkaConsumerProperty("group.id"));
        assertEquals("auto.offset.reset set to none", "none", config.getKafkaConsumerProperty("auto.offset.reset"));
        assertEquals("Key Deserializer set to bytes deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer", config.getKafkaConsumerProperty("key.deserializer"));
        assertEquals("Value Deserializer set to bytes deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer", config.getKafkaConsumerProperty("value.deserializer"));



    }
}