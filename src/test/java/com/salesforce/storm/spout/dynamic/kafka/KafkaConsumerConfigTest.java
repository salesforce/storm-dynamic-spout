/*
 * Copyright (c) 2018, Salesforce.com, Inc.
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

import com.google.common.collect.Lists;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Validates that we have some sane default settings.
 */
public class KafkaConsumerConfigTest {

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
        final KafkaConsumerConfig config = new KafkaConsumerConfig(brokerHosts, consumerId, topic);

        // now do validation on constructor arguments
        assertEquals("Topic set", topic, config.getTopic());
        assertEquals("ConsumerId set", consumerId, config.getConsumerId());
        assertEquals("BrokerHosts set", "broker1:9092,broker2:9093", config.getKafkaConsumerProperty("bootstrap.servers"));

        // Check defaults are sane.
        assertEquals("group.id set", consumerId, config.getKafkaConsumerProperty("group.id"));
        assertEquals("auto.offset.reset set to none", "none", config.getKafkaConsumerProperty("auto.offset.reset"));
        assertEquals(
            "Key Deserializer set to bytes deserializer",
            ByteArrayDeserializer.class.getName(),
            config.getKafkaConsumerProperty("key.deserializer"));
        assertEquals(
            "Value Deserializer set to bytes deserializer",
            ByteArrayDeserializer.class.getName(),
            config.getKafkaConsumerProperty("value.deserializer")
        );
    }
}