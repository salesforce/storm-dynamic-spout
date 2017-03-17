package com.salesforce.storm.spout.sideline.kafka.deserializer;

import org.apache.storm.shade.com.google.common.base.Charsets;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test over Utf8StringDeserializer.
 */
public class Utf8StringDeserializerTest {
    /**
     * Validates that we can deserialize.
     */
    @Test
    public void testDeserializeWithConsumerRecord() {
        // Define inputs
        final String expectedKey = "This is My Key";
        final String expectedValue = "This is my message \uD83D\uDCA9";
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 34;
        final long expectedOffset = 31337L;

        // Attempt to deserialize.
        final Deserializer deserializer = new Utf8StringDeserializer();
        final Values deserializedValues = deserializer.deserialize(expectedTopic, expectedPartition, expectedOffset, expectedKey.getBytes(Charsets.US_ASCII), expectedValue.getBytes(Charsets.UTF_8));

        assertEquals("Values has 2 entries", 2, deserializedValues.size());
        assertEquals("Got expected key", expectedKey, deserializedValues.get(0));
        assertEquals("Got expected value", expectedValue, deserializedValues.get(1));
    }

    @Test
    public void testGetOutputFields() {
        final Deserializer deserializer = new Utf8StringDeserializer();
        final Fields fields = deserializer.getOutputFields();
        assertEquals(fields.get(0), "key");
        assertEquals(fields.get(1), "value");
    }
}