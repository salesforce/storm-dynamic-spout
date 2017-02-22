package com.salesforce.storm.spout.sideline.kafka.deserializer;

import org.apache.storm.shade.com.google.common.base.Charsets;
import org.apache.storm.tuple.Values;

/**
 * Simple deserializer that deserializes the key and message fields as UTF8 Strings.
 */
public class Utf8StringDeserializer implements Deserializer {
    @Override
    public Values deserialize(String topic, int partition, long offset, byte[] key, byte[] value) {
        return new Values(
                new String(key, Charsets.UTF_8),
                new String(value, Charsets.UTF_8)
        );
    }
}
