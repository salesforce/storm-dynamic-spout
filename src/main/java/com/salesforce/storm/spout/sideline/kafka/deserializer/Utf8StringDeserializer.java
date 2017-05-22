package com.salesforce.storm.spout.sideline.kafka.deserializer;

import com.google.common.base.Charsets;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Simple deserializer that can deserialize the key and message fields as UTF8 Strings.
 */
public class Utf8StringDeserializer implements Deserializer {

    @Override
    public Values deserialize(String topic, int partition, long offset, byte[] key, byte[] value) {
        return new Values(
                new String(key, Charsets.UTF_8),
                new String(value, Charsets.UTF_8)
        );
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("key", "value");
    }
}
