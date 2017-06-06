package com.salesforce.storm.spout.sideline.kafka.deserializer;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Define a deserializer that always returns null.  Only used for testing purposes.
 */
public class NullDeserializer implements Deserializer {
    @Override
    public Values deserialize(String topic, int partition, long offset, byte[] key, byte[] value) {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields();
    }
}
