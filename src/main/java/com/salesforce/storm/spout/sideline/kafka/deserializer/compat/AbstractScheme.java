package com.salesforce.storm.spout.sideline.kafka.deserializer.compat;

import com.salesforce.storm.spout.sideline.kafka.deserializer.Deserializer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Provides a compatibility-like layer to Storm-Kafka Scheme interface.
 */
public abstract class AbstractScheme implements Deserializer {

    /**
     * @return - Define the fields that you will deserialize into.
     */
    public abstract Fields getOutputFields();

    /**
     * Implement deserialization logic and return List of objects/tuple values.
     * @param ser - byte buffer to deserialize.
     * @return - List of objects/tuple values.
     */
    public abstract List<Object> deserialize(ByteBuffer ser);

    /**
     * Provides compatibility layer to 'Storm-Kafka' Scheme-like interface.
     */
    public Values deserialize(String topic, int partition, long offset, byte[] key, byte[] value) {
        List<Object> list = deserialize(ByteBuffer.wrap(value));
        if (list == null) {
            return null;
        }
        Values values = new Values();
        values.addAll(list);
        return values;
    }
}
