package com.salesforce.storm.spout.sideline.kafka.deserializer.compat;

import org.apache.storm.tuple.Fields;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Interface shamelessly stolen from Storm-Kafka with the intent of providing a compatibility layer.
 */
public interface Scheme extends Serializable {
    List<Object> deserialize(ByteBuffer ser);

    Fields getOutputFields();
}