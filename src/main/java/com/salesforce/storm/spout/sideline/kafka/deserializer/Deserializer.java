package com.salesforce.storm.spout.sideline.kafka.deserializer;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.Serializable;

/**
 * This interface allows you to define how to deserialize messages coming from Kafka into values
 * that get emitted as tuples into the topology.
 */
public interface Deserializer extends Serializable {

    /**
     * This is the method your implementation would need define.
     * A null return value from here will result in this message being ignored.
     *
     * @param topic - represents what namespace this message came from.
     * @param partition - represents what partition this message came from.
     * @param offset - represents what offset this message came from.
     * @param key - byte array representing the key.
     * @param value - byte array representing the value.
     * @return Values that should be emitted by the spout to the topology.
     */
    Values deserialize(final String topic, final int partition, final long offset, final byte[] key, final byte[] value);

    /**
     * Declares the output fields for the deserializer.
     * @return An instance of the fields
     */
    Fields getOutputFields();
}
