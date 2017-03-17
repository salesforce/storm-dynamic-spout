package com.salesforce.storm.spout.sideline.tupleBuffer;

import com.salesforce.storm.spout.sideline.KafkaMessage;

/**
 *
 */
public interface TupleBuffer {
    public void addConsumerId(final String consumerId);

    public void removeConsumerId(final String consumerId);

    public void put(final String consumerId, final KafkaMessage kafkaMessage) throws InterruptedException;

    public KafkaMessage poll();
}
