package com.salesforce.storm.spout.dynamic;

import java.util.Optional;

/**
 * Facade in front of MessageBus reducing available scope down to only the methods
 * that should be available to the main Spout/DynamicSpout instance.
 */
public interface SpoutMessageBus {

    /**
     * @return Returns any errors that should be reported up to the topology.
     */
    Optional<Throwable> getErrors();

    /**
     * @return Returns the next available Message to be emitted into the topology.
     */
    Optional<Message> nextMessage();

    /**
     * Acks a tuple on the spout that it belongs to.
     * @param id Tuple message id to ack
     */
    void ack(final MessageId id);

    /**
     * Fails a tuple on the spout that it belongs to.
     * @param id Tuple message id to fail
     */
    void fail(final MessageId id);
}
