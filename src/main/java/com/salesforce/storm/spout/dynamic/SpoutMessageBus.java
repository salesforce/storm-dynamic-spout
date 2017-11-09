package com.salesforce.storm.spout.dynamic;

import java.util.Optional;

/**
 * Facade in front of SpoutCoordinator reducing available scope.
 *
 * These are all of the methods available to the Spout instance.
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
