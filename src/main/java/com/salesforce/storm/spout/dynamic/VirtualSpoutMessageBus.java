package com.salesforce.storm.spout.dynamic;

import java.util.Optional;

/**
 * Facade in front of MessageBus reducing available scope down to only the methods
 * that should be available to virtual spouts.
 */
public interface VirtualSpoutMessageBus {

    /**
     * For registering new VirtualSpout.
     * @param virtualSpoutIdentifier
     */
    void registerVirtualSpout(final VirtualSpoutIdentifier virtualSpoutIdentifier);

    /**
     * Publish message.
     * @param message
     */
    void publishMessage(final Message message) throws InterruptedException;

    /**
     * @return How many un-read messages exist.
     */
    int messageSize();

    /**
     * Publish an error.
     * @param throwable
     */
    void publishError(final Throwable throwable);

    /**
     * Get next acked messageId for the given VirtualSpout.
     * @param virtualSpoutIdentifier
     * @return
     */
    Optional<MessageId> getAckedMessage(final VirtualSpoutIdentifier virtualSpoutIdentifier);

    /**
     * Get next failed messageId for the given VirtualSpout.
     * @param virtualSpoutIdentifier
     * @return
     */
    Optional<MessageId> getFailedMessage(final VirtualSpoutIdentifier virtualSpoutIdentifier);

    /**
     * Called to un-register a VirtualSpout.
     * @param virtualSpoutIdentifier
     */
    void unregisterVirtualSpout(final VirtualSpoutIdentifier virtualSpoutIdentifier);
}
