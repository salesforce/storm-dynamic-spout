package com.salesforce.storm.spout.dynamic;

import java.util.Map;
import java.util.Optional;

/**
 * Facade in front of Coordinator reducing available scope.
 */
public interface SpoutCoordinator {
    void open(final Map<String, Object> config);
    Optional<Throwable> getErrors();
    Optional<Message> nextMessage();
    void close();
    void ack(final MessageId id);
    void fail(final MessageId id);
    void addVirtualSpout(final DelegateSpout spout);
    void removeVirtualSpout(final VirtualSpoutIdentifier virtualSpoutIdentifier);
    boolean hasVirtualSpout(final VirtualSpoutIdentifier spoutIdentifier);
}
