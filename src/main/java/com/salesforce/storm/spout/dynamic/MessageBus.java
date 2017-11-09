package com.salesforce.storm.spout.dynamic;

import com.salesforce.storm.spout.dynamic.buffer.MessageBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 */
public class MessageBus implements VirtualSpoutMessageBus, SpoutMessageBus {
    private static final Logger logger = LoggerFactory.getLogger(MessageBus.class);

    /**
     * Queue for tuples that are ready to be emitted out into the topology.
     */
    private final MessageBuffer messageBuffer;

    /**
     * Buffer by spout consumer id of messages that have been acked.
     */
    private final Map<VirtualSpoutIdentifier, Queue<MessageId>> ackedTuplesQueue = new ConcurrentHashMap<>();

    /**
     * Buffer by spout consumer id of messages that have been failed.
     */
    private final Map<VirtualSpoutIdentifier, Queue<MessageId>> failedTuplesQueue = new ConcurrentHashMap<>();

    /**
     * Buffer for errors that need to be reported.
     */
    private final Queue<Throwable> reportedErrorsQueue = new ConcurrentLinkedQueue<>();

    public MessageBus(final MessageBuffer messageBuffer) {
        this.messageBuffer = messageBuffer;
    }

////// Spout Message Bus Interface Methods
    @Override
    public Optional<Throwable> getErrors() {
        // Poll is non-blocking.
        return Optional.ofNullable(reportedErrorsQueue.poll());
    }

    @Override
    public Optional<Message> nextMessage() {
        return Optional.ofNullable(messageBuffer.poll());
    }

    @Override
    public void ack(final MessageId id) {
        if (!ackedTuplesQueue.containsKey(id.getSrcVirtualSpoutId())) {
            logger.warn("Acking tuple for unknown consumer");
            return;
        }

        ackedTuplesQueue.get(id.getSrcVirtualSpoutId()).add(id);
    }

    @Override
    public void fail(final MessageId id) {
        if (!failedTuplesQueue.containsKey(id.getSrcVirtualSpoutId())) {
            logger.warn("Failing tuple for unknown consumer");
            return;
        }

        failedTuplesQueue.get(id.getSrcVirtualSpoutId()).add(id);
    }

////// Virtual Spout Message Bus Interface Methods

    @Override
    public void registerVirtualSpout(final VirtualSpoutIdentifier virtualSpoutIdentifier) {
        messageBuffer.addVirtualSpoutId(virtualSpoutIdentifier);
        ackedTuplesQueue.put(virtualSpoutIdentifier, new ConcurrentLinkedQueue<>());
        failedTuplesQueue.put(virtualSpoutIdentifier, new ConcurrentLinkedQueue<>());
    }

    @Override
    public void publishMessage(final Message message) throws InterruptedException {
        messageBuffer.put(message);
    }

    @Override
    public int messageSize() {
        return messageBuffer.size();
    }

    @Override
    public void publishError(final Throwable throwable) {
        reportedErrorsQueue.add(throwable);
    }

    @Override
    public Optional<MessageId> getAckedMessage(final VirtualSpoutIdentifier virtualSpoutIdentifier) {
        final MessageId id = ackedTuplesQueue.get(virtualSpoutIdentifier).poll();
        return Optional.ofNullable(id);
    }

    @Override
    public Optional<MessageId> getFailedMessage(final VirtualSpoutIdentifier virtualSpoutIdentifier) {
        final MessageId id = failedTuplesQueue.get(virtualSpoutIdentifier).poll();
        return Optional.ofNullable(id);
    }

    @Override
    public void unregisterVirtualSpout(final VirtualSpoutIdentifier virtualSpoutIdentifier) {
        messageBuffer.removeVirtualSpoutId(virtualSpoutIdentifier);
        ackedTuplesQueue.remove(virtualSpoutIdentifier);
        failedTuplesQueue.remove(virtualSpoutIdentifier);
    }
}
