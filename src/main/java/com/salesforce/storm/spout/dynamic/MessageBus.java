/**
 * Copyright (c) 2017, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 *   disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * * Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or promote products
 *   derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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
 * Provides an abstraction for passing messages between the DynamicSpout instance
 * and VirtualSpout instances.
 *
 * Since these instances live across different threads internally we make use of
 * thread safe concurrent data structures making this instance ThreadSafe.
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

    /**
     * Constructor.
     * @param messageBuffer Provides the MessageBuffer implementation to use.
     */
    public MessageBus(final MessageBuffer messageBuffer) {
        this.messageBuffer = messageBuffer;
    }

    // SpoutMessageBus Interface methods.

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
        // TODO this actually isn't thread safe :(
        if (!ackedTuplesQueue.containsKey(id.getSrcVirtualSpoutId())) {
            logger.warn("Acking tuple for unknown consumer");
            return;
        }

        ackedTuplesQueue.get(id.getSrcVirtualSpoutId()).add(id);
    }

    @Override
    public void fail(final MessageId id) {
        // TODO this actually isn't thread safe :(
        if (!failedTuplesQueue.containsKey(id.getSrcVirtualSpoutId())) {
            logger.warn("Failing tuple for unknown consumer");
            return;
        }

        failedTuplesQueue.get(id.getSrcVirtualSpoutId()).add(id);
    }

    // VirtualSpoutMessageBus interface methods.

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

    // Additional Helper methods not exposed via interfaces.

    /**
     * @return Count of acked messageIds that exist within the bus.
     */
    public int ackSize() {
        int size = 0;
        for (final Queue queue: ackedTuplesQueue.values()) {
            size += queue.size();
        }
        return size;
    }

    /**
     * @return Count of failed messageIds that exist within the bus.
     */
    public int failSize() {
        int size = 0;
        for (final Queue queue: failedTuplesQueue.values()) {
            size += queue.size();
        }
        return size;
    }
}