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

package com.salesforce.storm.spout.dynamic.buffer;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.Message;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import com.salesforce.storm.spout.dynamic.config.DynamicSpoutConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A round-robin implementation.  Each virtual spout has its own queue that gets added too.  A very chatty
 * virtual spout will not block/overrun less chatty ones.  {@link #poll()} will RR through all the available
 * queues to get the next msg.
 *
 * Internally we make use of BlockingQueues so that we can put an upper bound on the queue size.
 * Once a queue is full, any producer attempting to put more messages onto the queue will block and wait
 * for available space in the queue.  This acts to throttle producers of messages.
 * Consumers from the queue on the other hand will never block attempting to read from a queue, even if its empty.
 * This means consuming from the queue will always be fast.
 *
 * There may be some concurrency issues here that need to be addressed between add/remove virtualSpoutId, and poll().
 */
public class RoundRobinBuffer implements MessageBuffer {
    private static final Logger logger = LoggerFactory.getLogger(RoundRobinBuffer.class);

    /**
     * A Map of VirtualSpoutIds => Its own Blocking Queue.
     */
    private final Map<VirtualSpoutIdentifier, BlockingQueue<Message>> messageBuffer = new ConcurrentHashMap<>();

    /**
     * Defines the bounded size of our buffer PER VirtualSpout.
     */
    private int maxBufferSizePerVirtualSpout = 2000;

    /**
     * An iterator over the Keys in buffer.  Used to Round Robin through the VirtualSpouts.
     */
    private Iterator<VirtualSpoutIdentifier> consumerIdIterator = null;

    public RoundRobinBuffer() {
    }

    /**
     * Helper method for creating a default instance.
     * @return factory method for create an instance of the buffer.
     */
    public static RoundRobinBuffer createDefaultInstance() {
        Map<String, Object> map = Maps.newHashMap();
        map.put(DynamicSpoutConfig.TUPLE_BUFFER_MAX_SIZE, 10000);
        final SpoutConfig spoutConfig = new SpoutConfig(new ConfigDefinition(), map);

        RoundRobinBuffer buffer = new RoundRobinBuffer();
        buffer.open(spoutConfig);

        return buffer;
    }

    @Override
    public void open(final SpoutConfig spoutConfig) {
        Object maxBufferSizeObj = spoutConfig.get(DynamicSpoutConfig.TUPLE_BUFFER_MAX_SIZE);
        if (maxBufferSizeObj == null) {
            // Not configured, use default value.
            return;
        }

        if (maxBufferSizeObj instanceof Number) {
            maxBufferSizePerVirtualSpout = ((Number) maxBufferSizeObj).intValue();
        }
    }

    /**
     * Let the Implementation know that we're adding a new VirtualSpoutId.
     * @param virtualSpoutId - Identifier of new Virtual Spout.
     */
    @Override
    public void addVirtualSpoutId(final VirtualSpoutIdentifier virtualSpoutId) {
        synchronized (messageBuffer) {
            messageBuffer.putIfAbsent(virtualSpoutId, createNewEmptyQueue());
        }
    }

    /**
     * Let the Implementation know that we're removing/cleaning up from closing a VirtualSpout.
     * @param virtualSpoutId - Identifier of Virtual Spout to be cleaned up.
     */
    @Override
    public void removeVirtualSpoutId(VirtualSpoutIdentifier virtualSpoutId) {
        synchronized (messageBuffer) {
            messageBuffer.remove(virtualSpoutId);
        }
    }

    /**
     * Put a new message onto the queue.  This method is blocking if the queue buffer is full.
     * @param message - Message to be added to the queue.
     * @throws InterruptedException - thrown if a thread is interrupted while blocked adding to the queue.
     */
    @Override
    public void put(final Message message) throws InterruptedException {
        // Grab the source virtual spoutId
        final VirtualSpoutIdentifier virtualSpoutId = message.getMessageId().getSrcVirtualSpoutId();

        // Add to correct buffer
        BlockingQueue<Message> virtualSpoutQueue = messageBuffer.get(virtualSpoutId);

        // If our queue doesn't exist
        if (virtualSpoutQueue == null) {
            // Attempt to put it
            messageBuffer.putIfAbsent(virtualSpoutId, createNewEmptyQueue());

            // Grab a reference.
            virtualSpoutQueue = messageBuffer.get(virtualSpoutId);
        }
        // Put it.
        virtualSpoutQueue.put(message);
    }

    @Override
    public int size() {
        int total = 0;
        for (final Queue queue: messageBuffer.values()) {
            total += queue.size();
        }
        return total;
    }

    /**
     * @return - returns the next Message to be processed out of the queue.
     */
    @Override
    public Message poll() {
        // If its null, or we hit the end, reset it.
        if (consumerIdIterator == null || !consumerIdIterator.hasNext()) {
            consumerIdIterator = messageBuffer.keySet().iterator();
        }

        // Try every buffer until we hit the end.
        Message returnMsg = null;
        while (returnMsg == null && consumerIdIterator.hasNext()) {

            // Advance iterator
            final VirtualSpoutIdentifier nextConsumerId = consumerIdIterator.next();

            // Find our buffer
            final BlockingQueue<Message> queue = messageBuffer.get(nextConsumerId);

            // We missed?
            if (queue == null) {
                logger.info("Non-existent queue found, resetting iterator.");
                consumerIdIterator = messageBuffer.keySet().iterator();
                continue;
            }
            returnMsg = queue.poll();
        }
        return returnMsg;
    }

    /**
     * @return - return a new LinkedBlockingQueue instance with a max size of our configured buffer.
     */
    private BlockingQueue<Message> createNewEmptyQueue() {
        return new LinkedBlockingQueue<>(getMaxBufferSizePerVirtualSpout());
    }

    /**
     * @return - returns the configured max buffer size.
     */
    int getMaxBufferSizePerVirtualSpout() {
        return maxBufferSizePerVirtualSpout;
    }
}
