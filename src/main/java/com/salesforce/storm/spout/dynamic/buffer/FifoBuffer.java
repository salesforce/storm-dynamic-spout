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
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * FIFO implementation.  Has absolutely no "fairness" between VirtualSpouts or any kind of
 * "scheduling."
 */
public class FifoBuffer implements MessageBuffer {
    private static final int DEFAULT_MAX_SIZE = 10_000;

    /**
     * This implementation uses a simple Blocking Queue in a FIFO manner.
     */
    private BlockingQueue<Message> messageBuffer;

    public FifoBuffer() {
    }

    /**
     * Helper method for creating a default instance.
     * @return factory method for create an instance of the buffer.
     */
    public static FifoBuffer createDefaultInstance() {
        final Map<String, Object> map = Maps.newHashMap();
        map.put(SpoutConfig.TUPLE_BUFFER_MAX_SIZE, DEFAULT_MAX_SIZE);
        final AbstractConfig spoutConfig = new AbstractConfig(new ConfigDefinition(), map);

        FifoBuffer buffer = new FifoBuffer();
        buffer.open(spoutConfig);

        return buffer;
    }

    @Override
    public void open(AbstractConfig spoutConfig) {
        // Defines the bounded size of our buffer.  Ideally this would be configurable.
        Object maxBufferSizeObj = spoutConfig.get(SpoutConfig.TUPLE_BUFFER_MAX_SIZE);
        int maxBufferSize = DEFAULT_MAX_SIZE;
        if (maxBufferSizeObj != null && maxBufferSizeObj instanceof Number) {
            maxBufferSize = ((Number) maxBufferSizeObj).intValue();
        }

        // Create buffer.
        messageBuffer = new LinkedBlockingQueue<>(maxBufferSize);
    }

    /**
     * Let the Implementation know that we're adding a new VirtualSpoutId.
     * @param virtualSpoutId - Identifier of new Virtual Spout.
     */
    @Override
    public void addVirtualSpoutId(final VirtualSpoutIdentifier virtualSpoutId) {
        // Nothing to do in this implementation.
    }

    /**
     * Let the Implementation know that we're removing/cleaning up from closing a VirtualSpout.
     * @param virtualSpoutId - Identifier of Virtual Spout to be cleaned up.
     */
    @Override
    public void removeVirtualSpoutId(final VirtualSpoutIdentifier virtualSpoutId) {
        // Nothing to do in this implementation.
    }

    /**
     * Put a new message onto the queue.  This method is blocking if the queue buffer is full.
     * @param message - Message to be added to the queue.
     * @throws InterruptedException - thrown if a thread is interrupted while blocked adding to the queue.
     */
    @Override
    public void put(final Message message) throws InterruptedException {
        messageBuffer.put(message);
    }

    @Override
    public int size() {
        return messageBuffer.size();
    }

    /**
     * @return - returns the next Message to be processed out of the queue.
     */
    @Override
    public Message poll() {
        return messageBuffer.poll();
    }

    public BlockingQueue<Message> getUnderlyingQueue() {
        return messageBuffer;
    }
}
