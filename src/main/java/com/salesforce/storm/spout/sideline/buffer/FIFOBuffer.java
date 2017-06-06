package com.salesforce.storm.spout.sideline.buffer;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.Message;
import com.salesforce.storm.spout.sideline.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * FIFO implementation.  Has absolutely no "fairness" between VirtualSpouts or any kind of
 * "scheduling."
 */
public class FIFOBuffer implements MessageBuffer {
    private static final int DEFAULT_MAX_SIZE = 10_000;

    /**
     * This implementation uses a simple Blocking Queue in a FIFO manner.
     */
    private BlockingQueue<Message> messageBuffer;

    public FIFOBuffer() {
    }

    /**
     * Helper method for creating a default instance.
     */
    public static FIFOBuffer createDefaultInstance() {
        Map<String, Object> map = Maps.newHashMap();
        map.put(SidelineSpoutConfig.TUPLE_BUFFER_MAX_SIZE, DEFAULT_MAX_SIZE);

        FIFOBuffer buffer = new FIFOBuffer();
        buffer.open(map);

        return buffer;
    }

    @Override
    public void open(Map spoutConfig) {
        // Defines the bounded size of our buffer.  Ideally this would be configurable.
        Object maxBufferSizeObj = spoutConfig.get(SidelineSpoutConfig.TUPLE_BUFFER_MAX_SIZE);
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
