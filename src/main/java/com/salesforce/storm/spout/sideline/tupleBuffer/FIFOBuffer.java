package com.salesforce.storm.spout.sideline.tupleBuffer;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * FIFO implementation.  Has absolutely no "fairness" between VirtualSpouts or any kind of
 * "scheduling."
 */
public class FIFOBuffer implements TupleBuffer {

    /**
     * This implementation uses a simple Blocking Queue in a FIFO manner.
     */
    private BlockingQueue<KafkaMessage> tupleBuffer;

    public FIFOBuffer() {
    }

    /**
     * Helper method for creating a default instance.
     */
    public static FIFOBuffer createDefaultInstance() {
        Map<String, Object> map = Maps.newHashMap();
        map.put(SidelineSpoutConfig.TUPLE_BUFFER_MAX_SIZE, 10000);

        FIFOBuffer buffer = new FIFOBuffer();
        buffer.open(map);

        return buffer;
    }

    @Override
    public void open(Map topologyConfig) {
        // Defines the bounded size of our buffer.  Ideally this would be configurable.
        final int maxBufferSize = (int) topologyConfig.get(SidelineSpoutConfig.TUPLE_BUFFER_MAX_SIZE);

        // Create buffer.
        tupleBuffer = new LinkedBlockingQueue<>(maxBufferSize);
    }

    /**
     * Let the Implementation know that we're adding a new VirtualSpoutId.
     * @param virtualSpoutId - Identifier of new Virtual Spout.
     */
    @Override
    public void addVirtualSpoutId(final String virtualSpoutId) {
        // Nothing to do in this implementation.
    }

    /**
     * Let the Implementation know that we're removing/cleaning up from closing a VirtualSpout.
     * @param virtualSpoutId - Identifier of Virtual Spout to be cleaned up.
     */
    @Override
    public void removeVirtualSpoutId(final String virtualSpoutId) {
        // Nothing to do in this implementation.
    }

    /**
     * Put a new message onto the queue.  This method is blocking if the queue buffer is full.
     * @param kafkaMessage - KafkaMessage to be added to the queue.
     * @throws InterruptedException - thrown if a thread is interrupted while blocked adding to the queue.
     */
    @Override
    public void put(final KafkaMessage kafkaMessage) throws InterruptedException {
        tupleBuffer.put(kafkaMessage);
    }

    /**
     * @return - returns the next KafkaMessage to be processed out of the queue.
     */
    @Override
    public KafkaMessage poll() {
        return tupleBuffer.poll();
    }

    public BlockingQueue<KafkaMessage> getUnderlyingQueue() {
        return tupleBuffer;
    }
}
