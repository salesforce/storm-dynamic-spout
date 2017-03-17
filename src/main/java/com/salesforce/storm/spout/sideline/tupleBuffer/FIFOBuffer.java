package com.salesforce.storm.spout.sideline.tupleBuffer;

import com.salesforce.storm.spout.sideline.KafkaMessage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * FIFO implementation.  Has absolutely no "fairness" between VirtualSpouts or any kind of
 * "scheduling."
 */
public class FIFOBuffer implements TupleBuffer {

    /**
     * Defines the bounded size of our buffer.  Ideally this would be configurable.
     */
    private static final int MAX_BUFFER_SIZE = 10000;

    /**
     * This implementation uses a simple Blocking Queue in a FIFO manner.
     */
    private final BlockingQueue<KafkaMessage> tupleBuffer = new LinkedBlockingDeque<>(MAX_BUFFER_SIZE);

    public FIFOBuffer() {
    }

    /**
     * Let the Implementation know that we're adding a new VirtualSpoutId.
     * @param virtualSpoutId - Identifier of new Virtual Spout.
     */
    public void addVirtualSpoutId(final String virtualSpoutId) {
        // Nothing to do in this implementation.
    }

    /**
     * Let the Implementation know that we're removing/cleaning up from closing a VirtualSpout.
     * @param virtualSpoutId - Identifier of Virtual Spout to be cleaned up.
     */
    public void removeVirtualSpoutId(final String virtualSpoutId) {
        // Nothing to do in this implementation.
    }

    /**
     * Put a new message onto the queue.  This method is blocking if the queue buffer is full.
     * @param virtualSpoutId - ConsumerId this message is from.
     * @param kafkaMessage - KafkaMessage to be added to the queue.
     * @throws InterruptedException - thrown if a thread is interrupted while blocked adding to the queue.
     */
    public void put(final String virtualSpoutId, final KafkaMessage kafkaMessage) throws InterruptedException {
        tupleBuffer.put(kafkaMessage);
    }

    /**
     * @return - returns the next KafkaMessage to be processed out of the queue.
     */
    public KafkaMessage poll() {
        return tupleBuffer.poll();
    }

    public BlockingQueue<KafkaMessage> getUnderlyingQueue() {
        return tupleBuffer;
    }
}
