package com.salesforce.storm.spout.sideline.tupleBuffer;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
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
 * virtual spout will not block/overrun less chatty ones.  {@link #poll()} will RR thru all the available
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
public class RoundRobinBuffer implements TupleBuffer {
    private static final Logger logger = LoggerFactory.getLogger(RoundRobinBuffer.class);

    /**
     * A Map of VirtualSpoutIds => Its own Blocking Queue.
     */
    private final Map<String, BlockingQueue<KafkaMessage>> tupleBuffer = new ConcurrentHashMap<>();

    /**
     * Defines the bounded size of our buffer PER VirtualSpout.
     */
    private int maxBufferSizePerVirtualSpout = 2000;

    /**
     * An iterator over the Keys in tupleBuffer.  Used to Round Robin thru the VirtualSpouts.
     */
    private Iterator<String> consumerIdIterator = null;

    public RoundRobinBuffer() {
    }

    /**
     * Helper method for creating a default instance.
     */
    public static RoundRobinBuffer createDefaultInstance() {
        Map<String, Object> map = Maps.newHashMap();
        map.put(SidelineSpoutConfig.TUPLE_BUFFER_MAX_SIZE, 10000);

        RoundRobinBuffer buffer = new RoundRobinBuffer();
        buffer.open(map);

        return buffer;
    }

    @Override
    public void open(Map topologyConfig) {
        maxBufferSizePerVirtualSpout = (int) topologyConfig.get(SidelineSpoutConfig.TUPLE_BUFFER_MAX_SIZE);
    }

    /**
     * Let the Implementation know that we're adding a new VirtualSpoutId.
     * @param virtualSpoutId - Identifier of new Virtual Spout.
     */
    @Override
    public void addVirtualSpoutId(final String virtualSpoutId) {
        synchronized (tupleBuffer) {
            tupleBuffer.putIfAbsent(virtualSpoutId, createNewEmptyQueue());
        }
    }

    /**
     * Let the Implementation know that we're removing/cleaning up from closing a VirtualSpout.
     * @param virtualSpoutId - Identifier of Virtual Spout to be cleaned up.
     */
    @Override
    public void removeVirtualSpoutId(String virtualSpoutId) {
        synchronized (tupleBuffer) {
            tupleBuffer.remove(virtualSpoutId);
        }
    }

    /**
     * Put a new message onto the queue.  This method is blocking if the queue buffer is full.
     * @param kafkaMessage - KafkaMessage to be added to the queue.
     * @throws InterruptedException - thrown if a thread is interrupted while blocked adding to the queue.
     */
    @Override
    public void put(final KafkaMessage kafkaMessage) throws InterruptedException {
        // Grab the source virtual spoutId
        final String virtualSpoutId = kafkaMessage.getTupleMessageId().getSrcVirtualSpoutId();

        // Add to correct buffer
        BlockingQueue virtualSpoutQueue = tupleBuffer.get(virtualSpoutId);

        // If our queue doesn't exist
        if (virtualSpoutQueue == null) {
            // Attempt to put it
            tupleBuffer.putIfAbsent(virtualSpoutId, createNewEmptyQueue());

            // Grab a reference.
            virtualSpoutQueue = tupleBuffer.get(virtualSpoutId);
        }
        // Put it.
        virtualSpoutQueue.put(kafkaMessage);
    }

    @Override
    public int size() {
        int total = 0;
        for (String key: tupleBuffer.keySet()) {
            Queue queue = tupleBuffer.get(key);
            if (queue != null) {
                total += queue.size();
            }
        }
        return total;
    }

    /**
     * @return - returns the next KafkaMessage to be processed out of the queue.
     */
    @Override
    public KafkaMessage poll() {
        // If its null, or we hit the end, reset it.
        if (consumerIdIterator == null || !consumerIdIterator.hasNext()) {
            consumerIdIterator = tupleBuffer.keySet().iterator();
        }

        // Try every buffer until we hit the end.
        KafkaMessage returnMsg = null;
        while (returnMsg == null && consumerIdIterator.hasNext()) {

            // Advance iterator
            final String nextConsumerId = consumerIdIterator.next();

            // Find our buffer
            final BlockingQueue<KafkaMessage> queue = tupleBuffer.get(nextConsumerId);

            // We missed?
            if (queue == null) {
                logger.info("Non-existant queue found, resetting iterator.");
                consumerIdIterator = tupleBuffer.keySet().iterator();
                continue;
            }
            returnMsg = queue.poll();
        }
        return returnMsg;
    }

    /**
     * @return - return a new LinkedBlockingQueue instance with a max size of our configured buffer.
     */
    private BlockingQueue<KafkaMessage> createNewEmptyQueue() {
        return new LinkedBlockingQueue<>(maxBufferSizePerVirtualSpout);
    }
}
