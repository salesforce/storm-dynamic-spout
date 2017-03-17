package com.salesforce.storm.spout.sideline.tupleBuffer;

import com.salesforce.storm.spout.sideline.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A round-robbin implementation.  Each virtual spout has its own buffer that both gets added to individually, and thus chatty
 * virtual spouts will not block less chatty ones.  We RR thru all the available buffers to get the next msg.
 *
 * I think there are some concurrency issues here that need to be addressed between add/remove consumerId, and poll().
 */
public class RoundRobinBuffer implements TupleBuffer {
    private static final Logger logger = LoggerFactory.getLogger(RoundRobinBuffer.class);

    /**
     * Defines the bounded size of our buffer PER VirtualSpout.  Ideally this would be configurable.
     */
    private static final int MAX_BUFFER_PER_VIRTUAL_SPOUT = 2000;

    /**
     * A Map of VirtualSpoutIds => Its own Blocking Queue.
     */
    private final Map<String, BlockingQueue<KafkaMessage>> tupleBuffer = new ConcurrentHashMap<>();

    /**
     * An iterator over the Keys in tupleBuffer.  Used to Round Robin thru the VirtualSpouts.
     */
    private Iterator<String> consumerIdIterator = null;

    public RoundRobinBuffer() {
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
        final String virtualSpoutId = kafkaMessage.getTupleMessageId().getSrcConsumerId();

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

    private BlockingQueue<KafkaMessage> createNewEmptyQueue() {
        return new LinkedBlockingQueue<>(MAX_BUFFER_PER_VIRTUAL_SPOUT);
    }
}
