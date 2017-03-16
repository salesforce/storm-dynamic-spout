package com.salesforce.storm.spout.sideline.tupleBuffer;

import com.salesforce.storm.spout.sideline.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * A round-robbin implementation.  Each virtual spout has its own buffer that gets cycled thru.
 * I think there are some concurrency issues here between add/remove consumerId, and poll().
 */
public class RoundRobbinBuffer implements TupleBuffer {
    // Logging.
    private static final Logger logger = LoggerFactory.getLogger(RoundRobbinBuffer.class);

    private final int max_buffer_per_consumer = 2000;

    private final Map<String, BlockingQueue<KafkaMessage>> tupleBuffer = new ConcurrentHashMap<>();

    private Iterator<String> consumerIdIterator = null;

    public RoundRobbinBuffer() {
    }

    public void addConsumerId(final String consumerId) {
        synchronized (tupleBuffer) {
            tupleBuffer.putIfAbsent(consumerId, new LinkedBlockingDeque<>(max_buffer_per_consumer));
        }
    }

    @Override
    public void removeConsumerId(String consumerId) {
        synchronized (tupleBuffer) {
            tupleBuffer.remove(consumerId);
        }
    }

    @Override
    public void put(final String consumerId, final KafkaMessage kafkaMessage) throws InterruptedException {
        tupleBuffer.get(consumerId).put(kafkaMessage);
    }

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
                consumerIdIterator = null;
                return null;
            }
            returnMsg = queue.poll();
        }
        return returnMsg;
    }
}
