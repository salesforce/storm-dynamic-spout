package com.salesforce.storm.spout.sideline.tupleBuffer;

import com.salesforce.storm.spout.sideline.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * FIFO implementation.
 */
public class FIFOBuffer implements TupleBuffer {
    // Logging.
    private static final Logger logger = LoggerFactory.getLogger(RoundRobbinBuffer.class);

    private final int max_buffer = 10000;

    private final BlockingQueue<KafkaMessage> tupleBuffer = new LinkedBlockingDeque<>(max_buffer);

    public FIFOBuffer() {
    }

    public void addConsumerId(final String consumerId) {
        // Nothing to do in this implementation.
        return;
    }

    public void removeConsumerId(final String consumerId) {
        // Nothing to do in this implementation.
        return;
    }

    public void put(final String consumerId, final KafkaMessage kafkaMessage) {
        try {
            tupleBuffer.put(kafkaMessage);
        } catch (InterruptedException e) {
            // TODO: Revisit this
            logger.error("{}", e);
        }
    }

    public KafkaMessage poll() {
        return tupleBuffer.poll();
    }
}
