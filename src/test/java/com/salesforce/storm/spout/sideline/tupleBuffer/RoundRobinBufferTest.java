package com.salesforce.storm.spout.sideline.tupleBuffer;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import org.apache.storm.tuple.Values;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import static org.junit.Assert.*;

public class RoundRobinBufferTest {
    /**
     * Attempts to validate round-robin-ness of this.
     * Not entirely sure its a completely valid test, but its a best effort.
     */
    @Test
    public void doTest() throws InterruptedException {
        // numberOfVSpoutIds * numberOfMessagesPer should be less than the configured
        // max buffer size.
        final int numberOfVSpoutIds = 5;
        final int numberOfMessagesPer = 1500;
        final int maxBufferSize = (numberOfMessagesPer * numberOfVSpoutIds) + 1;

        // Create config
        Map<String, Object> config = Maps.newHashMap();
        config.put(SidelineSpoutConfig.TUPLE_BUFFER_MAX_SIZE, maxBufferSize);

        // Create buffer
        TupleBuffer tupleBuffer = new RoundRobinBuffer();
        tupleBuffer.open(config);

        // Keep track of our order per spoutId
        final Map<String, Queue<KafkaMessage>> submittedOrder = Maps.newHashMap();

        // Create random number generator
        Random random = new Random();

        // Generate messages
        for (int x=0; x<(numberOfMessagesPer * numberOfVSpoutIds); x++) {
            // Generate source spout id
            final String sourceSpoutId = "srcSpoutId" + random.nextInt(numberOfVSpoutIds);
            final int partition = random.nextInt(10);


            KafkaMessage kafkaMessage = new KafkaMessage(
                    new TupleMessageId("my topic", partition, x, sourceSpoutId),
                    new Values("poop" + x));

            // Keep track of order
            if (!submittedOrder.containsKey(sourceSpoutId)) {
                submittedOrder.put(sourceSpoutId, Queues.newLinkedBlockingQueue());
            }
            submittedOrder.get(sourceSpoutId).add(kafkaMessage);

            // Add to our buffer
            tupleBuffer.put(kafkaMessage);
        }

        // Now ask for the messages back, they should get round robin'd
        Iterator<String> keyIterator = Iterators.cycle(submittedOrder.keySet());
        for (int x=0; x<(numberOfMessagesPer * numberOfVSpoutIds); x++) {
            String nextSourceSpout = keyIterator.next();

            // Pop a msg
            final KafkaMessage bufferedMsg = tupleBuffer.poll();

            // Skip null returns.
            if (bufferedMsg == null) {
                // decrement x so we don't skip an iteration.
                x--;
                continue;
            }

            // Get which spout this was a source from
            final String bufferedSrcSpoutId = bufferedMsg.getTupleMessageId().getSrcConsumerId();

            // Get the next message from this
            KafkaMessage nextExpectedKafkaMsg = null;
            while (nextExpectedKafkaMsg == null) {
                nextExpectedKafkaMsg = submittedOrder.get(nextSourceSpout).poll();

                // If the next msg is null, then we skip to next entry and try again
                if (nextExpectedKafkaMsg == null) {
                    assertNotEquals("Should not be next source spout id", nextSourceSpout, bufferedSrcSpoutId);

                    // Get next entry, and loop
                    nextSourceSpout = keyIterator.next();
                }
            }

            // Validate it
            assertNotNull("Should not be null", bufferedMsg);
            assertEquals("Objects should be the same", nextExpectedKafkaMsg, bufferedMsg);

            // Should be from the source
            assertEquals("Source Spout Id should be equal", nextSourceSpout, bufferedMsg.getTupleMessageId().getSrcConsumerId());


            // Validate the contents are the same
            assertEquals("partitions should be equal", nextExpectedKafkaMsg.getPartition(), bufferedMsg.getPartition());
            assertEquals("offsets should be equal", nextExpectedKafkaMsg.getOffset(), bufferedMsg.getOffset());
            assertEquals("topic should be equal", nextExpectedKafkaMsg.getTopic(), bufferedMsg.getTopic());
            assertEquals("TupleMessageIds should be equal", nextExpectedKafkaMsg.getTupleMessageId(), bufferedMsg.getTupleMessageId());
            assertEquals("Values should be equal", nextExpectedKafkaMsg.getValues(), bufferedMsg.getValues());
        }


        // Validate that the next polls are all null
        for (int x=0; x<128; x++) {
            assertNull("Should be null", tupleBuffer.poll());
        }
    }
}