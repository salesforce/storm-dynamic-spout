package com.salesforce.storm.spout.sideline.tupleBuffer;

import com.google.common.collect.Lists;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import org.apache.storm.tuple.Values;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

/**
 * Kind of silly.  Basically just testing a FIFO buffer.
 */
public class FIFOBufferTest {
    private static final Logger logger = LoggerFactory.getLogger(RoundRobinBufferTest.class);

    /**
     * Basically just tests that this does FIFO.
     * Kind of silly.
     */
    @Test
    public void doTest() throws InterruptedException {
        // numberOfVSpoutIds * numberOfMessagesPer should be less than the configured
        // max buffer size.
        final int numberOfVSpoutIds = 5;
        final int numberOfMessagesPer = 1500;

        // Create buffer
        TupleBuffer tupleBuffer = new FIFOBuffer();

        // Keep track of our order
        final List<KafkaMessage> submittedOrder = Lists.newArrayList();

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
            submittedOrder.add(kafkaMessage);

            // Add to our buffer
            tupleBuffer.put(kafkaMessage);
        }

        // Now pop them, order should be maintained
        for (KafkaMessage originalKafkaMsg: submittedOrder) {
            final KafkaMessage bufferedMsg = tupleBuffer.poll();

            // Validate it
            assertNotNull("Should not be null", bufferedMsg);
            assertEquals("Objects should be the same", originalKafkaMsg, bufferedMsg);

            // Validate the contents are the same
            assertEquals("Source Spout Id should be equal", originalKafkaMsg.getTupleMessageId().getSrcConsumerId(), bufferedMsg.getTupleMessageId().getSrcConsumerId());
            assertEquals("partitions should be equal", originalKafkaMsg.getPartition(), bufferedMsg.getPartition());
            assertEquals("offsets should be equal", originalKafkaMsg.getOffset(), bufferedMsg.getOffset());
            assertEquals("topic should be equal", originalKafkaMsg.getTopic(), bufferedMsg.getTopic());
            assertEquals("TupleMessageIds should be equal", originalKafkaMsg.getTupleMessageId(), bufferedMsg.getTupleMessageId());
            assertEquals("Values should be equal", originalKafkaMsg.getValues(), bufferedMsg.getValues());
        }

        // Validate that the next polls are all null
        for (int x=0; x<64; x++) {
            assertNull("Should be null", tupleBuffer.poll());
        }
    }

    /**
     * Tests this instance against multiple threads for weirdness.
     */
    @Test
    public void testConcurrentModification() throws InterruptedException {
        // Settings for test
        final int numberOfVSpoutIds = 10;
        final int numberOfMessagesPer = 2500;
        final int numberOfThreads = 10;

        // Create buffer
        final TupleBuffer tupleBuffer = new FIFOBuffer();

        // Create some threads
        List<Future> futures = Lists.newArrayList();
        for (int threadCount=0; threadCount<numberOfThreads; threadCount++) {
            futures.add(CompletableFuture.runAsync(() -> {
                final Random random = new Random();

                for (int x=0; x<(numberOfMessagesPer * numberOfVSpoutIds); x++) {
                    // Generate source spout id
                    final String sourceSpoutId = "srcSpoutId" + random.nextInt(numberOfVSpoutIds);
                    final int partition = random.nextInt(10);


                    KafkaMessage kafkaMessage = new KafkaMessage(
                            new TupleMessageId("my topic", partition, x, sourceSpoutId),
                            new Values("poop" + x));

                    try {
                        tupleBuffer.put(kafkaMessage);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                logger.info("Thread finished");
            }));
        }

        // Create a thread to read from it.
        int totalFound = 0;
        int totalExpected = numberOfMessagesPer * numberOfVSpoutIds * numberOfThreads;
        logger.info("Expecting to find {} messages", totalExpected);
        while (totalFound != totalExpected) {
            if (tupleBuffer.poll() != null) {
                totalFound++;
            }
        }
        logger.info("Found {} messages", totalFound);

        // Wait for futures to be complete
        await()
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> {
                    // Validate the futures are completed
                    for (Future future: futures) {
                        if (!future.isDone()) {
                            return false;
                        }
                    }
                    // All futures have completed.
                    return true;
                }, equalTo(true));


        // Poll a bunch, validating nothing else returned
        for (int x=0; x<1024; x++) {
            assertNull("Should be null", tupleBuffer.poll());
        }
    }
}