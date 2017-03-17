package com.salesforce.storm.spout.sideline.tupleBuffer;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import org.apache.storm.tuple.Values;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class RoundRobinBufferTest {
    private static final Logger logger = LoggerFactory.getLogger(RoundRobinBufferTest.class);

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

        // Create buffer
        TupleBuffer tupleBuffer = new RoundRobinBuffer();

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
        final TupleBuffer tupleBuffer = new RoundRobinBuffer();

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