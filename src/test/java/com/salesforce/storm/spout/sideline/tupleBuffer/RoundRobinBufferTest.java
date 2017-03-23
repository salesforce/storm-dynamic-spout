package com.salesforce.storm.spout.sideline.tupleBuffer;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
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
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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

        // Validate size
        assertEquals("Size should be known", (numberOfMessagesPer * numberOfVSpoutIds), tupleBuffer.size());

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
            final String bufferedSrcSpoutId = bufferedMsg.getTupleMessageId().getSrcVirtualSpoutId();

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
            assertEquals("Source Spout Id should be equal", nextSourceSpout, bufferedMsg.getTupleMessageId().getSrcVirtualSpoutId());


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
 *  The following tests are just sanity checks around Iterators.cycle()'s behavior.
 *  Currently we don't make use of it in our implementation, but it looks like we could
 *  if we wanted too.
 */

    /**
     * Just a sanity test of java behaviors.
     *  - Create a concurrent hashmap with 4 entries
     *  - Create a new thread that runs forever cycling over the keys
     *  - In main thread add a new key.
     *  - See what happens to the thread cycling over keys, it should show up.
     */
    @Test
    public void testBehaviorOfIteratorsCycleWhenAddingNewKey() throws ExecutionException, InterruptedException {
        final Map<String, String> myMap = Maps.newConcurrentMap();
        myMap.put("Key1", "Value1");
        myMap.put("Key2", "Value2");
        myMap.put("Key3", "Value3");
        myMap.put("Key4", "Value4");

        // Create new Thread
        final CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            System.out.println("Creating new iterator");
            Iterator<String> keyIterator = Iterators.cycle(myMap.keySet());
            while (keyIterator.hasNext()) {
                final String keyValue = keyIterator.next();
                if (keyValue.equals("Key5")) {
                    System.out.println("Found new key! " + keyValue);
                    return keyValue;
                }
            }
            return "Nada";
        });

        // Wait for a few iterations in the other thread have completed.
        Thread.sleep(1000L);

        // Modify the keys, adding a new one
        myMap.put("Key5", "Value5");

        // Make sure we get it.
        await()
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> {
                    // Ask for next tuple
                    return future.getNow("NotYet");
                }, equalTo("Key5"));

    }

    /**
     * Just a sanity test of java behaviors.
     *  - Create a concurrent hashmap with 4 entries
     *  - Create a new thread that runs forever cycling over the keys
     *  - In main thread add remove a key.
     *  - See what happens to the thread cycling over keys, it should get removed.
     */
    @Test
    public void testBehaviorOfIteratorsCycleWhenRemovingKey() throws ExecutionException, InterruptedException {
        final Map<String, String> myMap = Maps.newConcurrentMap();
        myMap.put("Key1", "Value1");
        myMap.put("Key2", "Value2");
        myMap.put("Key3", "Value3");
        myMap.put("Key4", "Value4");

        // Create new Thread
        final CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
            System.out.println("Creating new iterator");
            Iterator<String> keyIterator = Iterators.cycle(myMap.keySet());
            int lastFound = 0;
            while (keyIterator.hasNext()) {
                final String keyValue = keyIterator.next();
                if (keyValue.equals("Key4")) {
                    lastFound = 0;
                } else {
                    lastFound++;
                }
                // If we haven't seen this key in awhile
                if (lastFound >= 12) {
                    System.out.println("Hey Key4 is missing!");
                    return true;
                }
            }
            return false;
        });

        // Wait for a few iterations in the other thread have completed.
        Thread.sleep(1000L);

        // Modify the keys, removing Key4
        myMap.remove("Key4");

        // Make sure we get it.
        await()
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> {
                    // Ask for next tuple
                    return future.getNow(false);
                }, equalTo(true));

    }

    /**
     * Just a sanity test of java behaviors.
     *  - Create a concurrent hashmap with 4 entries
     *  - Create a new thread that runs forever cycling over the keys
     *  - In main thread add/remove a bunch of keys
     *  - See that nothing bad happens
     */
    @Test
    public void testBehaviorOfIteratorsCycleConcurrentlyModifyingUnderlyingMap() throws ExecutionException, InterruptedException {
        final Map<String, String> myMap = Maps.newConcurrentMap();
        myMap.put("Key1", "Value1");
        myMap.put("Key2", "Value2");
        myMap.put("Key3", "Value3");
        myMap.put("Key4", "Value4");

        final Set<String> foundKeys = Sets.newConcurrentHashSet();

        // Create new Thread
        final CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
            System.out.println("Creating new iterator");
            Iterator<String> keyIterator = Iterators.cycle(myMap.keySet());
            while (keyIterator.hasNext()) {
                final String keyValue = keyIterator.next();
                if (!foundKeys.contains(keyValue)) {
                    foundKeys.add(keyValue);
                }
            }
            return false;
        });

        // Wait for a few iterations in the other thread have completed.
        Thread.sleep(200L);

        // Add keys
        for (int x=5; x<100; x++) {
            myMap.put("Key" + x, "Value" + x);
        }

        // Sleep for a few
        await()
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> {
                return foundKeys.size();
            }, equalTo(myMap.size()));


        // See that we found all of our keys
        assertEquals("Found all keys", myMap.size(), foundKeys.size());

        // Now remove some keys
        for (int x=5; x<100; x++) {
            myMap.remove("Key" + x);
        }

        // Make sure nothing bad happened
        assertFalse("No exceptions thrown", future.isCompletedExceptionally());
        future.cancel(true);
        try {
            future.get();
        } catch (CancellationException e) {
            e.printStackTrace();
        }
    }
}