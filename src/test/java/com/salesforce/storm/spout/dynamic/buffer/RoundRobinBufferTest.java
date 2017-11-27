/**
 * Copyright (c) 2017, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 *   disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * * Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or promote products
 *   derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.salesforce.storm.spout.dynamic.buffer;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.dynamic.Message;
import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import com.salesforce.storm.spout.dynamic.config.DynamicSpoutConfig;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.storm.tuple.Values;
import org.junit.Test;
import org.junit.runner.RunWith;
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

/**
 * Test that {@link RoundRobinBufferTest} equally distributes messages from the buffer.
 */
@RunWith(DataProviderRunner.class)
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
        config.put(DynamicSpoutConfig.TUPLE_BUFFER_MAX_SIZE, maxBufferSize);
        final SpoutConfig spoutConfig = new SpoutConfig(new ConfigDefinition(), config);

        // Create buffer
        MessageBuffer messageBuffer = new RoundRobinBuffer();
        messageBuffer.open(spoutConfig);

        // Keep track of our order per spoutId
        final Map<DefaultVirtualSpoutIdentifier, Queue<Message>> submittedOrder = Maps.newHashMap();

        // Create random number generator
        Random random = new Random();

        // Generate messages
        for (int x = 0; x < (numberOfMessagesPer * numberOfVSpoutIds); x++) {
            // Generate source spout id
            final DefaultVirtualSpoutIdentifier sourceSpoutId = new DefaultVirtualSpoutIdentifier(
                "srcSpoutId" + random.nextInt(numberOfVSpoutIds)
            );
            final int partition = random.nextInt(10);


            Message message = new Message(
                new MessageId("my namespace", partition, x, sourceSpoutId),
                new Values("myValue" + x)
            );

            // Keep track of order
            if (!submittedOrder.containsKey(sourceSpoutId)) {
                submittedOrder.put(sourceSpoutId, Queues.newLinkedBlockingQueue());
            }
            submittedOrder.get(sourceSpoutId).add(message);

            // Add to our buffer
            messageBuffer.put(message);
        }

        // Validate size
        assertEquals("Size should be known", (numberOfMessagesPer * numberOfVSpoutIds), messageBuffer.size());

        // Now ask for the messages back, they should get round robin'd
        Iterator<DefaultVirtualSpoutIdentifier> keyIterator = Iterators.cycle(submittedOrder.keySet());
        for (int x = 0; x < (numberOfMessagesPer * numberOfVSpoutIds); x++) {
            DefaultVirtualSpoutIdentifier nextSourceSpout = keyIterator.next();

            // Pop a msg
            final Message bufferedMsg = messageBuffer.poll();

            // Skip null returns.
            if (bufferedMsg == null) {
                // decrement x so we don't skip an iteration.
                x--;
                continue;
            }

            // Get which spout this was a source from
            final VirtualSpoutIdentifier bufferedSrcSpoutId = bufferedMsg.getMessageId().getSrcVirtualSpoutId();

            // Get the next message from this
            Message nextExpectedKafkaMsg = null;
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
            assertEquals("Source Spout Id should be equal", nextSourceSpout, bufferedMsg.getMessageId().getSrcVirtualSpoutId());


            // Validate the contents are the same
            assertEquals("partitions should be equal", nextExpectedKafkaMsg.getPartition(), bufferedMsg.getPartition());
            assertEquals("offsets should be equal", nextExpectedKafkaMsg.getOffset(), bufferedMsg.getOffset());
            assertEquals("namespace should be equal", nextExpectedKafkaMsg.getNamespace(), bufferedMsg.getNamespace());
            assertEquals("MessageIds should be equal", nextExpectedKafkaMsg.getMessageId(), bufferedMsg.getMessageId());
            assertEquals("Values should be equal", nextExpectedKafkaMsg.getValues(), bufferedMsg.getValues());
        }


        // Validate that the next polls are all null
        for (int x = 0; x < 128; x++) {
            assertNull("Should be null", messageBuffer.poll());
        }
    }

    /**
     * Makes sure that we can properly parse long config values on open().
     */
    @Test
    @UseDataProvider("provideConfigObjects")
    public void testConstructorWithConfigValue(Number inputValue) throws InterruptedException {
        logger.info("Testing with object type {} and value {}", inputValue.getClass().getSimpleName(), inputValue);

        // Create config
        Map<String, Object> config = Maps.newHashMap();
        config.put(DynamicSpoutConfig.TUPLE_BUFFER_MAX_SIZE, inputValue);
        final SpoutConfig spoutConfig = new SpoutConfig(new ConfigDefinition(), config);

        // Create buffer
        RoundRobinBuffer messageBuffer = new RoundRobinBuffer();
        messageBuffer.open(spoutConfig);

        // Validate
        assertEquals("Set correct", inputValue.intValue(), messageBuffer.getMaxBufferSizePerVirtualSpout());
    }

    /**
     * Provides various tuple buffer implementation.
     */
    @DataProvider
    public static Object[][] provideConfigObjects() throws InstantiationException, IllegalAccessException {
        return new Object[][]{
                // Integer
                { 200 },

                // Long
                { 2000L }
        };
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
            Iterator<String> keyIterator = Iterators.cycle(myMap.keySet());
            while (keyIterator.hasNext()) {
                final String keyValue = keyIterator.next();
                if (keyValue.equals("Key5")) {
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
        for (int x = 5; x < 100; x++) {
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
        for (int x = 5; x < 100; x++) {
            myMap.remove("Key" + x);
        }

        // Make sure nothing bad happened
        assertFalse("No exceptions thrown", future.isCompletedExceptionally());
        future.cancel(true);
        try {
            future.get();
        } catch (CancellationException e) {
            // We expect an exception because we cancelled it. swallow.
        }
    }
}