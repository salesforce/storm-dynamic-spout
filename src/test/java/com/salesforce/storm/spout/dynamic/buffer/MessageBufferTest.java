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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.Message;
import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.storm.tuple.Values;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNull;

/**
 * Generally test the {@link MessageBuffer} interface's surface area.
 */
@RunWith(DataProviderRunner.class)
public class MessageBufferTest {

    private static final Logger logger = LoggerFactory.getLogger(MessageBufferTest.class);

    /**
     * Provides various tuple buffer implementation.
     */
    @DataProvider
    public static Object[][] provideMessageBuffers() throws InstantiationException, IllegalAccessException {
        return new Object[][]{
                { createInstance(FifoBuffer.class, 10000) },
                { createInstance(RoundRobinBuffer.class, 1000 ) },
        };
    }

    /**
     * Helper method for creating instances and configuring them.
     */
    private static MessageBuffer createInstance(Class clazz, int bufferSize) throws IllegalAccessException, InstantiationException {
        Map<String, Object> config = Maps.newHashMap();
        config.put(SpoutConfig.TUPLE_BUFFER_MAX_SIZE, bufferSize);
        final AbstractConfig spoutConfig = new AbstractConfig(new ConfigDefinition(), config);

        MessageBuffer messageBuffer = (MessageBuffer) clazz.newInstance();
        messageBuffer.open(spoutConfig);

        return messageBuffer;
    }

    /**
     * Tests this instance against multiple threads for weirdness.
     */
    @Test
    @UseDataProvider("provideMessageBuffers")
    public void testConcurrentModification(final MessageBuffer messageBuffer) throws InterruptedException {
        // Settings for test
        final int numberOfVSpoutIds = 10;
        final int numberOfMessagesPer = 100000;
        final int numberOfThreads = 15;

        logger.info("Running test with {}", messageBuffer.getClass().getSimpleName());

        // Create executor service with number of threads = how many we want to run
        final ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

        // Create some threads
        final List<CompletableFuture> futures = Lists.newArrayList();
        final CountDownLatch countDownLatch = new CountDownLatch(numberOfThreads);
        for (int threadCount = 0; threadCount < numberOfThreads; threadCount++) {
            futures.add(CompletableFuture.runAsync(() -> {
                final Random random = new Random();
                long longestTimeBlocked = 0L;

                // Use latch so all threads start at roughly the same time.
                countDownLatch.countDown();
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                final long startTime = System.currentTimeMillis();
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

                    try {
                        final long startPutTime = System.currentTimeMillis();
                        messageBuffer.put(message);
                        final long putTime = System.currentTimeMillis() - startPutTime;
                        if (putTime > longestTimeBlocked) {
                            longestTimeBlocked = putTime;
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                final long totalTime = System.currentTimeMillis() - startTime;
                logger.info("Thread finished in {} ms with longest blocked time {} ms", totalTime, longestTimeBlocked);
            }, executorService));
        }

        // Create a thread to read from it.
        int totalFound = 0;
        long totalNullsReturned = 0;
        int totalExpected = numberOfMessagesPer * numberOfVSpoutIds * numberOfThreads;
        logger.info("Expecting to find {} messages", totalExpected);
        while (totalFound != totalExpected) {
            if (messageBuffer.poll() != null) {
                totalFound++;
            } else {
                totalNullsReturned++;
            }

            // See if any problems with our futures
            for (CompletableFuture future: futures) {
                if (future.isCompletedExceptionally()) {
                    throw new RuntimeException("Failed to complete due to exception");
                }
            }

        }
        logger.info("Found {} messages with a total of {} nulls returned", totalFound, totalNullsReturned);

        // Wait for futures to be complete, at this point they should already be done tho.
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
        for (int x = 0; x < 1024; x++) {
            assertNull("Should be null", messageBuffer.poll());
        }

        // Shutdown executor service
        executorService.shutdown();
        executorService.awaitTermination(3, TimeUnit.SECONDS);
        if (!executorService.isShutdown()) {
            executorService.shutdownNow();
        }
    }
}