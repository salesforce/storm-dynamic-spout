package com.salesforce.storm.spout.sideline.tupleBuffer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
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

@RunWith(DataProviderRunner.class)
public class TupleBufferTest {

    private static final Logger logger = LoggerFactory.getLogger(RoundRobinBufferTest.class);

    /**
     * Provides various tuple buffer implementation.
     */
    @DataProvider
    public static Object[][] provideTupleBuffers() throws InstantiationException, IllegalAccessException {
        return new Object[][]{
                { createInstance(FIFOBuffer.class, 10000) },
                { createInstance(RoundRobinBuffer.class, 1000 ) },
        };
    }

    /**
     * Helper method for creating instances and configuring them.
     */
    private static TupleBuffer createInstance(Class clazz, int bufferSize) throws IllegalAccessException, InstantiationException {
        Map<String, Object> topologyConfig = Maps.newHashMap();
        topologyConfig.put(SidelineSpoutConfig.TUPLE_BUFFER_MAX_SIZE, bufferSize);

        TupleBuffer tupleBuffer = (TupleBuffer) clazz.newInstance();
        tupleBuffer.open(topologyConfig);

        return tupleBuffer;
    }

    /**
     * Tests this instance against multiple threads for weirdness.
     */
    @Test
    @UseDataProvider("provideTupleBuffers")
    public void testConcurrentModification(final TupleBuffer tupleBuffer) throws InterruptedException {
        // Settings for test
        final int numberOfVSpoutIds = 10;
        final int numberOfMessagesPer = 100000;
        final int numberOfThreads = 15;

        logger.info("Running test with {}", tupleBuffer.getClass().getSimpleName());

        // Create executor service with number of threads = how many we want to run
        final ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

        // Create some threads
        final List<CompletableFuture> futures = Lists.newArrayList();
        final CountDownLatch countDownLatch = new CountDownLatch(numberOfThreads);
        for (int threadCount=0; threadCount<numberOfThreads; threadCount++) {
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
                for (int x=0; x<(numberOfMessagesPer * numberOfVSpoutIds); x++) {
                    // Generate source spout id
                    final String sourceSpoutId = "srcSpoutId" + random.nextInt(numberOfVSpoutIds);
                    final int partition = random.nextInt(10);


                    KafkaMessage kafkaMessage = new KafkaMessage(
                            new TupleMessageId("my topic", partition, x, sourceSpoutId),
                            new Values("poop" + x));

                    try {
                        final long startPutTime = System.currentTimeMillis();
                        tupleBuffer.put(kafkaMessage);
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
            if (tupleBuffer.poll() != null) {
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

        // Wait for futures to be complete, at this point htey should already be done tho.
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