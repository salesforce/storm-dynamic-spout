package com.salesforce.storm.spout.sideline.coordinator;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;
import com.salesforce.storm.spout.sideline.mocks.MockDelegateSidelineSpout;
import com.salesforce.storm.spout.sideline.tupleBuffer.FIFOBuffer;
import com.salesforce.storm.spout.sideline.tupleBuffer.TupleBuffer;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(DataProviderRunner.class)
public class SpoutRunnerTest {
    private static final Logger logger = LoggerFactory.getLogger(SpoutRunnerTest.class);
    private static final int maxWaitTime = 5;

    private ThreadPoolExecutor executorService;

    @After
    public void shutDown() throws InterruptedException {
        // Shut down our executor service if it exists
        if (executorService == null) {
            return;
        }
        executorService.shutdown();
        try {
            executorService.awaitTermination(maxWaitTime, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (!executorService.isTerminated()) {
            executorService.shutdownNow();
        }
    }

    /**
     * Tests the constructor sets things appropriately.
     */
    @Test
    public void testConstructor() {
        // Define inputs
        final TupleBuffer tupleBuffer = new FIFOBuffer();
        final Map<String, Queue<TupleMessageId>> ackQueue = Maps.newConcurrentMap();
        final Map<String, Queue<TupleMessageId>> failQueue = Maps.newConcurrentMap();
        final CountDownLatch latch = new CountDownLatch(1);
        final Clock clock = Clock.systemUTC();

        // Define some config params
        final long consumerStateFlushInterval = 200L;

        // Create our spout delegate
        final DelegateSidelineSpout spout = mock(DelegateSidelineSpout.class);

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            spout,
            tupleBuffer,
            ackQueue,
            failQueue,
            latch,
            clock,
            topologyConfig
        );

        // Call getters and validate!
        assertEquals("Clock instance is what we expect", clock, spoutRunner.getClock());
        assertEquals("TopologyConfig looks legit", topologyConfig, spoutRunner.getTopologyConfig());
        assertEquals("getConsumerStateFlushIntervalMs() returns right value", consumerStateFlushInterval, spoutRunner.getConsumerStateFlushIntervalMs());
        assertEquals("Tuple Buffer got set", tupleBuffer, spoutRunner.getTupleQueue());
        assertEquals("Ack Queue got set", ackQueue, spoutRunner.getAckedTupleQueue());
        assertEquals("Fail Queue got set", failQueue, spoutRunner.getFailedTupleQueue());
        assertEquals("Latch got set", latch, spoutRunner.getLatch());
        assertNotNull("StartTime is null", spoutRunner.getStartTime());
        assertEquals("Spout delegate got set", spout, spoutRunner.getSpout());
    }

    /**
     * Tests that
     *   - our latch counts down when we start the thread.
     *   - open() is called on the spout on startup.
     *   - close() is called on the spout on shutdown.
     *   - spoutRunner.requestStop() shuts down the instance.
     *   - spoutDelegate.requestStop() shuts down the instance.
     */
    @Test
    @UseDataProvider("provideTrueAndFalse")
    public void testOpenandCloseOnSpoutIsCalled(final boolean shutdownViaSpout) throws InterruptedException {
        // Define inputs
        final CountDownLatch latch = new CountDownLatch(1);
        final Clock clock = Clock.systemUTC();
        final String virtualSpoutId = "MyVirtualSpoutId";

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSidelineSpout mockSpout = new MockDelegateSidelineSpout(virtualSpoutId);

        // Create our queues
        final TupleBuffer tupleBuffer = mock(TupleBuffer.class);

        // Setup mock ack queue
        final Map<String, Queue<TupleMessageId>> ackQueue = mock(Map.class);
        when(ackQueue.get(eq(virtualSpoutId))).thenReturn(new LinkedBlockingQueue<>());

        // Setup mock fail queue
        final Map<String, Queue<TupleMessageId>> failQueue = mock(Map.class);
        when(failQueue.get(eq(virtualSpoutId))).thenReturn(new LinkedBlockingQueue<>());

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            tupleBuffer,
            ackQueue,
            failQueue,
            latch,
            clock,
            topologyConfig
        );

        // Sanity test
        assertEquals("Latch has value of 1", 1, latch.getCount());

        // Start in a separate thread.
        CompletableFuture future = startSpoutRunner(spoutRunner);

        // Wait for latch to count down to 0
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> {
                return latch.getCount() == 0;
            }, equalTo(true));

        // sanity test
        assertEquals("Latch has value of 0", 0, latch.getCount());

        // Verify open was called once, but not close
        assertTrue("Open was called on our mock spout", mockSpout.wasOpenCalled);
        assertFalse("Close has not been called yet on our mock spout", mockSpout.wasCloseCalled);

        // Verify queues got setup
        verify(tupleBuffer, times(1)).addVirtualSpoutId(eq(virtualSpoutId));
        verify(ackQueue, times(1)).put(eq(virtualSpoutId), any(Queue.class));
        verify(failQueue, times(1)).put(eq(virtualSpoutId), any(Queue.class));

        // But not torn down yet
        verify(tupleBuffer, never()).removeVirtualSpoutId(anyString());
        verify(ackQueue, never()).remove(anyString());
        verify(failQueue, never()).remove(anyString());

        // Shut down
        if (shutdownViaSpout) {
            logger.info("Requesting stop via Spout.requestStop()");
            mockSpout.requestStop();
        } else {
            logger.info("Requesting stop via SpoutRunner.requestStop()");
            spoutRunner.requestStop();
        }

        // Wait for thread to stop.
        await()
            .atMost(maxWaitTime * 2, TimeUnit.SECONDS)
            .until(future::isDone);

        // Make sure it actually stopped
        assertEquals("Should have no running threads", 0, executorService.getActiveCount());

        // verify close was called
        assertTrue("Close was called on our mock spout", mockSpout.wasCloseCalled);

        // Verify entries removed from tupleBuffer, ackQueue, failQueue
        verify(tupleBuffer, times(1)).removeVirtualSpoutId(eq(virtualSpoutId));
        verify(ackQueue, times(1)).remove(eq(virtualSpoutId));
        verify(failQueue, times(1)).remove(eq(virtualSpoutId));
    }

    /**
     * Provides various tuple buffer implementation.
     */
    @DataProvider
    public static Object[][] provideTrueAndFalse() {
        return new Object[][]{
            { true },
            { false }
        };
    }

    // Test tuples get put into buffer

    // Test fails get sent to spout

    // test acks get sent to spout

    // test flushState gets called on spout periodically

    private Map<String, Object> getDefaultConfig(long consumerStateFlushIntervalMs) {
        final Map<String, Object> topologyConfig = SidelineSpoutConfig.setDefaults(Maps.newHashMap());
        topologyConfig.put(SidelineSpoutConfig.CONSUMER_STATE_FLUSH_INTERVAL_MS, consumerStateFlushIntervalMs);
        return topologyConfig;
    }

    private CompletableFuture startSpoutRunner(SpoutRunner spoutRunner) {
        if (executorService == null) {
            executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        }

        // Sanity check
        assertEquals("Executor service should be empty", 0, executorService.getActiveCount());

        // Submit task to start
        CompletableFuture future = CompletableFuture.runAsync(spoutRunner, executorService);

        // Wait until it actually starts.
        await()
                .atMost(maxWaitTime, TimeUnit.SECONDS)
                .until(() -> {
                    return executorService.getActiveCount() == 1;
                }, equalTo(true));

        // return the future
        return future;
    }
}