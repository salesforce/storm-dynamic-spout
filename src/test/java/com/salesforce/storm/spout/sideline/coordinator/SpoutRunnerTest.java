package com.salesforce.storm.spout.sideline.coordinator;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;
import com.salesforce.storm.spout.sideline.mocks.MockDelegateSidelineSpout;
import com.salesforce.storm.spout.sideline.tupleBuffer.FIFOBuffer;
import com.salesforce.storm.spout.sideline.tupleBuffer.TupleBuffer;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.storm.tuple.Values;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
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

    /**
     * Tests that if a DelegateSpout has KafkaMessages, they get moved into
     * the TupleBuffer.
     */
    @Test
    public void testTupleBufferGetsFilled() {
        // Define inputs
        final CountDownLatch latch = new CountDownLatch(1);
        final Clock clock = Clock.systemUTC();
        final String virtualSpoutId = "MyVirtualSpoutId";

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSidelineSpout mockSpout = new MockDelegateSidelineSpout(virtualSpoutId);

        // Create our queues
        final TupleBuffer tupleBuffer = FIFOBuffer.createDefaultInstance();

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
        assertEquals("TupleBuffer should be empty", 0, tupleBuffer.size());

        // Now Add some messages to our mock spout
        final KafkaMessage kafkaMessage1 = new KafkaMessage(new TupleMessageId("topic", 0, 0L, virtualSpoutId), new Values(1));
        final KafkaMessage kafkaMessage2 = new KafkaMessage(new TupleMessageId("topic", 0, 1L, virtualSpoutId), new Values(1));
        final KafkaMessage kafkaMessage3 = new KafkaMessage(new TupleMessageId("topic", 0, 2L, virtualSpoutId), new Values(1));
        mockSpout.emitQueue.add(kafkaMessage1);
        mockSpout.emitQueue.add(kafkaMessage2);
        mockSpout.emitQueue.add(kafkaMessage3);

        // Now wait for them to show up in our tupleBuffer
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> {
                return tupleBuffer.size() == 3;
            }, equalTo(true));

        // Sanity test
        assertEquals("TupleBuffer should have 3 entries", 3, tupleBuffer.size());

        logger.info("Requesting stop via SpoutRunner.requestStop()");
        spoutRunner.requestStop();

        // Wait for thread to stop.
        await()
            .atMost(maxWaitTime * 2, TimeUnit.SECONDS)
            .until(future::isDone);

        // Make sure it actually stopped
        assertEquals("Should have no running threads", 0, executorService.getActiveCount());

        // verify close was called
        assertTrue("Close was called on our mock spout", mockSpout.wasCloseCalled);
    }

    /**
     * Tests that fails get sent to the spout instance.
     */
    @Test
    public void testFailsGetSentToSpout() {
        // Define inputs
        final CountDownLatch latch = new CountDownLatch(1);
        final Clock clock = Clock.systemUTC();
        final String virtualSpoutId = "MyVirtualSpoutId";

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSidelineSpout mockSpout = new MockDelegateSidelineSpout(virtualSpoutId);

        // Create our queues
        final TupleBuffer tupleBuffer = FIFOBuffer.createDefaultInstance();
        final Map<String, Queue<TupleMessageId>> ackQueue = Maps.newConcurrentMap();
        final Map<String, Queue<TupleMessageId>> failQueue = Maps.newConcurrentMap();

        // Add other virtual spout id in ack and fail queues
        final String otherVirtualSpoutId = "OtherVirtualSpout";
        ackQueue.put(otherVirtualSpoutId, new ConcurrentLinkedQueue<>());
        failQueue.put(otherVirtualSpoutId, new ConcurrentLinkedQueue<>());

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
        assertEquals("TupleBuffer should be empty", 0, tupleBuffer.size());
        assertEquals("Ack Queue should be empty", 0, ackQueue.get(virtualSpoutId).size());
        assertEquals("fail Queue should be empty", 0, failQueue.get(virtualSpoutId).size());

        // Create some TupleMessageIds for our virtualSpoutId
        final TupleMessageId tupleMessageId1 = new TupleMessageId("topic", 0, 0L, virtualSpoutId);
        final TupleMessageId tupleMessageId2 = new TupleMessageId("topic", 0, 1L, virtualSpoutId);
        final TupleMessageId tupleMessageId3 = new TupleMessageId("topic", 0, 2L, virtualSpoutId);

        // Create some TupleMessageIds for a different virtualSpoutId
        final TupleMessageId tupleMessageId4 = new TupleMessageId("topic", 0, 0L, otherVirtualSpoutId);
        final TupleMessageId tupleMessageId5 = new TupleMessageId("topic", 0, 1L, otherVirtualSpoutId);
        final TupleMessageId tupleMessageId6 = new TupleMessageId("topic", 0, 2L, otherVirtualSpoutId);

        // Add them to the appropriate queues
        failQueue.get(virtualSpoutId).add(tupleMessageId1);
        failQueue.get(virtualSpoutId).add(tupleMessageId2);
        failQueue.get(virtualSpoutId).add(tupleMessageId3);
        failQueue.get(otherVirtualSpoutId).add(tupleMessageId4);
        failQueue.get(otherVirtualSpoutId).add(tupleMessageId5);
        failQueue.get(otherVirtualSpoutId).add(tupleMessageId6);

        // Now wait for them to show up in our spout instance
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> {
                return mockSpout.failedTupleIds.size() == 3;
            }, equalTo(true));

        // Sanity test
        assertEquals("failQueue should now be empty", 0, failQueue.get(virtualSpoutId).size());
        assertEquals("mock spout should have gotten 3 fail() calls", 3, mockSpout.failedTupleIds.size());
        assertTrue("Should have tupleMessageId", mockSpout.failedTupleIds.contains(tupleMessageId1));
        assertTrue("Should have tupleMessageId", mockSpout.failedTupleIds.contains(tupleMessageId2));
        assertTrue("Should have tupleMessageId", mockSpout.failedTupleIds.contains(tupleMessageId3));
        assertFalse("Should NOT have tupleMessageId", mockSpout.failedTupleIds.contains(tupleMessageId4));
        assertFalse("Should NOT have tupleMessageId", mockSpout.failedTupleIds.contains(tupleMessageId5));
        assertFalse("Should NOT have tupleMessageId", mockSpout.failedTupleIds.contains(tupleMessageId6));

        // Other virtualspout id queue should still be populated
        assertEquals("fail queue for other virtual spout should remain full", 3, failQueue.get(otherVirtualSpoutId).size());

        // No failed ids
        assertTrue("acked() never called", mockSpout.ackedTupleIds.isEmpty());

        logger.info("Requesting stop via SpoutRunner.requestStop()");
        spoutRunner.requestStop();

        // Wait for thread to stop.
        await()
            .atMost(maxWaitTime * 2, TimeUnit.SECONDS)
            .until(future::isDone);

        // Make sure it actually stopped
        assertEquals("Should have no running threads", 0, executorService.getActiveCount());

        // verify close was called
        assertTrue("Close was called on our mock spout", mockSpout.wasCloseCalled);
    }

    /**
     * Tests that acks get sent to the spout instance.
     */
    @Test
    public void testAcksGetSentToSpout() {
        // Define inputs
        final CountDownLatch latch = new CountDownLatch(1);
        final Clock clock = Clock.systemUTC();
        final String virtualSpoutId = "MyVirtualSpoutId";

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSidelineSpout mockSpout = new MockDelegateSidelineSpout(virtualSpoutId);

        // Create our queues
        final TupleBuffer tupleBuffer = FIFOBuffer.createDefaultInstance();
        final Map<String, Queue<TupleMessageId>> ackQueue = Maps.newConcurrentMap();
        final Map<String, Queue<TupleMessageId>> failQueue = Maps.newConcurrentMap();

        // Add other virtual spout id in ack and fail queues
        final String otherVirtualSpoutId = "OtherVirtualSpout";
        ackQueue.put(otherVirtualSpoutId, new ConcurrentLinkedQueue<>());
        failQueue.put(otherVirtualSpoutId, new ConcurrentLinkedQueue<>());

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
        assertEquals("TupleBuffer should be empty", 0, tupleBuffer.size());
        assertEquals("Ack Queue should be empty", 0, ackQueue.get(virtualSpoutId).size());
        assertEquals("fail Queue should be empty", 0, failQueue.get(virtualSpoutId).size());

        // Create some TupleMessageIds for our virtualSpoutId
        final TupleMessageId tupleMessageId1 = new TupleMessageId("topic", 0, 0L, virtualSpoutId);
        final TupleMessageId tupleMessageId2 = new TupleMessageId("topic", 0, 1L, virtualSpoutId);
        final TupleMessageId tupleMessageId3 = new TupleMessageId("topic", 0, 2L, virtualSpoutId);

        // Create some TupleMessageIds for a different virtualSpoutId
        final TupleMessageId tupleMessageId4 = new TupleMessageId("topic", 0, 0L, otherVirtualSpoutId);
        final TupleMessageId tupleMessageId5 = new TupleMessageId("topic", 0, 1L, otherVirtualSpoutId);
        final TupleMessageId tupleMessageId6 = new TupleMessageId("topic", 0, 2L, otherVirtualSpoutId);

        // Add them to the appropriate queues
        ackQueue.get(virtualSpoutId).add(tupleMessageId1);
        ackQueue.get(virtualSpoutId).add(tupleMessageId2);
        ackQueue.get(virtualSpoutId).add(tupleMessageId3);
        ackQueue.get(otherVirtualSpoutId).add(tupleMessageId4);
        ackQueue.get(otherVirtualSpoutId).add(tupleMessageId5);
        ackQueue.get(otherVirtualSpoutId).add(tupleMessageId6);

        // Now wait for them to show up in our spout instance
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> {
                return mockSpout.ackedTupleIds.size() == 3;
            }, equalTo(true));

        // Sanity test
        assertEquals("ackQueue should now be empty", 0, ackQueue.get(virtualSpoutId).size());
        assertEquals("mock spout should have gotten 3 ack() calls", 3, mockSpout.ackedTupleIds.size());
        assertTrue("Should have tupleMessageId", mockSpout.ackedTupleIds.contains(tupleMessageId1));
        assertTrue("Should have tupleMessageId", mockSpout.ackedTupleIds.contains(tupleMessageId2));
        assertTrue("Should have tupleMessageId", mockSpout.ackedTupleIds.contains(tupleMessageId3));
        assertFalse("Should NOT have tupleMessageId", mockSpout.ackedTupleIds.contains(tupleMessageId4));
        assertFalse("Should NOT have tupleMessageId", mockSpout.ackedTupleIds.contains(tupleMessageId5));
        assertFalse("Should NOT have tupleMessageId", mockSpout.ackedTupleIds.contains(tupleMessageId6));

        // Other virtualspout id queue should still be populated
        assertEquals("ack queue for other virtual spout should remain full", 3, ackQueue.get(otherVirtualSpoutId).size());

        // No failed ids
        assertTrue("Failed() never called", mockSpout.failedTupleIds.isEmpty());

        logger.info("Requesting stop via SpoutRunner.requestStop()");
        spoutRunner.requestStop();

        // Wait for thread to stop.
        await()
                .atMost(maxWaitTime * 2, TimeUnit.SECONDS)
                .until(future::isDone);

        // Make sure it actually stopped
        assertEquals("Should have no running threads", 0, executorService.getActiveCount());

        // verify close was called
        assertTrue("Close was called on our mock spout", mockSpout.wasCloseCalled);
    }

    /**
     * Tests that flushState() gets called on spout periodically.
     */
    @Test
    public void testFlushStateGetsCalled() {
        // Define inputs
        final CountDownLatch latch = new CountDownLatch(1);
        final Clock clock = Clock.systemUTC();
        final String virtualSpoutId = "MyVirtualSpoutId";

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSidelineSpout mockSpout = new MockDelegateSidelineSpout(virtualSpoutId);

        // Create our queues
        final TupleBuffer tupleBuffer = FIFOBuffer.createDefaultInstance();
        final Map<String, Queue<TupleMessageId>> ackQueue = Maps.newConcurrentMap();
        final Map<String, Queue<TupleMessageId>> failQueue = Maps.newConcurrentMap();

        // Add other virtual spout id in ack and fail queues
        final String otherVirtualSpoutId = "OtherVirtualSpout";
        ackQueue.put(otherVirtualSpoutId, new ConcurrentLinkedQueue<>());
        failQueue.put(otherVirtualSpoutId, new ConcurrentLinkedQueue<>());

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

        // Wait for flush state to get called
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> {
                return mockSpout.flushStateCalled;
            }, equalTo(true));


        logger.info("Requesting stop via SpoutRunner.requestStop()");
        spoutRunner.requestStop();

        // Wait for thread to stop.
        await()
                .atMost(maxWaitTime * 2, TimeUnit.SECONDS)
                .until(future::isDone);

        // Make sure it actually stopped
        assertEquals("Should have no running threads", 0, executorService.getActiveCount());

        // verify close was called
        assertTrue("Close was called on our mock spout", mockSpout.wasCloseCalled);
    }

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