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

package com.salesforce.storm.spout.dynamic.coordinator;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.Message;
import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.DelegateSpout;
import com.salesforce.storm.spout.dynamic.mocks.MockDelegateSpout;
import com.salesforce.storm.spout.dynamic.buffer.FifoBuffer;
import com.salesforce.storm.spout.dynamic.buffer.MessageBuffer;
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
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test that the {@link SpoutRunner} starts spouts when passed in and handles their lifecycle correctly.
 */
@RunWith(DataProviderRunner.class)
public class SpoutRunnerTest {

    private static final Logger logger = LoggerFactory.getLogger(SpoutRunnerTest.class);
    private static final int maxWaitTime = 5;

    private ThreadPoolExecutor executorService;

    /**
     * Shutdown the thread executor service when the test is all over.
     * @throws InterruptedException something went wrong.
     */
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
        final MessageBuffer messageBuffer = new FifoBuffer();
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> ackQueue = Maps.newConcurrentMap();
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> failQueue = Maps.newConcurrentMap();
        final CountDownLatch latch = new CountDownLatch(1);
        final Clock clock = Clock.systemUTC();

        // Define some config params
        final long consumerStateFlushInterval = 200L;

        // Create our spout delegate
        final DelegateSpout spout = mock(DelegateSpout.class);

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            spout,
            messageBuffer,
            ackQueue,
            failQueue,
            latch,
            clock,
            topologyConfig
        );

        // Call getters and validate!
        assertEquals("Clock instance is what we expect", clock, spoutRunner.getClock());
        assertEquals("TopologyConfig looks legit", topologyConfig, spoutRunner.getTopologyConfig());
        assertEquals(
            "getConsumerStateFlushIntervalMs() returns right value",
            consumerStateFlushInterval,
            spoutRunner.getConsumerStateFlushIntervalMs()
        );
        assertEquals("Tuple Buffer got set", messageBuffer, spoutRunner.getTupleQueue());
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
        final DefaultVirtualSpoutIdentifier virtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSpout mockSpout = new MockDelegateSpout(virtualSpoutId);

        // Create our queues
        final MessageBuffer messageBuffer = mock(MessageBuffer.class);

        // Setup mock ack queue
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> ackQueue = mock(Map.class);
        when(ackQueue.get(eq(virtualSpoutId))).thenReturn(new LinkedBlockingQueue<>());

        // Setup mock fail queue
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> failQueue = mock(Map.class);
        when(failQueue.get(eq(virtualSpoutId))).thenReturn(new LinkedBlockingQueue<>());

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            messageBuffer,
            ackQueue,
            failQueue,
            latch,
            clock,
            topologyConfig
        );

        // Sanity test
        assertEquals("Latch has value of 1", 1, latch.getCount());

        // Start in a separate thread.
        final CompletableFuture future = startSpoutRunner(spoutRunner);

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
        verify(messageBuffer, times(1)).addVirtualSpoutId(eq(virtualSpoutId));
        verify(ackQueue, times(1)).put(eq(virtualSpoutId), any(Queue.class));
        verify(failQueue, times(1)).put(eq(virtualSpoutId), any(Queue.class));

        // But not torn down yet
        verify(messageBuffer, never()).removeVirtualSpoutId(anyObject());
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

        // Verify entries removed from buffer, ackQueue, failQueue
        verify(messageBuffer, times(1)).removeVirtualSpoutId(eq(virtualSpoutId));
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
     * Tests that if a DelegateSpout has Messages, they get moved into
     * the MessageBuffer.
     */
    @Test
    public void testMessageBufferGetsFilled() {
        // Define inputs
        final CountDownLatch latch = new CountDownLatch(1);
        final Clock clock = Clock.systemUTC();
        final DefaultVirtualSpoutIdentifier virtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSpout mockSpout = new MockDelegateSpout(virtualSpoutId);

        // Create our queues
        final MessageBuffer messageBuffer = FifoBuffer.createDefaultInstance();

        // Setup mock ack queue
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> ackQueue = mock(Map.class);
        when(ackQueue.get(eq(virtualSpoutId))).thenReturn(new LinkedBlockingQueue<>());

        // Setup mock fail queue
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> failQueue = mock(Map.class);
        when(failQueue.get(eq(virtualSpoutId))).thenReturn(new LinkedBlockingQueue<>());

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            messageBuffer,
            ackQueue,
            failQueue,
            latch,
            clock,
            topologyConfig
        );

        // Sanity test
        assertEquals("Latch has value of 1", 1, latch.getCount());

        // Start in a separate thread.
        final CompletableFuture future = startSpoutRunner(spoutRunner);

        // Wait for latch to count down to 0
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> {
                return latch.getCount() == 0;
            }, equalTo(true));

        // sanity test
        assertEquals("Latch has value of 0", 0, latch.getCount());
        assertEquals("MessageBuffer should be empty", 0, messageBuffer.size());

        // Now Add some messages to our mock spout
        final Message message1 = new Message(new MessageId("namespace", 0, 0L, virtualSpoutId), new Values(1));
        final Message message2 = new Message(new MessageId("namespace", 0, 1L, virtualSpoutId), new Values(1));
        final Message message3 = new Message(new MessageId("namespace", 0, 2L, virtualSpoutId), new Values(1));
        mockSpout.emitQueue.add(message1);
        mockSpout.emitQueue.add(message2);
        mockSpout.emitQueue.add(message3);

        // Now wait for them to show up in our buffer
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> {
                return messageBuffer.size() == 3;
            }, equalTo(true));

        // Sanity test
        assertEquals("MessageBuffer should have 3 entries", 3, messageBuffer.size());

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
        final DefaultVirtualSpoutIdentifier virtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSpout mockSpout = new MockDelegateSpout(virtualSpoutId);

        // Create our queues
        final MessageBuffer messageBuffer = FifoBuffer.createDefaultInstance();
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> ackQueue = Maps.newConcurrentMap();
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> failQueue = Maps.newConcurrentMap();

        // Add other virtual spout id in ack and fail queues
        final VirtualSpoutIdentifier otherVirtualSpoutId = new DefaultVirtualSpoutIdentifier("OtherVirtualSpout");
        ackQueue.put(otherVirtualSpoutId, new ConcurrentLinkedQueue<>());
        failQueue.put(otherVirtualSpoutId, new ConcurrentLinkedQueue<>());

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            messageBuffer,
            ackQueue,
            failQueue,
            latch,
            clock,
            topologyConfig
        );

        // Sanity test
        assertEquals("Latch has value of 1", 1, latch.getCount());

        // Start in a separate thread.
        final CompletableFuture future = startSpoutRunner(spoutRunner);

        // Wait for latch to count down to 0
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> {
                return latch.getCount() == 0;
            }, equalTo(true));

        // sanity test
        assertEquals("Latch has value of 0", 0, latch.getCount());
        assertEquals("MessageBuffer should be empty", 0, messageBuffer.size());
        assertEquals("Ack Queue should be empty", 0, ackQueue.get(virtualSpoutId).size());
        assertEquals("fail Queue should be empty", 0, failQueue.get(virtualSpoutId).size());

        // Create some MessageIds for our virtualSpoutId
        final MessageId messageId1 = new MessageId("namespace", 0, 0L, virtualSpoutId);
        final MessageId messageId2 = new MessageId("namespace", 0, 1L, virtualSpoutId);
        final MessageId messageId3 = new MessageId("namespace", 0, 2L, virtualSpoutId);

        // Create some MessageIds for a different virtualSpoutId
        final MessageId messageId4 = new MessageId("namespace", 0, 0L, otherVirtualSpoutId);
        final MessageId messageId5 = new MessageId("namespace", 0, 1L, otherVirtualSpoutId);
        final MessageId messageId6 = new MessageId("namespace", 0, 2L, otherVirtualSpoutId);

        // Add them to the appropriate queues
        failQueue.get(virtualSpoutId).add(messageId1);
        failQueue.get(virtualSpoutId).add(messageId2);
        failQueue.get(virtualSpoutId).add(messageId3);
        failQueue.get(otherVirtualSpoutId).add(messageId4);
        failQueue.get(otherVirtualSpoutId).add(messageId5);
        failQueue.get(otherVirtualSpoutId).add(messageId6);

        // Now wait for them to show up in our spout instance
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> {
                return mockSpout.failedTupleIds.size() == 3;
            }, equalTo(true));

        // Sanity test
        assertEquals("failQueue should now be empty", 0, failQueue.get(virtualSpoutId).size());
        assertEquals("mock spout should have gotten 3 fail() calls", 3, mockSpout.failedTupleIds.size());
        assertTrue("Should have messageId", mockSpout.failedTupleIds.contains(messageId1));
        assertTrue("Should have messageId", mockSpout.failedTupleIds.contains(messageId2));
        assertTrue("Should have messageId", mockSpout.failedTupleIds.contains(messageId3));
        assertFalse("Should NOT have messageId", mockSpout.failedTupleIds.contains(messageId4));
        assertFalse("Should NOT have messageId", mockSpout.failedTupleIds.contains(messageId5));
        assertFalse("Should NOT have messageId", mockSpout.failedTupleIds.contains(messageId6));

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
        final VirtualSpoutIdentifier virtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSpout mockSpout = new MockDelegateSpout(virtualSpoutId);

        // Create our queues
        final MessageBuffer messageBuffer = FifoBuffer.createDefaultInstance();
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> ackQueue = Maps.newConcurrentMap();
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> failQueue = Maps.newConcurrentMap();

        // Add other virtual spout id in ack and fail queues
        final DefaultVirtualSpoutIdentifier otherVirtualSpoutId = new DefaultVirtualSpoutIdentifier("OtherVirtualSpout");
        ackQueue.put(otherVirtualSpoutId, new ConcurrentLinkedQueue<>());
        failQueue.put(otherVirtualSpoutId, new ConcurrentLinkedQueue<>());

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            messageBuffer,
            ackQueue,
            failQueue,
            latch,
            clock,
            topologyConfig
        );

        // Sanity test
        assertEquals("Latch has value of 1", 1, latch.getCount());

        // Start in a separate thread.
        final CompletableFuture future = startSpoutRunner(spoutRunner);

        // Wait for latch to count down to 0
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> {
                return latch.getCount() == 0;
            }, equalTo(true));

        // sanity test
        assertEquals("Latch has value of 0", 0, latch.getCount());
        assertEquals("MessageBuffer should be empty", 0, messageBuffer.size());
        assertEquals("Ack Queue should be empty", 0, ackQueue.get(virtualSpoutId).size());
        assertEquals("fail Queue should be empty", 0, failQueue.get(virtualSpoutId).size());

        // Create some MessageIds for our virtualSpoutId
        final MessageId messageId1 = new MessageId("namespace", 0, 0L, virtualSpoutId);
        final MessageId messageId2 = new MessageId("namespace", 0, 1L, virtualSpoutId);
        final MessageId messageId3 = new MessageId("namespace", 0, 2L, virtualSpoutId);

        // Create some MessageIds for a different virtualSpoutId
        final MessageId messageId4 = new MessageId("namespace", 0, 0L, otherVirtualSpoutId);
        final MessageId messageId5 = new MessageId("namespace", 0, 1L, otherVirtualSpoutId);
        final MessageId messageId6 = new MessageId("namespace", 0, 2L, otherVirtualSpoutId);

        // Add them to the appropriate queues
        ackQueue.get(virtualSpoutId).add(messageId1);
        ackQueue.get(virtualSpoutId).add(messageId2);
        ackQueue.get(virtualSpoutId).add(messageId3);
        ackQueue.get(otherVirtualSpoutId).add(messageId4);
        ackQueue.get(otherVirtualSpoutId).add(messageId5);
        ackQueue.get(otherVirtualSpoutId).add(messageId6);

        // Now wait for them to show up in our spout instance
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> {
                return mockSpout.ackedTupleIds.size() == 3;
            }, equalTo(true));

        // Sanity test
        assertEquals("ackQueue should now be empty", 0, ackQueue.get(virtualSpoutId).size());
        assertEquals("mock spout should have gotten 3 ack() calls", 3, mockSpout.ackedTupleIds.size());
        assertTrue("Should have messageId", mockSpout.ackedTupleIds.contains(messageId1));
        assertTrue("Should have messageId", mockSpout.ackedTupleIds.contains(messageId2));
        assertTrue("Should have messageId", mockSpout.ackedTupleIds.contains(messageId3));
        assertFalse("Should NOT have messageId", mockSpout.ackedTupleIds.contains(messageId4));
        assertFalse("Should NOT have messageId", mockSpout.ackedTupleIds.contains(messageId5));
        assertFalse("Should NOT have messageId", mockSpout.ackedTupleIds.contains(messageId6));

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
        final DefaultVirtualSpoutIdentifier virtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSpout mockSpout = new MockDelegateSpout(virtualSpoutId);

        // Create our queues
        final MessageBuffer messageBuffer = FifoBuffer.createDefaultInstance();
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> ackQueue = Maps.newConcurrentMap();
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> failQueue = Maps.newConcurrentMap();

        // Add other virtual spout id in ack and fail queues
        final DefaultVirtualSpoutIdentifier otherVirtualSpoutId = new DefaultVirtualSpoutIdentifier("OtherVirtualSpout");
        ackQueue.put(otherVirtualSpoutId, new ConcurrentLinkedQueue<>());
        failQueue.put(otherVirtualSpoutId, new ConcurrentLinkedQueue<>());

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            messageBuffer,
            ackQueue,
            failQueue,
            latch,
            clock,
            topologyConfig
        );

        // Sanity test
        assertEquals("Latch has value of 1", 1, latch.getCount());

        // Start in a separate thread.
        final CompletableFuture future = startSpoutRunner(spoutRunner);

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
        final Map<String, Object> topologyConfig = SpoutConfig.setDefaults(Maps.newHashMap());
        topologyConfig.put(SpoutConfig.CONSUMER_STATE_FLUSH_INTERVAL_MS, consumerStateFlushIntervalMs);
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