/*
 * Copyright (c) 2017, 2018, Salesforce.com, Inc.
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

import com.salesforce.storm.spout.dynamic.Message;
import com.salesforce.storm.spout.dynamic.MessageBus;
import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.VirtualSpoutMessageBus;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.DelegateSpout;
import com.salesforce.storm.spout.dynamic.mocks.MockDelegateSpout;
import com.salesforce.storm.spout.dynamic.buffer.FifoBuffer;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test that the {@link SpoutRunner} starts spouts when passed in and handles their lifecycle correctly.
 */
public class SpoutRunnerTest {

    private static final Logger logger = LoggerFactory.getLogger(SpoutRunnerTest.class);
    private static final int maxWaitTime = 5;

    private ThreadPoolExecutor executorService;

    /**
     * Shutdown the thread executor service when the test is all over.
     * @throws InterruptedException something went wrong.
     */
    @AfterEach
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
        final MessageBus messageBus = new MessageBus(new FifoBuffer());
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> ackQueue = new ConcurrentHashMap<>();
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> failQueue = new ConcurrentHashMap<>();
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
            messageBus,
            clock,
            topologyConfig
        );

        // Call getters and validate!
        assertEquals(clock, spoutRunner.getClock(), "Clock instance is what we expect");
        assertEquals(topologyConfig, spoutRunner.getTopologyConfig(), "TopologyConfig looks legit");
        assertEquals(
            consumerStateFlushInterval,
            spoutRunner.getConsumerStateFlushIntervalMs(),
            "getConsumerStateFlushIntervalMs() returns right value"
        );

        assertNotNull(spoutRunner.getStartTime(), "StartTime is null");
        assertNotEquals(0, spoutRunner.getStartTime(), "StartTime is not zero");
        assertEquals(spout, spoutRunner.getSpout(), "Spout delegate got set");
    }

    /**
     * Tests that
     *   - our latch counts down when we start the thread.
     *   - open() is called on the spout on startup.
     *   - close() is called on the spout on shutdown.
     *   - spoutRunner.requestStop() shuts down the instance.
     *   - spoutDelegate.requestStop() shuts down the instance.
     */
    @ParameterizedTest
    @MethodSource("provideTrueAndFalse")
    public void testOpenandCloseOnSpoutIsCalled(final boolean shutdownViaSpout) throws InterruptedException {
        // Define inputs
        final Clock clock = Clock.systemUTC();
        final DefaultVirtualSpoutIdentifier virtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSpout mockSpout = new MockDelegateSpout(virtualSpoutId);

        // Setup mock MessageBus
        final VirtualSpoutMessageBus messageBus = mock(VirtualSpoutMessageBus.class);
        when(messageBus.getAckedMessage(eq(virtualSpoutId))).thenReturn(null);
        when(messageBus.getFailedMessage(eq(virtualSpoutId))).thenReturn(null);

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            messageBus,
            clock,
            topologyConfig
        );

        // Start in a separate thread.
        final CompletableFuture future = startSpoutRunner(spoutRunner);

        // Wait for open to be called since it runs async.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> mockSpout.wasOpenCalled, equalTo(true));

        // Verify open was called once, but not close
        assertTrue(mockSpout.wasOpenCalled, "Open was called on our mock spout");
        assertFalse(mockSpout.wasCloseCalled, "Close has not been called yet on our mock spout");

        // Verify queues got setup
        verify(messageBus, times(1)).registerVirtualSpout(eq(virtualSpoutId));

        // But not torn down yet
        verify(messageBus, never()).unregisterVirtualSpout(anyObject());

        // Shut down
        if (shutdownViaSpout) {
            logger.info("Requesting stop via Spout.isCompleted()");
            mockSpout.completed = true;
        } else {
            logger.info("Requesting stop via SpoutRunner.requestStop()");
            spoutRunner.requestStop();
        }

        // Wait for thread to stop.
        await()
            .atMost(maxWaitTime * 2, TimeUnit.SECONDS)
            .until(future::isDone);

        // Make sure it actually stopped
        assertEquals(0, executorService.getActiveCount(), "Should have no running threads");

        // verify close was called
        assertTrue(mockSpout.wasCloseCalled, "Close was called on our mock spout");

        // Verify entries removed from buffer, ackQueue, failQueue
        verify(messageBus, times(1)).unregisterVirtualSpout(eq(virtualSpoutId));
    }

    /**
     * Provides various tuple buffer implementation.
     */
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
        final Clock clock = Clock.systemUTC();
        final DefaultVirtualSpoutIdentifier virtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSpout mockSpout = new MockDelegateSpout(virtualSpoutId);

        // Create message bus.
        final MessageBus messageBus = new MessageBus(FifoBuffer.createDefaultInstance());

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            messageBus,
            clock,
            topologyConfig
        );

        // Start in a separate thread.
        final CompletableFuture future = startSpoutRunner(spoutRunner);

        // Wait for open to be called since it runs async.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> mockSpout.wasOpenCalled, equalTo(true));

        // sanity test
        assertEquals(0, messageBus.messageSize(), "MessageBuffer should be empty");

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
            .until(() -> messageBus.messageSize() == 3, equalTo(true));

        // Sanity test
        assertEquals(3, messageBus.messageSize(), "MessageBuffer should have 3 entries");

        logger.info("Requesting stop via SpoutRunner.requestStop()");
        spoutRunner.requestStop();

        // Wait for thread to stop.
        await()
            .atMost(maxWaitTime * 2, TimeUnit.SECONDS)
            .until(future::isDone);

        // Make sure it actually stopped
        assertEquals(0, executorService.getActiveCount(), "Should have no running threads");

        // verify close was called
        assertTrue(mockSpout.wasCloseCalled, "Close was called on our mock spout");
    }

    /**
     * Tests that fails get sent to the spout instance.
     */
    @Test
    public void testFailsGetSentToSpout() {
        // Define inputs
        final Clock clock = Clock.systemUTC();
        final DefaultVirtualSpoutIdentifier virtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSpout mockSpout = new MockDelegateSpout(virtualSpoutId);

        // Create message bus.
        final MessageBus messageBus = new MessageBus(FifoBuffer.createDefaultInstance());

        // Add other virtual spout id in ack and fail queues
        final VirtualSpoutIdentifier otherVirtualSpoutId = new DefaultVirtualSpoutIdentifier("OtherVirtualSpout");
        messageBus.registerVirtualSpout(otherVirtualSpoutId);

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            messageBus,
            clock,
            topologyConfig
        );

        // Start in a separate thread.
        final CompletableFuture future = startSpoutRunner(spoutRunner);

        // Wait for open to be called since it runs async.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> mockSpout.wasOpenCalled, equalTo(true));

        // sanity test
        assertEquals(0, messageBus.messageSize(), "MessageBuffer should be empty");
        assertEquals(0, messageBus.ackSize(), "Ack Queue should be empty");
        assertEquals(0, messageBus.failSize(), "fail Queue should be empty");

        // Create some MessageIds for our virtualSpoutId
        final MessageId messageId1 = new MessageId("namespace", 0, 0L, virtualSpoutId);
        final MessageId messageId2 = new MessageId("namespace", 0, 1L, virtualSpoutId);
        final MessageId messageId3 = new MessageId("namespace", 0, 2L, virtualSpoutId);

        // Create some MessageIds for a different virtualSpoutId
        final MessageId messageId4 = new MessageId("namespace", 0, 0L, otherVirtualSpoutId);
        final MessageId messageId5 = new MessageId("namespace", 0, 1L, otherVirtualSpoutId);
        final MessageId messageId6 = new MessageId("namespace", 0, 2L, otherVirtualSpoutId);

        // Add them to the appropriate queues
        messageBus.fail(messageId1);
        messageBus.fail(messageId2);
        messageBus.fail(messageId3);
        messageBus.fail(messageId4);
        messageBus.fail(messageId5);
        messageBus.fail(messageId6);

        // Now wait for them to show up in our spout instance
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> mockSpout.failedTupleIds.size() == 3, equalTo(true));

        // Sanity test
        assertEquals(3, messageBus.failSize(), "failQueue should now contain 3 entries (for the other vspout id)");
        assertEquals(3, mockSpout.failedTupleIds.size(), "mock spout should have gotten 3 fail() calls");
        assertTrue(mockSpout.failedTupleIds.contains(messageId1), "Should have messageId");
        assertTrue(mockSpout.failedTupleIds.contains(messageId2), "Should have messageId");
        assertTrue(mockSpout.failedTupleIds.contains(messageId3), "Should have messageId");
        assertFalse(mockSpout.failedTupleIds.contains(messageId4), "Should NOT have messageId");
        assertFalse(mockSpout.failedTupleIds.contains(messageId5), "Should NOT have messageId");
        assertFalse(mockSpout.failedTupleIds.contains(messageId6), "Should NOT have messageId");

        // Calling getFailedMessage with our VirtualSpoutId should return empty optionals
        for (int loopCount = 0; loopCount < 10; loopCount++) {
            assertNull(messageBus.getFailedMessage(virtualSpoutId), "Should be empty/null");
        }

        // Other virtualspout id queue should still be populated
        assertEquals(3, messageBus.failSize(), "fail queue for other virtual spout should remain full");

        // No failed ids
        assertTrue(mockSpout.ackedTupleIds.isEmpty(), "acked() never called");

        logger.info("Requesting stop via SpoutRunner.requestStop()");
        spoutRunner.requestStop();

        // Wait for thread to stop.
        await()
            .atMost(maxWaitTime * 2, TimeUnit.SECONDS)
            .until(future::isDone);

        // Make sure it actually stopped
        assertEquals(0, executorService.getActiveCount(), "Should have no running threads");

        // verify close was called
        assertTrue(mockSpout.wasCloseCalled, "Close was called on our mock spout");
    }

    /**
     * Tests that acks get sent to the spout instance.
     */
    @Test
    public void testAcksGetSentToSpout() {
        // Define inputs
        final Clock clock = Clock.systemUTC();
        final VirtualSpoutIdentifier virtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSpout mockSpout = new MockDelegateSpout(virtualSpoutId);

        // Create message bus.
        final MessageBus messageBus = new MessageBus(FifoBuffer.createDefaultInstance());

        // Add other virtual spout id in ack and fail queues
        final DefaultVirtualSpoutIdentifier otherVirtualSpoutId = new DefaultVirtualSpoutIdentifier("OtherVirtualSpout");
        messageBus.registerVirtualSpout(otherVirtualSpoutId);

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            messageBus,
            clock,
            topologyConfig
        );

        // Start in a separate thread.
        final CompletableFuture future = startSpoutRunner(spoutRunner);

        // Wait for open to be called on the spout since it happens async.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> mockSpout.wasOpenCalled, equalTo(true));

        // sanity test
        assertEquals(0, messageBus.messageSize(), "MessageBuffer should be empty");
        assertEquals(0, messageBus.ackSize(), "Ack Queue should be empty");
        assertEquals(0, messageBus.failSize(), "fail Queue should be empty");

        // Create some MessageIds for our virtualSpoutId
        final MessageId messageId1 = new MessageId("namespace", 0, 0L, virtualSpoutId);
        final MessageId messageId2 = new MessageId("namespace", 0, 1L, virtualSpoutId);
        final MessageId messageId3 = new MessageId("namespace", 0, 2L, virtualSpoutId);

        // Create some MessageIds for a different virtualSpoutId
        final MessageId messageId4 = new MessageId("namespace", 0, 0L, otherVirtualSpoutId);
        final MessageId messageId5 = new MessageId("namespace", 0, 1L, otherVirtualSpoutId);
        final MessageId messageId6 = new MessageId("namespace", 0, 2L, otherVirtualSpoutId);

        // Add them to the appropriate queues
        messageBus.ack(messageId1);
        messageBus.ack(messageId2);
        messageBus.ack(messageId3);
        messageBus.ack(messageId4);
        messageBus.ack(messageId5);
        messageBus.ack(messageId6);

        // Now wait for them to show up in our spout instance
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> mockSpout.ackedTupleIds.size() == 3, equalTo(true));

        // Sanity test
        assertEquals(3, messageBus.ackSize(), "ackQueue should now have 3 entries (for other vspoutId)");
        assertEquals(3, mockSpout.ackedTupleIds.size(), "mock spout should have gotten 3 ack() calls");
        assertTrue(mockSpout.ackedTupleIds.contains(messageId1), "Should have messageId");
        assertTrue(mockSpout.ackedTupleIds.contains(messageId2), "Should have messageId");
        assertTrue(mockSpout.ackedTupleIds.contains(messageId3), "Should have messageId");
        assertFalse(mockSpout.ackedTupleIds.contains(messageId4), "Should NOT have messageId");
        assertFalse(mockSpout.ackedTupleIds.contains(messageId5), "Should NOT have messageId");
        assertFalse(mockSpout.ackedTupleIds.contains(messageId6), "Should NOT have messageId");

        // Calling getAckedMessage with our VirtualSpoutId should return empty optionals
        for (int loopCount = 0; loopCount < 10; loopCount++) {
            assertNull(messageBus.getAckedMessage(virtualSpoutId), "Should be empty/null");
        }

        // Other virtualspout id queue should still be populated
        assertEquals(3, messageBus.ackSize(), "ack queue for other virtual spout should remain full");

        // No failed ids
        assertTrue(mockSpout.failedTupleIds.isEmpty(), "Failed() never called");

        logger.info("Requesting stop via SpoutRunner.requestStop()");
        spoutRunner.requestStop();

        // Wait for thread to stop.
        await()
            .atMost(maxWaitTime * 2, TimeUnit.SECONDS)
            .until(future::isDone);

        // Make sure it actually stopped
        assertEquals(0, executorService.getActiveCount(), "Should have no running threads");

        // verify close was called
        assertTrue(mockSpout.wasCloseCalled, "Close was called on our mock spout");
    }

    /**
     * Tests that flushState() gets called on spout periodically.
     */
    @Test
    public void testFlushStateGetsCalled() {
        // Define inputs
        final Clock clock = Clock.systemUTC();
        final DefaultVirtualSpoutIdentifier virtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSpout mockSpout = new MockDelegateSpout(virtualSpoutId);

        // Create message bus.
        final MessageBus messageBus = new MessageBus(FifoBuffer.createDefaultInstance());

        // Add other virtual spout id in ack and fail queues
        final DefaultVirtualSpoutIdentifier otherVirtualSpoutId = new DefaultVirtualSpoutIdentifier("OtherVirtualSpout");
        messageBus.registerVirtualSpout(otherVirtualSpoutId);

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            messageBus,
            clock,
            topologyConfig
        );

        // Start in a separate thread.
        final CompletableFuture future = startSpoutRunner(spoutRunner);

        // Wait for open to be called since it runs async.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> mockSpout.wasOpenCalled, equalTo(true));

        // Wait for flush state to get called
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> mockSpout.flushStateCalled, equalTo(true));


        logger.info("Requesting stop via SpoutRunner.requestStop()");
        spoutRunner.requestStop();

        // Wait for thread to stop.
        await()
            .atMost(maxWaitTime * 2, TimeUnit.SECONDS)
            .until(future::isDone);

        // Make sure it actually stopped
        assertEquals(0, executorService.getActiveCount(), "Should have no running threads");

        // verify close was called
        assertTrue(mockSpout.wasCloseCalled, "Close was called on our mock spout");
    }

    private Map<String, Object> getDefaultConfig(long consumerStateFlushIntervalMs) {
        final Map<String, Object> topologyConfig = SpoutConfig.setDefaults(new HashMap<>());
        topologyConfig.put(SpoutConfig.CONSUMER_STATE_FLUSH_INTERVAL_MS, consumerStateFlushIntervalMs);
        return topologyConfig;
    }

    private CompletableFuture startSpoutRunner(SpoutRunner spoutRunner) {
        if (executorService == null) {
            executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        }

        // Sanity check
        assertEquals(0, executorService.getActiveCount(), "Executor service should be empty");

        // Submit task to start
        CompletableFuture future = CompletableFuture.runAsync(spoutRunner, executorService);

        // Wait until it actually starts.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> executorService.getActiveCount() == 1, equalTo(true));

        // return the future
        return future;
    }
}