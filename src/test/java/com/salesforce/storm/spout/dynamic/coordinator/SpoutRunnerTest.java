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
import com.salesforce.storm.spout.dynamic.MessageBus;
import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.VirtualSpoutMessageBus;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.DelegateSpout;
import com.salesforce.storm.spout.dynamic.mocks.MockDelegateSpout;
import com.salesforce.storm.spout.dynamic.buffer.FifoBuffer;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
        final MessageBus messageBus = new MessageBus(new FifoBuffer());
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> ackQueue = Maps.newConcurrentMap();
        final Map<VirtualSpoutIdentifier, Queue<MessageId>> failQueue = Maps.newConcurrentMap();
        final Clock clock = Clock.systemUTC();

        // Define some config params
        final long consumerStateFlushInterval = 200L;

        // Create our spout delegate
        final DelegateSpout spout = mock(DelegateSpout.class);

        // Create config
        final AbstractConfig spoutConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            spout,
            messageBus,
            clock,
            spoutConfig
        );

        // Call getters and validate!
        assertEquals("Clock instance is what we expect", clock, spoutRunner.getClock());
        assertEquals("SpoutConfig looks legit", spoutConfig, spoutRunner.getSpoutConfig());
        assertEquals(
            "getConsumerStateFlushIntervalMs() returns right value",
            consumerStateFlushInterval,
            spoutRunner.getConsumerStateFlushIntervalMs()
        );

        assertNotNull("StartTime is null", spoutRunner.getStartTime());
        assertNotEquals("StartTime is not zero", 0, spoutRunner.getStartTime());
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
        final AbstractConfig spoutConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            messageBus,
            clock,
            spoutConfig
        );

        // Start in a separate thread.
        final CompletableFuture future = startSpoutRunner(spoutRunner);

        // Wait for open to be called since it runs async.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> mockSpout.wasOpenCalled, equalTo(true));

        // Verify open was called once, but not close
        assertTrue("Open was called on our mock spout", mockSpout.wasOpenCalled);
        assertFalse("Close has not been called yet on our mock spout", mockSpout.wasCloseCalled);

        // Verify queues got setup
        verify(messageBus, times(1)).registerVirtualSpout(eq(virtualSpoutId));

        // But not torn down yet
        verify(messageBus, never()).unregisterVirtualSpout(anyObject());

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
        verify(messageBus, times(1)).unregisterVirtualSpout(eq(virtualSpoutId));
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
        final Clock clock = Clock.systemUTC();
        final DefaultVirtualSpoutIdentifier virtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");

        // Define some config params
        final long consumerStateFlushInterval = TimeUnit.MILLISECONDS.convert(maxWaitTime, TimeUnit.SECONDS);

        // Create our spout delegate
        final MockDelegateSpout mockSpout = new MockDelegateSpout(virtualSpoutId);

        // Create message bus.
        final MessageBus messageBus = new MessageBus(FifoBuffer.createDefaultInstance());

        // Create config
        final AbstractConfig spoutConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            messageBus,
            clock,
            spoutConfig
        );

        // Start in a separate thread.
        final CompletableFuture future = startSpoutRunner(spoutRunner);

        // Wait for open to be called since it runs async.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> mockSpout.wasOpenCalled, equalTo(true));

        // sanity test
        assertEquals("MessageBuffer should be empty", 0, messageBus.messageSize());

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
        assertEquals("MessageBuffer should have 3 entries", 3, messageBus.messageSize());

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
        final AbstractConfig spoutConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            messageBus,
            clock,
            spoutConfig
        );

        // Start in a separate thread.
        final CompletableFuture future = startSpoutRunner(spoutRunner);

        // Wait for open to be called since it runs async.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> mockSpout.wasOpenCalled, equalTo(true));

        // sanity test
        assertEquals("MessageBuffer should be empty", 0, messageBus.messageSize());
        assertEquals("Ack Queue should be empty", 0, messageBus.ackSize());
        assertEquals("fail Queue should be empty", 0, messageBus.failSize());

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
        assertEquals("failQueue should now contain 3 entries (for the other vspout id)", 3, messageBus.failSize());
        assertEquals("mock spout should have gotten 3 fail() calls", 3, mockSpout.failedTupleIds.size());
        assertTrue("Should have messageId", mockSpout.failedTupleIds.contains(messageId1));
        assertTrue("Should have messageId", mockSpout.failedTupleIds.contains(messageId2));
        assertTrue("Should have messageId", mockSpout.failedTupleIds.contains(messageId3));
        assertFalse("Should NOT have messageId", mockSpout.failedTupleIds.contains(messageId4));
        assertFalse("Should NOT have messageId", mockSpout.failedTupleIds.contains(messageId5));
        assertFalse("Should NOT have messageId", mockSpout.failedTupleIds.contains(messageId6));

        // Calling getFailedMessage with our VirtualSpoutId should return empty optionals
        for (int loopCount = 0; loopCount < 10; loopCount++) {
            assertNull("Should be empty/null", messageBus.getFailedMessage(virtualSpoutId));
        }

        // Other virtualspout id queue should still be populated
        assertEquals("fail queue for other virtual spout should remain full", 3, messageBus.failSize());

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
        final AbstractConfig spoutConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            messageBus,
            clock,
            spoutConfig
        );

        // Start in a separate thread.
        final CompletableFuture future = startSpoutRunner(spoutRunner);

        // Wait for open to be called on the spout since it happens async.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> mockSpout.wasOpenCalled, equalTo(true));

        // sanity test
        assertEquals("MessageBuffer should be empty", 0, messageBus.messageSize());
        assertEquals("Ack Queue should be empty", 0, messageBus.ackSize());
        assertEquals("fail Queue should be empty", 0, messageBus.failSize());

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
        assertEquals("ackQueue should now have 3 entries (for other vspoutId)", 3, messageBus.ackSize());
        assertEquals("mock spout should have gotten 3 ack() calls", 3, mockSpout.ackedTupleIds.size());
        assertTrue("Should have messageId", mockSpout.ackedTupleIds.contains(messageId1));
        assertTrue("Should have messageId", mockSpout.ackedTupleIds.contains(messageId2));
        assertTrue("Should have messageId", mockSpout.ackedTupleIds.contains(messageId3));
        assertFalse("Should NOT have messageId", mockSpout.ackedTupleIds.contains(messageId4));
        assertFalse("Should NOT have messageId", mockSpout.ackedTupleIds.contains(messageId5));
        assertFalse("Should NOT have messageId", mockSpout.ackedTupleIds.contains(messageId6));

        // Calling getAckedMessage with our VirtualSpoutId should return empty optionals
        for (int loopCount = 0; loopCount < 10; loopCount++) {
            assertNull("Should be empty/null", messageBus.getAckedMessage(virtualSpoutId));
        }

        // Other virtualspout id queue should still be populated
        assertEquals("ack queue for other virtual spout should remain full", 3, messageBus.ackSize());

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
        final AbstractConfig spoutConfig = getDefaultConfig(consumerStateFlushInterval);

        // Create instance.
        SpoutRunner spoutRunner = new SpoutRunner(
            mockSpout,
            messageBus,
            clock,
            spoutConfig
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
        assertEquals("Should have no running threads", 0, executorService.getActiveCount());

        // verify close was called
        assertTrue("Close was called on our mock spout", mockSpout.wasCloseCalled);
    }

    private AbstractConfig getDefaultConfig(long consumerStateFlushIntervalMs) {
        final Map<String, Object> config = new HashMap<>();
        config.put(SpoutConfig.CONSUMER_STATE_FLUSH_INTERVAL_MS, consumerStateFlushIntervalMs);
        return new SpoutConfig(config);
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
            .until(() -> executorService.getActiveCount() == 1, equalTo(true));

        // return the future
        return future;
    }
}