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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.salesforce.storm.spout.dynamic.Message;
import com.salesforce.storm.spout.dynamic.MessageBus;
import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.DelegateSpout;
import com.salesforce.storm.spout.dynamic.exception.SpoutAlreadyExistsException;
import com.salesforce.storm.spout.dynamic.exception.SpoutDoesNotExistException;
import com.salesforce.storm.spout.dynamic.metrics.LogRecorder;
import com.salesforce.storm.spout.dynamic.metrics.MetricsRecorder;
import com.salesforce.storm.spout.dynamic.mocks.MockDelegateSpout;
import com.salesforce.storm.spout.dynamic.mocks.MockTopologyContext;
import com.salesforce.storm.spout.dynamic.buffer.FifoBuffer;
import com.salesforce.storm.spout.dynamic.buffer.MessageBuffer;
import org.apache.storm.tuple.Values;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *  Test that the {@link SpoutMonitor} detects new spouts and manages {@link SpoutRunner} instances for them.
 */
public class SpoutMonitorTest {

    private static final Logger logger = LoggerFactory.getLogger(SpoutMonitorTest.class);
    private static final int maxWaitTime = 5;

    /**
     * This is the executor pool we run tests against.
     */
    private ThreadPoolExecutor executorService;

    /**
     * Expect no exceptions by default.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
        final Clock clock = Clock.systemUTC();

        // Define some config params
        final int maxConcurrentSpouts = 2;
        final long maxShutdownTime = 200L;
        final long monitorInterval = 300L;

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(maxConcurrentSpouts, maxShutdownTime, monitorInterval);

        // Create metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, new MockTopologyContext());

        // Create instance.
        final SpoutMonitor spoutMonitor = new SpoutMonitor(
            messageBus,
            clock,
            topologyConfig,
            metricsRecorder
        );

        // Call getters and validate!
        assertEquals("Clock instance is what we expect", clock, spoutMonitor.getClock());
        assertEquals("TopologyConfig looks legit", topologyConfig, spoutMonitor.getTopologyConfig());
        assertEquals("getMonitorThreadIntervalMs() returns right value", monitorInterval, spoutMonitor.getMonitorThreadIntervalMs());
        assertEquals("getMaxTerminationWaitTimeMs() returns right value", maxShutdownTime, spoutMonitor.getMaxTerminationWaitTimeMs());
        assertEquals(
            "getMaxConcurrentVirtualSpouts() returns right value",
            maxConcurrentSpouts, spoutMonitor.getMaxConcurrentVirtualSpouts()
        );
        assertTrue("KeepRunning should default to true", spoutMonitor.keepRunning());
        assertEquals("getMetricsRecorder returns right value", metricsRecorder, spoutMonitor.getMetricsRecorder());

        // Validate executor
        final ThreadPoolExecutor executor = spoutMonitor.getExecutor();
        assertNotNull("Executor should be not null", executor);
        assertEquals("Should have max threads set", maxConcurrentSpouts, executor.getMaximumPoolSize());

        // Close up shop.
        spoutMonitor.close();

        // Verify that executor service is terminated
        assertTrue("Executor service is terminated", spoutMonitor.getExecutor().isTerminated());
        assertEquals("ExecutorService has no running threads", 0, spoutMonitor.getExecutor().getActiveCount());
        assertFalse("Keep running should return false", spoutMonitor.keepRunning());
    }

    /**
     * Simple smoke test starting the monitor in a thread, and then asking it to close.
     */
    @Test
    public void testStartAndCloseSmokeTest() throws InterruptedException, ExecutionException {
        // Create instance.
        final SpoutMonitor spoutMonitor = getDefaultMonitorInstance();

        // Define how long to wait for async operations
        final long testWaitTime = (spoutMonitor.getMonitorThreadIntervalMs() * 2) + 10;

        // call run in async thread.
        final CompletableFuture future = startSpoutMonitor(spoutMonitor);

        // Wait for it to fire up
        Thread.sleep(testWaitTime);

        // Now validate we have no spouts submitted
        assertEquals("Should have no spouts", 0, spoutMonitor.getTotalSpouts());

        // validate the executor is running nothing
        assertEquals("Should have 0 running task", 0, spoutMonitor.getExecutor().getActiveCount());

        // Close spout monitor
        shutdownSpoutMonitor(spoutMonitor, future);

        // Verify that executor service is terminated
        assertTrue("Executor service is terminated", spoutMonitor.getExecutor().isTerminated());
        assertEquals("ExecutorService has no running threads", 0, spoutMonitor.getExecutor().getActiveCount());
    }

    /**
     * Test submitting new virtual spout.
     */
    @Test
    public void testSubmittingNewSpout() throws InterruptedException, ExecutionException {
        // Create instance.
        final SpoutMonitor spoutMonitor = getDefaultMonitorInstance();

        // Define how long to wait for async operations
        final long testWaitTime = (spoutMonitor.getMonitorThreadIntervalMs() * 2) + 10;

        // call run in async thread.
        final CompletableFuture future = startSpoutMonitor(spoutMonitor);

        // Wait for it to fire up
        Thread.sleep(testWaitTime);

        // Sanity test - Now validate we have no spouts submitted
        assertEquals("Should have no spouts", 0, spoutMonitor.getTotalSpouts());

        // Create a mock spout
        final DelegateSpout mockSpout = mock(DelegateSpout.class);
        when(mockSpout.getVirtualSpoutId()).thenReturn(new DefaultVirtualSpoutIdentifier("MySpoutId"));

        // Add it to our queue
        spoutMonitor.addVirtualSpout(mockSpout);

        // Wait for spout count to increase
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(spoutMonitor::getTotalSpouts, equalTo(1));

        // validate the executor is running it
        assertEquals("Should have 1 running task", 1, spoutMonitor.getExecutor().getActiveCount());

        // Now validate some calls onto the mock
        // Our spout should have been opened
        verify(mockSpout, times(1)).open();

        // But never closed... yet
        verify(mockSpout, never()).close();

        // Verify nextTuple is called at least once
        verify(mockSpout, atLeastOnce()).nextTuple();

        // Close the monitor
        shutdownSpoutMonitor(spoutMonitor, future);

        // Verify close called on the mock spout
        verify(mockSpout, times(1)).close();

        // Verify that executor service is terminated
        assertTrue("Executor service is terminated", spoutMonitor.getExecutor().isTerminated());
        assertEquals("ExecutorService has no running threads", 0, spoutMonitor.getExecutor().getActiveCount());
    }

    /**
     * Test what happens when a running spout instance finishes "normally" by
     * requesting it to stop.
     */
    @Test
    public void testWhatHappensWhenSpoutClosesNormally() throws InterruptedException, ExecutionException {
        // Create instance.
        final SpoutMonitor spoutMonitor = getDefaultMonitorInstance();

        // Define how long to wait for async operations
        final long testWaitTime = (spoutMonitor.getMonitorThreadIntervalMs() * 10);

        // call run in async thread.
        final CompletableFuture future = startSpoutMonitor(spoutMonitor);

        // Wait for it to fire up
        Thread.sleep(testWaitTime);

        // Sanity test - Now validate we have no spouts submitted
        assertEquals("Should have no spouts", 0, spoutMonitor.getTotalSpouts());

        // Create a mock spout
        MockDelegateSpout mockSpout = new MockDelegateSpout(new DefaultVirtualSpoutIdentifier("MySpoutId"));
        mockSpout.requestedStop = false;

        // Add it to our queue
        spoutMonitor.addVirtualSpout(mockSpout);

        // Wait for spout count to increase
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(spoutMonitor::getTotalSpouts, equalTo(1));

        // Verify open was called
        assertTrue("open() should have been called", mockSpout.wasOpenCalled);

        // validate the executor is running it
        assertEquals("Should have 1 running task", 1, spoutMonitor.getExecutor().getActiveCount());

        // Make the mock spout stop.
        mockSpout.requestStop();

        // Wait for spout count to decrease
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(spoutMonitor::getTotalSpouts, equalTo(0));

        // validate the executor should no longer have any running tasks?
        assertEquals("Should have no running tasks", 0, spoutMonitor.getExecutor().getActiveCount());

        // Close the monitor
        shutdownSpoutMonitor(spoutMonitor, future);

        // Verify closed was called
        assertTrue("Close() should have been called", mockSpout.wasCloseCalled);

        // Verify that executor service is terminated
        assertTrue("Executor service is terminated", spoutMonitor.getExecutor().isTerminated());
        assertEquals("ExecutorService has no running threads", 0, spoutMonitor.getExecutor().getActiveCount());
    }

    /**
     * Tests that if you ask for more virtual spout instances to run than configured threads,
     * we will never run them.
     */
    @Test
    public void testQueuedSpoutInstancesAreNeverStarted() throws InterruptedException, ExecutionException {
        // Create instance.
        final SpoutMonitor spoutMonitor = getDefaultMonitorInstance();

        // Define how long to wait for async operations
        final long testWaitTime = (spoutMonitor.getMonitorThreadIntervalMs() * 10);

        final int maxConcurrentInstances = (int) spoutMonitor.getTopologyConfig().get(SpoutConfig.MAX_CONCURRENT_VIRTUAL_SPOUTS);

        // Lets create some virtual spouts
        List<MockDelegateSpout> mockSpouts = Lists.newArrayList();
        for (int x = 0; x < maxConcurrentInstances + 2; x++) {
            mockSpouts.add(new MockDelegateSpout(new DefaultVirtualSpoutIdentifier("SpoutInstance" + x)));
        }

        // call run in async thread.
        final CompletableFuture future = startSpoutMonitor(spoutMonitor);

        // Wait for it to fire up
        Thread.sleep(testWaitTime);

        // Sanity test - Now validate we have no spouts submitted
        assertEquals("Should have no spouts", 0, spoutMonitor.getTotalSpouts());

        // Add all of our spouts to the queue.
        mockSpouts.forEach(spoutMonitor::addVirtualSpout);

        // Wait for spout count to increase to the number of spout instances we submitted.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(spoutMonitor::getTotalSpouts, equalTo(mockSpouts.size()));

        // Now the executor should only run a certain number
        assertEquals("Only configured max running concurrently", maxConcurrentInstances, spoutMonitor.getExecutor().getActiveCount());

        // The difference should be queued
        assertEquals(
            "Should have some queued instances",
            (mockSpouts.size() - maxConcurrentInstances), spoutMonitor.getExecutor().getQueue().size()
        );

        // Add additional sleep time, just so logs show up
        Thread.sleep(testWaitTime);

        // On spouts that should have run
        for (int x = 0; x < maxConcurrentInstances; x++) {
            final MockDelegateSpout mockSpout = mockSpouts.get(x);
            assertTrue("open() should have been called", mockSpout.wasOpenCalled);
        }

        // Now call close on spout monitor
        logger.info("Starting to close spout monitor...");

        // Close the monitor
        shutdownSpoutMonitor(spoutMonitor, future);

        // Verify close was called on running spouts
        for (int x = 0; x < mockSpouts.size(); x++) {
            final MockDelegateSpout mockSpout = mockSpouts.get(x);

            // If it was a running spout instance
            if (x < maxConcurrentInstances) {
                assertTrue("close() should have been called", mockSpout.wasCloseCalled);
            } else {
                assertFalse("open() should NOT have been called", mockSpout.wasOpenCalled);
                assertFalse("close() should NOT have been called", mockSpout.wasCloseCalled);
            }
        }

        // Verify that executor service is terminated
        assertTrue("Executor service is terminated", spoutMonitor.getExecutor().isTerminated());
        assertEquals("ExecutorService has no running threads", 0, spoutMonitor.getExecutor().getActiveCount());
    }

    /**
     * Tests that if you ask for more virtual spout instances to run than configured threads,
     * we will run them as threads become available.
     */
    @Test
    public void testQueuedSpoutInstancesAreStartedWhenAvailableThreads() throws InterruptedException, ExecutionException {
        // Create instance.
        final SpoutMonitor spoutMonitor = getDefaultMonitorInstance();

        // Define how long to wait for async operations
        final long testWaitTime = (spoutMonitor.getMonitorThreadIntervalMs() * 10);

        final int maxConcurrentInstances = (int) spoutMonitor.getTopologyConfig().get(SpoutConfig.MAX_CONCURRENT_VIRTUAL_SPOUTS);

        // Lets create some virtual spouts
        List<MockDelegateSpout> mockSpouts = Lists.newArrayList();
        for (int x = 0; x < maxConcurrentInstances + 1; x++) {
            mockSpouts.add(new MockDelegateSpout(new DefaultVirtualSpoutIdentifier("SpoutInstance" + x)));
        }

        // call run in async thread.
        final CompletableFuture future = startSpoutMonitor(spoutMonitor);

        // Wait for it to fire up
        Thread.sleep(testWaitTime);

        // Sanity test - Now validate we have no spouts submitted
        assertEquals("Should have no spouts", 0, spoutMonitor.getTotalSpouts());

        // Add all of our spouts to the queue.
        mockSpouts.forEach(spoutMonitor::addVirtualSpout);

        // Wait for spout count to increase to the number of spout instances we submitted.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(spoutMonitor::getTotalSpouts, equalTo(mockSpouts.size()));

        // Now the executor should only run a certain number
        assertEquals("Only configured max running concurrently", maxConcurrentInstances, spoutMonitor.getExecutor().getActiveCount());

        // The difference should be queued
        assertEquals(
            "Should have some queued instances",
            (mockSpouts.size() - maxConcurrentInstances), spoutMonitor.getExecutor().getQueue().size()
        );

        // Add additional sleep time, just so logs show up
        Thread.sleep(testWaitTime);

        // On spouts that should have run
        for (int x = 0; x < maxConcurrentInstances; x++) {
            final MockDelegateSpout mockSpout = mockSpouts.get(x);
            assertTrue("open() should have been called", mockSpout.wasOpenCalled);
        }

        final MockDelegateSpout notStartedSpout = mockSpouts.get(mockSpouts.size() - 1);
        assertFalse("Should not have called open on our spout thats not running", notStartedSpout.wasOpenCalled);

        // Now stop the first instance
        mockSpouts.get(0).requestStop();

        // Wait for it to be closed
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> mockSpouts.get(0).wasCloseCalled, equalTo(true));

        // Our not started instance should now start...
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> notStartedSpout.wasOpenCalled, equalTo(true));

        // Now call close on spout monitor
        logger.info("Starting to close spout monitor...");

        // Close spout monitor
        shutdownSpoutMonitor(spoutMonitor, future);

        // Verify close was called on all spouts
        for (MockDelegateSpout mockSpout : mockSpouts) {
            assertTrue("close() should have been called", mockSpout.wasCloseCalled);
        }

        // Verify that executor service is terminated
        assertTrue("Executor service is terminated", spoutMonitor.getExecutor().isTerminated());
        assertEquals("ExecutorService has no running threads", 0, spoutMonitor.getExecutor().getActiveCount());
    }

    /**
     * Test what happens when a running spout instance does not finish "normally",
     * but instead throws an exception.
     *
     * TODO: This behavior is yet to be defined.
     */
    @Test
    public void testWhatHappensWhenSpoutThrowsException() throws InterruptedException, ExecutionException {
        // Create messageBus
        final MessageBus messageBus = new MessageBus(FifoBuffer.createDefaultInstance());

        // Create instance.
        final SpoutMonitor spoutMonitor = getDefaultMonitorInstance(messageBus);

        // call run in async thread.
        final CompletableFuture future = startSpoutMonitor(spoutMonitor);

        // Sanity test - Now validate we have no spouts submitted
        assertEquals("Should have no spouts", 0, spoutMonitor.getTotalSpouts());
        assertEquals("Should have no failed tasks yet", 0, spoutMonitor.getNumberOfFailedTasks());

        // Create a mock spout
        MockDelegateSpout mockSpout = new MockDelegateSpout(new DefaultVirtualSpoutIdentifier("MySpoutId"));
        mockSpout.requestedStop = false;

        // Add it to our queue
        spoutMonitor.addVirtualSpout(mockSpout);

        // Wait for spout count to increase
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(spoutMonitor::getTotalSpouts, equalTo(1));

        // Verify open was called
        assertTrue("open() should have been called", mockSpout.wasOpenCalled);

        // Tell the spout to throw an exception.
        final RuntimeException runtimeException = new RuntimeException("This is my runtime exception");
        mockSpout.exceptionToThrow = runtimeException;

        // Wait for spout count to decrease
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(spoutMonitor::getTotalSpouts, equalTo(0));

        // Validate that we incremented our failed task counter
        assertEquals("Should have 1 failed task", 1, spoutMonitor.getNumberOfFailedTasks());

        // Close spout monitor
        shutdownSpoutMonitor(spoutMonitor, future);

        // Verify that the exception error was reported.
        final Optional<Throwable> throwableOptional = messageBus.getErrors();
        assertTrue("Should have reported one error", throwableOptional.isPresent());
        assertTrue("Should be our reported error", throwableOptional.get().equals(runtimeException));

        // Should have no other errors
        assertFalse("Should have no other errors", messageBus.getErrors().isPresent());

        // Verify that executor service is terminated
        assertTrue("Executor service is terminated", spoutMonitor.getExecutor().isTerminated());
        assertEquals("ExecutorService has no running threads", 0, spoutMonitor.getExecutor().getActiveCount());
    }

    /**
     * End-to-end integration test over SpoutMonitor + MessageBus + VirtualSpouts.
     */
    @Test
    public void testIntegrationTest() throws Exception {
        // How often we want the monitor thread to run
        final long internalOperationsIntervalMs = 2000;

        // Define how long we'll wait for internal operations to complete
        final long waitTime = internalOperationsIntervalMs * 4;

        final List<Message> expected = Lists.newArrayList();

        final MockDelegateSpout fireHoseSpout = new MockDelegateSpout();
        final MockDelegateSpout sidelineSpout1 = new MockDelegateSpout();
        final MockDelegateSpout sidelineSpout2 = new MockDelegateSpout();

        // Note: I set the topic here to different things largely to aide debugging the message ids later on
        final Message message1 = new Message(new MessageId("message1", 1, 1L, fireHoseSpout.getVirtualSpoutId()), new Values("message1"));
        final Message message2 = new Message(new MessageId("message2", 1, 1L, sidelineSpout1.getVirtualSpoutId()), new Values("message2"));
        final Message message3 = new Message(new MessageId("message3", 1, 1L, fireHoseSpout.getVirtualSpoutId()), new Values("message3"));

        final FifoBuffer buffer = FifoBuffer.createDefaultInstance();
        final MessageBus messageBus = new MessageBus(buffer);

        // Create noop metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(Maps.newHashMap(), new MockTopologyContext());

        // Define our configuration
        final Map<String, Object> config = SpoutConfig.setDefaults(Maps.newHashMap());

        // Configure our internal operations to run frequently for our test case.
        config.put(SpoutConfig.MONITOR_THREAD_INTERVAL_MS, internalOperationsIntervalMs);
        config.put(SpoutConfig.CONSUMER_STATE_FLUSH_INTERVAL_MS, internalOperationsIntervalMs);

        // Create SpoutMonitor
        final SpoutMonitor spoutMonitor = new SpoutMonitor(
            messageBus,
            Clock.systemUTC(),
            config,
            metricsRecorder
        );

        // call run in async thread.
        final CompletableFuture future = startSpoutMonitor(spoutMonitor);

        // Add fire hose and wait for it to start.
        spoutMonitor.addVirtualSpout(fireHoseSpout);
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(spoutMonitor::getTotalSpouts, equalTo(1));

        // Add Other VirtualSpouts
        spoutMonitor.addVirtualSpout(sidelineSpout1);
        spoutMonitor.addVirtualSpout(sidelineSpout2);

        logger.info("Waiting for SpoutMonitor to detect and open() our spout instances");
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(spoutMonitor::getTotalSpouts, equalTo(3));
        assertEquals("Should have 3 spouts running", 3, spoutMonitor.getTotalSpouts());
        logger.info("SpoutMonitor now has {} spout instances", spoutMonitor.getTotalSpouts());

        // Add 1 message to each spout
        fireHoseSpout.emitQueue.add(message1);
        expected.add(message1);

        sidelineSpout1.emitQueue.add(message2);
        expected.add(message2);

        fireHoseSpout.emitQueue.add(message3);
        expected.add(message3);

        // The SpoutRunner threads should pop these messages off.
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(messageBus::messageSize, equalTo(3));

        // Ack the first two messages
        messageBus.ack(message1.getMessageId());
        messageBus.ack(message2.getMessageId());

        // Fail the third
        messageBus.fail(message3.getMessageId());

        // Wait for those to come through to the correct VirtualSpouts.
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(fireHoseSpout.ackedTupleIds::size, equalTo(1));
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(fireHoseSpout.failedTupleIds::size, equalTo(1));
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(sidelineSpout1.ackedTupleIds::size, equalTo(1));

        assertTrue(
            message1.getMessageId().equals(fireHoseSpout.ackedTupleIds.toArray()[0])
        );

        assertTrue(
            message3.getMessageId().equals(fireHoseSpout.failedTupleIds.toArray()[0])
        );

        assertTrue(
            message2.getMessageId().equals(sidelineSpout1.ackedTupleIds.toArray()[0])
        );

        // Close spout monitor
        shutdownSpoutMonitor(spoutMonitor, future);

        logger.info("Expected = " + expected);
        logger.info("Actual = " + buffer);

        for (Message a : expected) {
            boolean found = false;

            for (Message b : buffer.getUnderlyingQueue()) {
                if (a.equals(b)) {
                    found = true;
                }
            }

            assertTrue("Did not find " + a, found);
        }

        assertEquals(0, spoutMonitor.getTotalSpouts());

        // Verify the executor is terminated, and has no active tasks
        assertTrue("Executor is terminated", spoutMonitor.getExecutor().isTerminated());
    }

    /**
     * Test that adding a spout with the same id will throw an exception.
     */
    @Test
    public void testAddDuplicateSpout() {
        final MessageBus messageBus = new MessageBus(FifoBuffer.createDefaultInstance());

        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(Maps.newHashMap(), new MockTopologyContext());

        // Define our configuration
        final Map<String, Object> config = SpoutConfig.setDefaults(Maps.newHashMap());

        // Create SpoutMonitor
        final SpoutMonitor spoutMonitor = new SpoutMonitor(
            messageBus,
            Clock.systemUTC(),
            config,
            metricsRecorder
        );

        // call run in async thread.
        final CompletableFuture future = startSpoutMonitor(spoutMonitor);

        final DefaultVirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("Foobar");

        final DelegateSpout spout1 = new MockDelegateSpout(virtualSpoutIdentifier);
        final DelegateSpout spout2 = new MockDelegateSpout(virtualSpoutIdentifier);

        spoutMonitor.addVirtualSpout(spout1);

        try {
            expectedException.expect(SpoutAlreadyExistsException.class);
            spoutMonitor.addVirtualSpout(spout2);
        } catch (Exception exception) {
            // Ensure we cleanup appropriately.
            shutdownSpoutMonitor(spoutMonitor, future);

            // Re-throw the expected exception so the test passes.
            throw exception;
        }
    }


    /**
     * Test that we can check for the existence of a spout inside of the SpoutRunner.
     */
    @Test
    public void testHasVirtualSpout() {
        final MessageBus messageBus = new MessageBus(FifoBuffer.createDefaultInstance());

        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(Maps.newHashMap(), new MockTopologyContext());

        // Define our configuration
        final Map<String, Object> config = SpoutConfig.setDefaults(Maps.newHashMap());

        // Create SpoutMonitor
        final SpoutMonitor spoutMonitor = new SpoutMonitor(
            messageBus,
            Clock.systemUTC(),
            config,
            metricsRecorder
        );

        // call run in async thread.
        final CompletableFuture future = startSpoutMonitor(spoutMonitor);

        final DefaultVirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("Foobar");

        final DelegateSpout spout1 = new MockDelegateSpout(virtualSpoutIdentifier);

        spoutMonitor.addVirtualSpout(spout1);

        assertTrue("Spout is not in the spoutCoordinator", spoutMonitor.hasVirtualSpout(virtualSpoutIdentifier));

        assertFalse("Spout should not be in the spoutCoordinator", spoutMonitor.hasVirtualSpout(
            new DefaultVirtualSpoutIdentifier("made up spout that should not exist")
        ));

        // Close monitor.
        shutdownSpoutMonitor(spoutMonitor, future);
    }

    /**
     * Validates that SpoutRunner does not die if it catches an exception.
     * How we setup this test is pretty nasty.  Basically we submit a mock VirtualSpout
     * configured to toss an exception when SpoutMonitor pulls it from the queue BEFORE
     * it gets pushed into its own thread and started.
     *
     * We trigger this multiple times to ensure the loop keeps on trucking.
     */
    @Test
    public void testRestartsSpoutMonitorOnDeath() throws InterruptedException {
        final MessageBus messageBus = new MessageBus(FifoBuffer.createDefaultInstance());

        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(Maps.newHashMap(), new MockTopologyContext());

        // Define our configuration with reduced run time.
        final Map<String, Object> config = SpoutConfig.setDefaults(Maps.newHashMap());
        config.put(SpoutConfig.MONITOR_THREAD_INTERVAL_MS, 1000);

        // Create SpoutMonitor
        final SpoutMonitor spoutMonitor = new SpoutMonitor(
            messageBus,
            Clock.systemUTC(),
            config,
            metricsRecorder
        );

        // call run in async thread.
        final CompletableFuture future = startSpoutMonitor(spoutMonitor);

        final AtomicInteger counter = new AtomicInteger(0);

        // Create a mock VirtualSpout
        final DelegateSpout mockSpout = mock(DelegateSpout.class);

        // When we call getVirtualSpout on SpoutMonitor
        doAnswer(invocation -> {
            // Increment counter
            final int count = counter.incrementAndGet();

            // The First call is called prior to being run within the SpoutMonitors thread
            // So we trigger it to throw an exception on every 2nd call.
            // Super hacky/ugly
            if (count % 2 == 0) {
                // Kind of evil, re-submit ourselves. so we can trigger this multiple times.
                spoutMonitor.addVirtualSpout(mockSpout);

                // Throw an exception
                logger.info("About to throw an exception teehee!");
                throw new RuntimeException("my exception");
            } else {
                return new DefaultVirtualSpoutIdentifier("myId" + count);
            }
        }).when(mockSpout).getVirtualSpoutId();

        spoutMonitor.addVirtualSpout(mockSpout);

        // Wait until RUN has been called multiple times on the mock SpoutRunner
        await()
            .atMost(10, TimeUnit.SECONDS)
            .until(() -> counter.get() > 12);

        // close spout monitor
        shutdownSpoutMonitor(spoutMonitor, future);
    }

    /**
     * Test that removing a virtual spout takes it out of the coordinator.
     */
    @Test
    public void testAddAndRemoveVirtualSpout() {
        final MessageBus messageBus = new MessageBus(FifoBuffer.createDefaultInstance());

        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(Maps.newHashMap(), new MockTopologyContext());

        // Define our configuration with reduced run time.
        final Map<String, Object> config = SpoutConfig.setDefaults(Maps.newHashMap());
        config.put(SpoutConfig.MONITOR_THREAD_INTERVAL_MS, 1000);

        // Create SpoutMonitor
        final SpoutMonitor spoutMonitor = new SpoutMonitor(
            messageBus,
            Clock.systemUTC(),
            config,
            metricsRecorder
        );

        // call run in async thread.
        final CompletableFuture future = startSpoutMonitor(spoutMonitor);

        // Create a mock spout
        final DefaultVirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("Foobar");
        final DelegateSpout mockSpout = mock(DelegateSpout.class);
        when(mockSpout.getVirtualSpoutId()).thenReturn(virtualSpoutIdentifier);
        final AtomicInteger isOpenCounter = new AtomicInteger(0);

        // When open is called, we know to continue;
        doAnswer(invocation -> {
            isOpenCounter.incrementAndGet();
            return null;
        }).when(mockSpout).open();

        // Sanity test, should not be in SpoutMonitor yet.
        assertFalse("Spout should not already be in the SpoutMonitor!", spoutMonitor.hasVirtualSpout(virtualSpoutIdentifier));

        // Add it.
        spoutMonitor.addVirtualSpout(mockSpout);

        // Sanity test, should be in monitor now
        assertTrue("Spout is not in the SpoutMonitor, but should be!", spoutMonitor.hasVirtualSpout(virtualSpoutIdentifier));

        // Wait until this spout is running.
        await().until(isOpenCounter::get, equalTo(1));

        // Now that it's into the monitor, go ahead and remove it
        // Note if we hadn't waited we would have gotten an exception
        spoutMonitor.removeVirtualSpout(virtualSpoutIdentifier);

        // Need to wait for this to complete!
        await().until(() -> spoutMonitor.getExecutor().getActiveCount(), equalTo(0));

        assertFalse("Spout is still in the SpoutMonitor", spoutMonitor.hasVirtualSpout(virtualSpoutIdentifier));

        try {
            // We are going to try to remove it again, at this point it does not exist, so we expect to get an
            // exception thrown at us indicating so
            expectedException.expect(SpoutDoesNotExistException.class);
            spoutMonitor.removeVirtualSpout(virtualSpoutIdentifier);
        } catch (Exception exception) {
            // Make sure to close out SpoutCoordinator
            shutdownSpoutMonitor(spoutMonitor, future);

            // Rethrow expected exception.
            throw exception;
        }
    }

    private Map<String, Object> getDefaultConfig(int maxConcurrentSpoutInstances, long maxShutdownTime, long monitorThreadTime) {
        final Map<String, Object> topologyConfig = SpoutConfig.setDefaults(Maps.newHashMap());
        topologyConfig.put(SpoutConfig.MAX_CONCURRENT_VIRTUAL_SPOUTS, maxConcurrentSpoutInstances);
        topologyConfig.put(SpoutConfig.MAX_SPOUT_SHUTDOWN_TIME_MS, maxShutdownTime);
        topologyConfig.put(SpoutConfig.MONITOR_THREAD_INTERVAL_MS, monitorThreadTime);

        return topologyConfig;
    }

    private SpoutMonitor getDefaultMonitorInstance(final MessageBus messageBus) {
        // Define inputs
        final Clock clock = Clock.systemUTC();

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(2, 2000L, 100L);

        // Create metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, new MockTopologyContext());

        // Create instance.
        return new SpoutMonitor(
            messageBus,
            clock,
            topologyConfig,
            metricsRecorder
        );
    }

    private SpoutMonitor getDefaultMonitorInstance() {
        return getDefaultMonitorInstance(new MessageBus(new FifoBuffer()));
    }

    private CompletableFuture startSpoutMonitor(SpoutMonitor spoutMonitor) {
        if (executorService == null) {
            executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
        }

        // Sanity check
        assertEquals("Executor service should be empty", 0, executorService.getActiveCount());

        // Submit task to start
        CompletableFuture future = CompletableFuture.runAsync(spoutMonitor, executorService);

        // Wait until it actually starts.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> executorService.getActiveCount() == 1, equalTo(true));

        // return the future
        return future;
    }

    private void shutdownSpoutMonitor(final SpoutMonitor spoutMonitor, CompletableFuture asyncFuture) {
        // Call close on spout monitor
        spoutMonitor.close();
        assertFalse("Keep running should return false", spoutMonitor.keepRunning());

        // Wait for it to stop running.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(asyncFuture::isDone, equalTo(true));
    }
}