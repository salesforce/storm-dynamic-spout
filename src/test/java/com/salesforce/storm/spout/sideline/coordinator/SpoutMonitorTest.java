package com.salesforce.storm.spout.sideline.coordinator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;
import com.salesforce.storm.spout.sideline.tupleBuffer.FIFOBuffer;
import com.salesforce.storm.spout.sideline.tupleBuffer.TupleBuffer;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class SpoutMonitorTest {
    private static final Logger logger = LoggerFactory.getLogger(SpoutMonitorTest.class);
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
        if (!executorService.isShutdown()) {
            executorService.shutdownNow();
        }
    }

    /**
     * Tests the constructor sets things appropriately.
     */
    @Test
    public void testConstructor() {
        // Define inputs
        final Queue<DelegateSidelineSpout> newSpoutQueue = Queues.newConcurrentLinkedQueue();
        final TupleBuffer tupleBuffer = new FIFOBuffer();
        final Map<String, Queue<TupleMessageId>> ackQueue = Maps.newConcurrentMap();
        final Map<String, Queue<TupleMessageId>> failQueue = Maps.newConcurrentMap();
        final CountDownLatch latch = new CountDownLatch(0);
        final Clock clock = Clock.systemUTC();

        // Define some config params
        final int maxConcurrentSpouts = 2;
        final long maxShutdownTime = 200L;
        final long monitorInterval = 300L;

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(maxConcurrentSpouts, maxShutdownTime, monitorInterval);

        // Create instance.
        SpoutMonitor spoutMonitor = new SpoutMonitor(
            newSpoutQueue,
            tupleBuffer,
            ackQueue,
            failQueue,
            latch,
            clock,
            topologyConfig
        );

        // Call getters and validate!
        assertEquals("Clock instance is what we expect", clock, spoutMonitor.getClock());
        assertEquals("TopologyConfig looks legit", topologyConfig, spoutMonitor.getTopologyConfig());
        assertEquals("getMonitorThreadIntervalMs() returns right value", monitorInterval, spoutMonitor.getMonitorThreadIntervalMs());
        assertEquals("getMaxTerminationWaitTimeMs() returns right value", maxShutdownTime, spoutMonitor.getMaxTerminationWaitTimeMs());
        assertEquals("getMaxConcurrentVirtualSpouts() returns right value", maxConcurrentSpouts, spoutMonitor.getMaxConcurrentVirtualSpouts());
        assertTrue("KeepRunning should default to true", spoutMonitor.keepRunning());

        // Validate executor
        final ThreadPoolExecutor executor = spoutMonitor.getExecutor();
        assertNotNull("Executor should be not null", executor);
        assertEquals("Should have max threads set", maxConcurrentSpouts, executor.getMaximumPoolSize());

        // Close up shop.
        spoutMonitor.close();

        // Verify that executor service is terminated
        assertTrue("Executor service is terminated", spoutMonitor.getExecutor().isTerminated());
        assertEquals("ExecutorService has no running threads", 0, spoutMonitor.getExecutor().getActiveCount());
    }

    /**
     * Simple smoke test starting the monitor in a thread, and then asking it to close.
     */
    @Test
    public void testStartAndCloseSmokeTest() throws InterruptedException, ExecutionException {
        // Create instance.
        SpoutMonitor spoutMonitor = getDefaultMonitorInstance();

        // Define how long to wait for async operations
        final long testWaitTime = (spoutMonitor.getMonitorThreadIntervalMs() * 2) + 10;

        // call run in async thread.
        CompletableFuture future = startSpoutMonitor(spoutMonitor);

        // Wait for it to fire up
        Thread.sleep(testWaitTime);

        // Now validate we have no spouts submitted
        assertEquals("Should have no spouts", 0, spoutMonitor.getTotalSpouts());

        // validate the executor is running nothing
        assertEquals("Should have 0 running task", 0, spoutMonitor.getExecutor().getActiveCount());

        // Close the monitor
        spoutMonitor.close();

        // Wait for it to stop running.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(future::isDone, equalTo(true));

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
        SpoutMonitor spoutMonitor = getDefaultMonitorInstance();

        // Define how long to wait for async operations
        final long testWaitTime = (spoutMonitor.getMonitorThreadIntervalMs() * 2) + 10;

        // Our new spout queue
        Queue<DelegateSidelineSpout> newSpoutQueue = spoutMonitor.getNewSpoutQueue();

        // call run in async thread.
        CompletableFuture future = startSpoutMonitor(spoutMonitor);

        // Wait for it to fire up
        Thread.sleep(testWaitTime);

        // Sanity test - Now validate we have no spouts submitted
        assertEquals("Should have no spouts", 0, spoutMonitor.getTotalSpouts());

        // Create a mock spout
        DelegateSidelineSpout mockSpout = mock(DelegateSidelineSpout.class);
        when(mockSpout.getVirtualSpoutId()).thenReturn("MySpoutId");

        // Add it to our queue
        newSpoutQueue.add(mockSpout);

        // wait for it to be picked up
        // This means our queue should go to 0
        await()
                .atMost(maxWaitTime, TimeUnit.SECONDS)
                .until(newSpoutQueue::isEmpty, equalTo(true));

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
        spoutMonitor.close();

        // Wait for it to stop running.
        await()
                .atMost(maxWaitTime, TimeUnit.SECONDS)
                .until(future::isDone, equalTo(true));

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
        SpoutMonitor spoutMonitor = getDefaultMonitorInstance();

        // Define how long to wait for async operations
        final long testWaitTime = (spoutMonitor.getMonitorThreadIntervalMs() * 10);

        // Our new spout queue
        Queue<DelegateSidelineSpout> newSpoutQueue = spoutMonitor.getNewSpoutQueue();

        // call run in async thread.
        CompletableFuture future = startSpoutMonitor(spoutMonitor);

        // Wait for it to fire up
        Thread.sleep(testWaitTime);

        // Sanity test - Now validate we have no spouts submitted
        assertEquals("Should have no spouts", 0, spoutMonitor.getTotalSpouts());

        // Create a mock spout
        MockDelegateSidelineSpout mockSpout = new MockDelegateSidelineSpout("MySpoutId");
        mockSpout.requestedStop = false;

        // Add it to our queue
        newSpoutQueue.add(mockSpout);

        // wait for it to be picked up
        // This means our queue should go to 0
        await()
                .atMost(maxWaitTime, TimeUnit.SECONDS)
                .until(newSpoutQueue::isEmpty, equalTo(true));

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
        spoutMonitor.close();

        // Wait for it to stop running.
        await()
                .atMost(maxWaitTime, TimeUnit.SECONDS)
                .until(future::isDone, equalTo(true));

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
        SpoutMonitor spoutMonitor = getDefaultMonitorInstance();

        // Define how long to wait for async operations
        final long testWaitTime = (spoutMonitor.getMonitorThreadIntervalMs() * 10);

        final int maxConccurentInstances = (int) spoutMonitor.getTopologyConfig().get(SidelineSpoutConfig.MAX_CONCURRENT_VIRTUAL_SPOUTS);

        // Lets create some virtual spouts
        List<MockDelegateSidelineSpout> mockSpouts = Lists.newArrayList();
        for (int x=0; x<maxConccurentInstances + 2; x++) {
            mockSpouts.add(new MockDelegateSidelineSpout("SpoutInstance" + x));
        }

        // Our new spout queue
        Queue<DelegateSidelineSpout> newSpoutQueue = spoutMonitor.getNewSpoutQueue();

        // call run in async thread.
        CompletableFuture future = startSpoutMonitor(spoutMonitor);

        // Wait for it to fire up
        Thread.sleep(testWaitTime);

        // Sanity test - Now validate we have no spouts submitted
        assertEquals("Should have no spouts", 0, spoutMonitor.getTotalSpouts());

        // Add all of our spouts to the queue.
        newSpoutQueue.addAll(mockSpouts);

        // wait for it to be picked up
        // This means our queue should go to 0
        await()
                .atMost(maxWaitTime, TimeUnit.SECONDS)
                .until(newSpoutQueue::isEmpty, equalTo(true));

        // Wait for spout count to increase to the number of spout instances we submitted.
        await()
                .atMost(maxWaitTime, TimeUnit.SECONDS)
                .until(spoutMonitor::getTotalSpouts, equalTo(mockSpouts.size()));

        // Now the executor should only run a certain number
        assertEquals("Only configured max running concurrently", maxConccurentInstances, spoutMonitor.getExecutor().getActiveCount());

        // The difference should be queued
        assertEquals("Should have some queued instances", (mockSpouts.size() - maxConccurentInstances), spoutMonitor.getExecutor().getQueue().size());

        // Add additional sleep time, just so logs show up
        Thread.sleep(testWaitTime);

        // On spouts that should have run
        for (int x=0; x<maxConccurentInstances; x++) {
            final MockDelegateSidelineSpout mockSpout = mockSpouts.get(x);
            assertTrue("open() should have been called", mockSpout.wasOpenCalled);
        }

        // Now call close on spout monitor
        logger.info("Starting to close spout monitor...");

        // Close the monitor
        spoutMonitor.close();

        // Wait for it to stop running.
        await()
                .atMost(maxWaitTime, TimeUnit.SECONDS)
                .until(future::isDone, equalTo(true));

        // Verify close was called on running spouts
        for (int x=0; x<mockSpouts.size(); x++) {
            final MockDelegateSidelineSpout mockSpout = mockSpouts.get(x);

            // If it was a running spout instance
            if (x < maxConccurentInstances) {
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
        SpoutMonitor spoutMonitor = getDefaultMonitorInstance();

        // Define how long to wait for async operations
        final long testWaitTime = (spoutMonitor.getMonitorThreadIntervalMs() * 10);

        final int maxConccurentInstances = (int) spoutMonitor.getTopologyConfig().get(SidelineSpoutConfig.MAX_CONCURRENT_VIRTUAL_SPOUTS);

        // Lets create some virtual spouts
        List<MockDelegateSidelineSpout> mockSpouts = Lists.newArrayList();
        for (int x=0; x<maxConccurentInstances + 1; x++) {
            mockSpouts.add(new MockDelegateSidelineSpout("SpoutInstance" + x));
        }

        // Our new spout queue
        Queue<DelegateSidelineSpout> newSpoutQueue = spoutMonitor.getNewSpoutQueue();

        // call run in async thread.
        CompletableFuture future = startSpoutMonitor(spoutMonitor);

        // Wait for it to fire up
        Thread.sleep(testWaitTime);

        // Sanity test - Now validate we have no spouts submitted
        assertEquals("Should have no spouts", 0, spoutMonitor.getTotalSpouts());

        // Add all of our spouts to the queue.
        newSpoutQueue.addAll(mockSpouts);

        // wait for it to be picked up
        // This means our queue should go to 0
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(newSpoutQueue::isEmpty, equalTo(true));

        // Wait for spout count to increase to the number of spout instances we submitted.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(spoutMonitor::getTotalSpouts, equalTo(mockSpouts.size()));

        // Now the executor should only run a certain number
        assertEquals("Only configured max running concurrently", maxConccurentInstances, spoutMonitor.getExecutor().getActiveCount());

        // The difference should be queued
        assertEquals("Should have some queued instances", (mockSpouts.size() - maxConccurentInstances), spoutMonitor.getExecutor().getQueue().size());

        // Add additional sleep time, just so logs show up
        Thread.sleep(testWaitTime);

        // On spouts that should have run
        for (int x=0; x<maxConccurentInstances; x++) {
            final MockDelegateSidelineSpout mockSpout = mockSpouts.get(x);
            assertTrue("open() should have been called", mockSpout.wasOpenCalled);
        }

        final MockDelegateSidelineSpout notStartedSpout = mockSpouts.get(mockSpouts.size() - 1);
        assertFalse("Should not have called open on our spout thats not running", notStartedSpout.wasOpenCalled);

        // Now stop the first instance
        mockSpouts.get(0).requestStop();

        // Wait for it to be closed
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> {
                return mockSpouts.get(0).wasCloseCalled;
            }, equalTo(true));

        // Our not started instance should now start...
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(() -> {
                    return notStartedSpout.wasOpenCalled;
            }, equalTo(true));

        // Now call close on spout monitor
        logger.info("Starting to close spout monitor...");

        // Close the monitor
        spoutMonitor.close();

        // Wait for it to stop running.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(future::isDone, equalTo(true));

        // Verify close was called on all spouts
        for (MockDelegateSidelineSpout mockSpout : mockSpouts) {
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
        // Create instance.
        SpoutMonitor spoutMonitor = getDefaultMonitorInstance();

        // Define how long to wait for async operations
        final long testWaitTime = (spoutMonitor.getMonitorThreadIntervalMs() * 10);

        // Our new spout queue
        Queue<DelegateSidelineSpout> newSpoutQueue = spoutMonitor.getNewSpoutQueue();

        // call run in async thread.
        CompletableFuture future = startSpoutMonitor(spoutMonitor);

        // Wait for it to fire up
        Thread.sleep(testWaitTime);

        // Sanity test - Now validate we have no spouts submitted
        assertEquals("Should have no spouts", 0, spoutMonitor.getTotalSpouts());

        // Create a mock spout
        MockDelegateSidelineSpout mockSpout = new MockDelegateSidelineSpout("MySpoutId");
        mockSpout.requestedStop = false;

        // Add it to our queue
        newSpoutQueue.add(mockSpout);

        // wait for it to be picked up
        // This means our queue should go to 0
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(newSpoutQueue::isEmpty, equalTo(true));

        // Wait for spout count to increase
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(spoutMonitor::getTotalSpouts, equalTo(1));

        // Verify open was called
        assertTrue("open() should have been called", mockSpout.wasOpenCalled);

        // Tell the spout to throw an exception.
        mockSpout.exceptionToThrow = new RuntimeException("This is my runtime exception");

        // Wait for spout count to decrease
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(spoutMonitor::getTotalSpouts, equalTo(0));

        // Close the monitor
        spoutMonitor.close();

        // Wait for it to stop running.
        await()
            .atMost(maxWaitTime, TimeUnit.SECONDS)
            .until(future::isDone, equalTo(true));

        // Verify closed was called on the spout instance?
        //assertTrue("Close() should have been called", mockSpout.wasCloseCalled);

        // Verify spout instance was re-started?

        // Verify that executor service is terminated
        assertTrue("Executor service is terminated", spoutMonitor.getExecutor().isTerminated());
        assertEquals("ExecutorService has no running threads", 0, spoutMonitor.getExecutor().getActiveCount());
    }

    private Map<String, Object> getDefaultConfig(int maxConcurrentSpoutInstances, long maxShutdownTime, long monitorThreadTime) {
        final Map<String, Object> topologyConfig = SidelineSpoutConfig.setDefaults(Maps.newHashMap());
        topologyConfig.put(SidelineSpoutConfig.MAX_CONCURRENT_VIRTUAL_SPOUTS, maxConcurrentSpoutInstances);
        topologyConfig.put(SidelineSpoutConfig.MAX_SPOUT_SHUTDOWN_TIME_MS, maxShutdownTime);
        topologyConfig.put(SidelineSpoutConfig.MONITOR_THREAD_INTERVAL_MS, monitorThreadTime);

        return topologyConfig;
    }

    private SpoutMonitor getDefaultMonitorInstance() {
        // Define inputs
        final Queue<DelegateSidelineSpout> newSpoutQueue = Queues.newConcurrentLinkedQueue();
        final TupleBuffer tupleBuffer = new FIFOBuffer();
        final Map<String, Queue<TupleMessageId>> ackQueue = Maps.newConcurrentMap();
        final Map<String, Queue<TupleMessageId>> failQueue = Maps.newConcurrentMap();
        final CountDownLatch latch = new CountDownLatch(0);
        final Clock clock = Clock.systemUTC();

        // Create config
        final Map<String, Object> topologyConfig = getDefaultConfig(2, 2000L, 100L);

        // Create instance.
        SpoutMonitor spoutMonitor = new SpoutMonitor(
                newSpoutQueue,
                tupleBuffer,
                ackQueue,
                failQueue,
                latch,
                clock,
                topologyConfig
        );

        return spoutMonitor;
    }

    static class MockDelegateSidelineSpout implements DelegateSidelineSpout {
        final String virtualSpoutId;
        volatile boolean requestedStop = false;
        volatile boolean wasOpenCalled = false;
        volatile boolean wasCloseCalled = false;
        volatile RuntimeException exceptionToThrow = null;

        MockDelegateSidelineSpout(final String virtualSpoutId) {
            this.virtualSpoutId = virtualSpoutId;
        }

        @Override
        public void open() {
            wasOpenCalled = true;
        }

        @Override
        public void close() {
            wasCloseCalled = true;
        }

        @Override
        public KafkaMessage nextTuple() {
            if (exceptionToThrow != null) {
                throw exceptionToThrow;
            }
            return null;
        }

        @Override
        public void ack(Object msgId) {

        }

        @Override
        public void fail(Object msgId) {

        }

        @Override
        public String getVirtualSpoutId() {
            return virtualSpoutId;
        }

        @Override
        public void flushState() {

        }

        @Override
        public synchronized void requestStop() {
            requestedStop = true;
        }

        @Override
        public synchronized boolean isStopRequested() {
            return requestedStop;
        }
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
            .until(() -> {
                return executorService.getActiveCount() == 1;
            }, equalTo(true));

        // return the future
        return future;
    }
}