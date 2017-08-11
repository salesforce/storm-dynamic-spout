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
package com.salesforce.storm.spout.sideline;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.SpoutConfig;
import com.salesforce.storm.spout.sideline.metrics.LogRecorder;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.mocks.MockDelegateSpout;
import com.salesforce.storm.spout.sideline.mocks.MockTopologyContext;
import com.salesforce.storm.spout.sideline.buffer.FifoBuffer;
import org.apache.storm.tuple.Values;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SpoutCoordinatorTest {
    private static final Logger logger = LoggerFactory.getLogger(SpoutCoordinatorTest.class);

    @Test
    public void testCoordinator() throws Exception {
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

        final FifoBuffer actual = FifoBuffer.createDefaultInstance();

        // Create noop metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(Maps.newHashMap(), new MockTopologyContext());

        // Define our configuration
        final Map<String, Object> config = SpoutConfig.setDefaults(Maps.newHashMap());

        // Configure our internal operations to run frequently for our test case.
        config.put(SpoutConfig.MONITOR_THREAD_INTERVAL_MS, internalOperationsIntervalMs);
        config.put(SpoutConfig.CONSUMER_STATE_FLUSH_INTERVAL_MS, internalOperationsIntervalMs);

        // Create coordinator
        final SpoutCoordinator coordinator = new SpoutCoordinator(metricsRecorder, actual);
        coordinator.open(config);

        coordinator.addVirtualSpout(fireHoseSpout);

        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> coordinator.getTotalSpouts(), equalTo(1));

        coordinator.addVirtualSpout(sidelineSpout1);
        coordinator.addVirtualSpout(sidelineSpout2);

        logger.info("Waiting for Coordinator to detect and open() our spout instances");
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> coordinator.getTotalSpouts(), equalTo(3));
        assertEquals(3, coordinator.getTotalSpouts());
        logger.info("Coordinator now has {} spout instances", coordinator.getTotalSpouts());

        // Add 1 message to each spout
        fireHoseSpout.emitQueue.add(message1);
        expected.add(message1);

        sidelineSpout1.emitQueue.add(message2);
        expected.add(message2);

        fireHoseSpout.emitQueue.add(message3);
        expected.add(message3);

        // The SpoutRunner threads should pop these messages off.
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> actual.getUnderlyingQueue().size(), equalTo(3));

        // Ack the first two messages
        coordinator.ack(message1.getMessageId());
        coordinator.ack(message2.getMessageId());

        // Fail the third
        coordinator.fail(message3.getMessageId());

        // Wait for those to come through to the correct VirtualSpouts.
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> fireHoseSpout.ackedTupleIds.size(), equalTo(1));
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> fireHoseSpout.failedTupleIds.size(), equalTo(1));
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> sidelineSpout1.ackedTupleIds.size(), equalTo(1));

        assertTrue(
            message1.getMessageId().equals(fireHoseSpout.ackedTupleIds.toArray()[0])
        );

        assertTrue(
            message3.getMessageId().equals(fireHoseSpout.failedTupleIds.toArray()[0])
        );

        assertTrue(
            message2.getMessageId().equals(sidelineSpout1.ackedTupleIds.toArray()[0])
        );

        coordinator.close();

        logger.info("Expected = " + expected);
        logger.info("Actual = " + actual);

        for (Message a : expected) {
            boolean found = false;

            for (Message b : actual.getUnderlyingQueue()) {
                if (a.equals(b)) {
                    found = true;
                }
            }

            assertTrue("Did not find " + a, found);
        }

        assertEquals(0, coordinator.getTotalSpouts());

        // Verify the executor is terminated, and has no active tasks
        assertTrue("Executor is terminated", coordinator.getExecutor().isTerminated());
    }

    /**
     * Test that if we try to add a spout before the coordinator is open it'll blow up.
     */
    @Rule
    public ExpectedException expectedExceptionAddingSpoutBeforeOpen = ExpectedException.none();
    @Test
    public void testAddingSpoutBeforeOpen() {
        final FifoBuffer messageBuffer = FifoBuffer.createDefaultInstance();

        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(Maps.newHashMap(), new MockTopologyContext());

        // Define our configuration
        final Map<String, Object> config = SpoutConfig.setDefaults(Maps.newHashMap());

        // Create coordinator
        final SpoutCoordinator coordinator = new SpoutCoordinator(metricsRecorder, messageBuffer);

        expectedExceptionAddingSpoutBeforeOpen.expect(IllegalStateException.class);
        expectedExceptionAddingSpoutBeforeOpen.expectMessage("before it has been opened");

        coordinator.addVirtualSpout(new MockDelegateSpout());
    }

    /**
     * Test that adding a spout with the same id will throw an exception
     */
    @Rule
    public ExpectedException expectedExceptionAddDuplicateSpout = ExpectedException.none();
    @Test
    public void testAddDuplicateSpout() {
        final FifoBuffer messageBuffer = FifoBuffer.createDefaultInstance();

        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(Maps.newHashMap(), new MockTopologyContext());

        // Define our configuration
        final Map<String, Object> config = SpoutConfig.setDefaults(Maps.newHashMap());

        // Create coordinator
        final SpoutCoordinator coordinator = new SpoutCoordinator(metricsRecorder, messageBuffer);
        coordinator.open(config);

        final DefaultVirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("Foobar");

        final DelegateSpout spout1 = new MockDelegateSpout(virtualSpoutIdentifier);
        final DelegateSpout spout2 = new MockDelegateSpout(virtualSpoutIdentifier);

        coordinator.addVirtualSpout(spout1);

        expectedExceptionAddDuplicateSpout.expect(SpoutAlreadyExistsException.class);

        coordinator.addVirtualSpout(spout2);
    }

    /**
     * Test that we can check for the existence of a spout inside of the coordinator.
     */
    @Test
    public void testHasVirtualSpout() {
        final FifoBuffer messageBuffer = FifoBuffer.createDefaultInstance();

        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(Maps.newHashMap(), new MockTopologyContext());

        // Define our configuration
        final Map<String, Object> config = SpoutConfig.setDefaults(Maps.newHashMap());

        // Create coordinator
        final SpoutCoordinator coordinator = new SpoutCoordinator(metricsRecorder, messageBuffer);
        coordinator.open(config);

        final DefaultVirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("Foobar");

        final DelegateSpout spout1 = new MockDelegateSpout(virtualSpoutIdentifier);

        coordinator.addVirtualSpout(spout1);

        assertTrue("Spout is not in the coordinator", coordinator.hasVirtualSpout(virtualSpoutIdentifier));
    }
}