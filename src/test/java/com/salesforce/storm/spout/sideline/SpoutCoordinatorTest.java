package com.salesforce.storm.spout.sideline;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.metrics.LogRecorder;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.mocks.MockDelegateSidelineSpout;
import com.salesforce.storm.spout.sideline.mocks.MockTopologyContext;
import com.salesforce.storm.spout.sideline.tupleBuffer.FIFOBuffer;
import org.apache.storm.tuple.Values;
import org.junit.Test;
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

        final List<KafkaMessage> expected = Lists.newArrayList();

        final MockDelegateSidelineSpout fireHoseSpout = new MockDelegateSidelineSpout();
        final MockDelegateSidelineSpout sidelineSpout1 = new MockDelegateSidelineSpout();
        final MockDelegateSidelineSpout sidelineSpout2 = new MockDelegateSidelineSpout();

        // Note: I set the topic here to different things largely to aide debugging the message ids later on
        final KafkaMessage message1 = new KafkaMessage(new TupleMessageId("message1", 1, 1L, fireHoseSpout.getVirtualSpoutId()), new Values("message1"));
        final KafkaMessage message2 = new KafkaMessage(new TupleMessageId("message2", 1, 1L, sidelineSpout1.getVirtualSpoutId()), new Values("message2"));
        final KafkaMessage message3 = new KafkaMessage(new TupleMessageId("message3", 1, 1L, fireHoseSpout.getVirtualSpoutId()), new Values("message3"));

        final FIFOBuffer actual = FIFOBuffer.createDefaultInstance();

        // Create noop metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(Maps.newHashMap(), new MockTopologyContext());

        // Define our configuration
        Map<String, Object> config = SidelineSpoutConfig.setDefaults(Maps.newHashMap());

        // Configure our internal operations to run frequently for our test case.
        config.put(SidelineSpoutConfig.MONITOR_THREAD_INTERVAL_MS, internalOperationsIntervalMs);
        config.put(SidelineSpoutConfig.CONSUMER_STATE_FLUSH_INTERVAL_MS, internalOperationsIntervalMs);

        // Create coordinator
        final SpoutCoordinator coordinator = new SpoutCoordinator(fireHoseSpout, metricsRecorder, actual);
        coordinator.open(config);

        assertEquals(1, coordinator.getTotalSpouts());

        coordinator.addSidelineSpout(sidelineSpout1);
        coordinator.addSidelineSpout(sidelineSpout2);

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
        coordinator.ack(message1.getTupleMessageId());
        coordinator.ack(message2.getTupleMessageId());

        // Fail the third
        coordinator.fail(message3.getTupleMessageId());

        // Wait for those to come through to the correct VirtualSpouts.
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> fireHoseSpout.ackedTupleIds.size(), equalTo(1));
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> fireHoseSpout.failedTupleIds.size(), equalTo(1));
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> sidelineSpout1.ackedTupleIds.size(), equalTo(1));

        assertTrue(
            message1.getTupleMessageId().equals(fireHoseSpout.ackedTupleIds.toArray()[0])
        );

        assertTrue(
            message3.getTupleMessageId().equals(fireHoseSpout.failedTupleIds.toArray()[0])
        );

        assertTrue(
            message2.getTupleMessageId().equals(sidelineSpout1.ackedTupleIds.toArray()[0])
        );

        coordinator.close();

        logger.info("Expected = " + expected);
        logger.info("Actual = " + actual);

        for (KafkaMessage a : expected) {
            boolean found = false;

            for (KafkaMessage b : actual.getUnderlyingQueue()) {
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
}