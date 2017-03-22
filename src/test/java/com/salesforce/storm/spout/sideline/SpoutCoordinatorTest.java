package com.salesforce.storm.spout.sideline;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;
import com.salesforce.storm.spout.sideline.metrics.LogRecorder;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.mocks.MockTopologyContext;
import com.salesforce.storm.spout.sideline.tupleBuffer.FIFOBuffer;
import org.apache.storm.tuple.Values;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SpoutCoordinatorTest {
    private static final Logger logger = LoggerFactory.getLogger(SpoutCoordinatorTest.class);

    @Test
    public void testCoordinator() throws Exception {
        final int waitTime = SpoutCoordinator.MONITOR_THREAD_SLEEP_MS + 200;

        final List<KafkaMessage> expected = new ArrayList<>();

        final MockDelegateSidelineSpout fireHoseSpout = new MockDelegateSidelineSpout();
        final MockDelegateSidelineSpout sidelineSpout1 = new MockDelegateSidelineSpout();
        final MockDelegateSidelineSpout sidelineSpout2 = new MockDelegateSidelineSpout();

        // Note: I set the topic here to different things largely to aide debugging the message ids later on
        final KafkaMessage message1 = new KafkaMessage(new TupleMessageId("message1", 1, 1L, fireHoseSpout.getConsumerId()), new Values("message1"));
        final KafkaMessage message2 = new KafkaMessage(new TupleMessageId("message2", 1, 1L, sidelineSpout1.getConsumerId()), new Values("message2"));
        final KafkaMessage message3 = new KafkaMessage(new TupleMessageId("message3", 1, 1L, fireHoseSpout.getConsumerId()), new Values("message3"));

        final FIFOBuffer actual = FIFOBuffer.createDefaultInstance();

        // Create noop metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(Maps.newHashMap(), new MockTopologyContext());

        // Create coordinator
        final SpoutCoordinator coordinator = new SpoutCoordinator(fireHoseSpout, metricsRecorder, actual);
        coordinator.open(Maps.newHashMap());

        assertEquals(1, coordinator.getTotalSpouts());

        coordinator.addSidelineSpout(sidelineSpout1);
        coordinator.addSidelineSpout(sidelineSpout2);

        logger.info("Waiting for Coordinator to detect and open() our spout instances");
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> coordinator.getTotalSpouts(), equalTo(3));
        assertEquals(3, coordinator.getTotalSpouts());
        logger.info("Coordinator now has {} spout instances", coordinator.getTotalSpouts());

        // Add 1 message to each spout
        fireHoseSpout.addMessage(message1);
        expected.add(message1);

        sidelineSpout1.addMessage(message2);
        expected.add(message2);

        fireHoseSpout.addMessage(message3);
        expected.add(message3);

        // The SpoutRunner threads should pop these messages off.
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> actual.getUnderlyingQueue().size(), equalTo(3));

        // Ack the first two messages
        coordinator.ack(message1.getTupleMessageId());
        coordinator.ack(message2.getTupleMessageId());

        // Fail the third
        coordinator.fail(message3.getTupleMessageId());

        // Wait for those to come thru to the correct VirtualSpouts.
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> fireHoseSpout.acks.size(), equalTo(1));
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> fireHoseSpout.fails.size(), equalTo(1));
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> sidelineSpout1.acks.size(), equalTo(1));

        assertTrue(
            message1.getTupleMessageId().equals(fireHoseSpout.acks.poll())
        );

        assertTrue(
            message3.getTupleMessageId().equals(fireHoseSpout.fails.poll())
        );

        assertTrue(
            message2.getTupleMessageId().equals(sidelineSpout1.acks.poll())
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
    }


    private static class MockDelegateSidelineSpout implements DelegateSidelineSpout {

        private static final Logger logger = LoggerFactory.getLogger(MockDelegateSidelineSpout.class);

        private String consumerId;
        private boolean requestedStop = false;
        private Queue<TupleMessageId> acks = new ConcurrentLinkedQueue<>();
        private Queue<TupleMessageId> fails = new ConcurrentLinkedQueue<>();
        private Queue<KafkaMessage> messages = new ConcurrentLinkedQueue<>();

        public MockDelegateSidelineSpout() {
            this.consumerId = this.getClass().getSimpleName() + UUID.randomUUID().toString();
            logger.info("Creating spout {}", consumerId);
        }

        public void addMessage(KafkaMessage message) {
            this.messages.add(message);
        }

        @Override
        public void open() {

        }

        @Override
        public void close() {
            logger.info("Closing spout {}", getConsumerId());
        }

        @Override
        public KafkaMessage nextTuple() {
            if (!this.messages.isEmpty()) {
                return this.messages.poll();
            }
            return null;
        }

        @Override
        public void ack(Object id) {
            this.acks.add((TupleMessageId) id);
        }

        @Override
        public void fail(Object id) {
            this.fails.add((TupleMessageId) id);
        }



        @Override
        public void flushState() {

        }

        @Override
        public void requestStop() {
            requestedStop = true;
        }

        @Override
        public boolean isStopRequested() {
            return requestedStop;
        }

        @Override
        public String getConsumerId() {
            return consumerId;
        }
    }
}