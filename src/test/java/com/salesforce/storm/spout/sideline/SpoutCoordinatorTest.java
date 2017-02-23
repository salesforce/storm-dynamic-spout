package com.salesforce.storm.spout.sideline;

import com.google.common.collect.Sets;
import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;
import org.apache.storm.tuple.Values;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

public class SpoutCoordinatorTest {

    @Test
    public void start() throws Exception {
        final List<KafkaMessage> expected = new ArrayList<>();

        final MockDelegateSidelineSpout fireHoseSpout = new MockDelegateSidelineSpout();
        final MockDelegateSidelineSpout sidelineSpout1 = new MockDelegateSidelineSpout();
        final MockDelegateSidelineSpout sidelineSpout2 = new MockDelegateSidelineSpout();

        final KafkaMessage message1 = new KafkaMessage(new TupleMessageId("topic", 1, 1L, fireHoseSpout.getConsumerId()), new Values("message1"));
        final KafkaMessage message2 = new KafkaMessage(new TupleMessageId("topic", 1, 1L, fireHoseSpout.getConsumerId()), new Values("message2"));
        final KafkaMessage message3 = new KafkaMessage(new TupleMessageId("topic", 1, 1L, fireHoseSpout.getConsumerId()), new Values("message3"));

        final Set<KafkaMessage> actual = Sets.newConcurrentHashSet();

        final SpoutCoordinator coordinator = new SpoutCoordinator(fireHoseSpout);
        coordinator.start(actual::add);

        assertEquals(1, coordinator.getTotalSpouts());

        coordinator.addSidelineSpout(sidelineSpout1);
        coordinator.addSidelineSpout(sidelineSpout2);

        await().atMost(5, TimeUnit.SECONDS).until(() -> coordinator.getTotalSpouts(), equalTo(3));

        assertEquals(3, coordinator.getTotalSpouts());

        fireHoseSpout.addMessage(message1);
        expected.add(message1);

        sidelineSpout1.addMessage(message2);
        expected.add(message2);

        fireHoseSpout.addMessage(message3);
        expected.add(message3);

        await().atMost(5, TimeUnit.SECONDS).until(() -> actual.size(), equalTo(3));

        coordinator.stop();

        System.out.println("Expected = " + expected);
        System.out.println("Actual = " + actual);

        for (KafkaMessage a : expected) {
            boolean found = false;

            for (KafkaMessage b : actual) {
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
        private boolean finished = false;
        private List<TupleMessageId> acks = new ArrayList<>();
        private List<TupleMessageId> fails = new ArrayList<>();
        private List<KafkaMessage> messages = new ArrayList<>();

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

        }

        @Override
        public KafkaMessage nextTuple() {
            if (!this.messages.isEmpty()) {
                return this.messages.remove(this.messages.size() - 1);
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
        public boolean isFinished() {
            return this.finished;
        }

        @Override
        public void finish() {
            this.finished = true;
        }

        @Override
        public String getConsumerId() {
            return consumerId;
        }
    }
}