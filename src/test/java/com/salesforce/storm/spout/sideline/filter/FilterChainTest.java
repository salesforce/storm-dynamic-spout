package com.salesforce.storm.spout.sideline.filter;

import com.salesforce.storm.spout.sideline.Message;
import com.salesforce.storm.spout.sideline.MessageId;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilterChainTest {

    /**
     * Test that each step of a chain is processed using a string
     */
    @Test
    public void testChain() {
        final Message message1 = new Message(
            new MessageId("foobar", 1, 0L, "FakeConsumer"),
            new Values(1)
        );

        final Message message2 = new Message(
            new MessageId("foobar", 1, 0L, "FakeConsumer"),
            new Values(2)
        );

        final Message message3 = new Message(
            new MessageId("foobar", 1, 0L, "FakeConsumer"),
            new Values(3)
        );

        final Message message4 = new Message(
            new MessageId("foobar", 1, 0L, "FakeConsumer"),
            new Values(4)
        );

        final Message message5 = new Message(
            new MessageId("foobar", 1, 0L, "FakeConsumer"),
            new Values(5)
        );

        final FilterChain filterChain = new FilterChain()
            .addStep(new SidelineRequestIdentifier(), new NumberFilter(2))
            .addStep(new SidelineRequestIdentifier(), new NumberFilter(4))
            .addStep(new SidelineRequestIdentifier(), new NumberFilter(5))
        ;

        assertTrue(filterChain.filter(message2));
        assertTrue(filterChain.filter(message4));
        assertTrue(filterChain.filter(message5));

        assertFalse(filterChain.filter(message1));
        assertFalse(filterChain.filter(message3));
    }

    /**
     * Test that each step of a chain is processed using a string
     */
    @Test
    public void testNegatingChain() {
        final Message message1 = new Message(
            new MessageId("foobar", 1, 0L, "FakeConsumer"),
            new Values(1)
        );

        final Message message2 = new Message(
            new MessageId("foobar", 1, 0L, "FakeConsumer"),
            new Values(2)
        );

        final FilterChain filterChain = new FilterChain()
            .addStep(new SidelineRequestIdentifier(), new NegatingFilterChainStep(new NumberFilter(2)))
        ;

        assertTrue(filterChain.filter(message1));

        assertFalse(filterChain.filter(message2));
    }

    public static class NumberFilter implements FilterChainStep {

        final private int number;

        public NumberFilter(final int number) {
            this.number = number;
        }

        public boolean filter(Message message) {
            Integer messageNumber = (Integer) message.getValues().get(0);
            // Filter them if they don't match, in other words "not" equals
            return messageNumber.equals(number);
        }
    }
}
