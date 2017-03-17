package com.salesforce.storm.spout.sideline;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Simple test over TupleMessageId.
 */
public class TupleMessageIdTest {

    /**
     * Simple test over constructor + getter methods.
     */
    @Test
    public void testConstructor() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedConsumerId = "MyConsumerId";
        final TupleMessageId tupleMessageId = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);

        // Validate TupleMessageId
        assertEquals("Got expected topic", expectedTopic, tupleMessageId.getTopic());
        assertEquals("Got expected partition", expectedPartition, tupleMessageId.getPartition());
        assertEquals("Got expected offset", expectedOffset, tupleMessageId.getOffset());
        assertEquals("Got expected consumerId", expectedConsumerId, tupleMessageId.getSrcConsumerId());
    }

    /**
     * Tests equality.
     */
    @Test
    public void testEqualsSameInstance() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedConsumerId = "MyConsumerId";
        final TupleMessageId tupleMessageId = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);

        // Validate its equal to itself
        assertTrue("Should be equal", tupleMessageId.equals(tupleMessageId));
    }

    /**
     * Tests equality.
     */
    @Test
    public void testEqualsDifferentInstances() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedConsumerId = "MyConsumerId";
        final TupleMessageId tupleMessageId1 = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);
        final TupleMessageId tupleMessageId2 = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);

        // Validate its equal to itself
        assertTrue("Should be equal", tupleMessageId1.equals(tupleMessageId2));
        assertTrue("Should be equal", tupleMessageId2.equals(tupleMessageId1));
    }

}