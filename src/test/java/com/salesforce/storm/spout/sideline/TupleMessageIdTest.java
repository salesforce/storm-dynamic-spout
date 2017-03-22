package com.salesforce.storm.spout.sideline;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
        final String expectedVirtualSpoutId = "MyVirtualSpoutId";
        final TupleMessageId tupleMessageId = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);

        // Validate TupleMessageId
        assertEquals("Got expected topic", expectedTopic, tupleMessageId.getTopic());
        assertEquals("Got expected partition", expectedPartition, tupleMessageId.getPartition());
        assertEquals("Got expected offset", expectedOffset, tupleMessageId.getOffset());
        assertEquals("Got expected virtual spout id", expectedVirtualSpoutId, tupleMessageId.getSrcVirtualSpoutId());
    }

    /**
     * Validates getTopicPartition() method.
     */
    @Test
    public void testGetTopicPartition() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedVirtualSpoutId = "MyVirtualSpoutId";
        final TupleMessageId tupleMessageId = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);

        // Validate
        final TopicPartition result = tupleMessageId.getTopicPartition();
        assertNotNull("Should not be null", result);
        assertEquals("Should have right topic", expectedTopic, result.topic());
        assertEquals("Should have right partition", expectedPartition, result.partition());
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
        final String expectedVirtualSpoutId = "MyVirtualSpoutId";
        final TupleMessageId tupleMessageId = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);

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
        final String expectedVirtualSpoutId = "MyVirtualSpoutId";
        final TupleMessageId tupleMessageId1 = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);
        final TupleMessageId tupleMessageId2 = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);

        // Validate its equal to itself
        assertTrue("Should be equal", tupleMessageId1.equals(tupleMessageId2));
        assertTrue("Should be equal", tupleMessageId2.equals(tupleMessageId1));
    }

    /**
     * Verifies not equal.
     */
    @Test
    public void testNotEqualsPartition() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedVirtualSpoutId = "MyVirtualSpoutId";
        final TupleMessageId tupleMessageId1 = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);
        final TupleMessageId tupleMessageId2 = new TupleMessageId(expectedTopic, expectedPartition + 1, expectedOffset, expectedVirtualSpoutId);

        // Validate not equal
        assertFalse("Should not be equal", tupleMessageId1.equals(tupleMessageId2));
        assertFalse("Should not be equal", tupleMessageId2.equals(tupleMessageId1));
    }

    /**
     * Verifies not equal.
     */
    @Test
    public void testNotEqualsOffset() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedVirtualSpoutId = "MyVirtualSpoutId";
        final TupleMessageId tupleMessageId1 = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);
        final TupleMessageId tupleMessageId2 = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset + 1, expectedVirtualSpoutId);

        // Validate not equal
        assertFalse("Should not be equal", tupleMessageId1.equals(tupleMessageId2));
        assertFalse("Should not be equal", tupleMessageId2.equals(tupleMessageId1));
    }

    /**
     * Verifies not equal.
     */
    @Test
    public void testNotEqualsTopic() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedVirtualSpoutId = "MyVirtualSpoutId";
        final TupleMessageId tupleMessageId1 = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);
        final TupleMessageId tupleMessageId2 = new TupleMessageId(expectedTopic + "A", expectedPartition, expectedOffset, expectedVirtualSpoutId);

        // Validate not equal
        assertFalse("Should not be equal", tupleMessageId1.equals(tupleMessageId2));
        assertFalse("Should not be equal", tupleMessageId2.equals(tupleMessageId1));
    }

    /**
     * Verifies not equal.
     */
    @Test
    public void testNotEqualsVirtualSpoutId() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedVirtualSpoutId = "MyVirtualSpoutId";
        final TupleMessageId tupleMessageId1 = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);
        final TupleMessageId tupleMessageId2 = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId + "A");

        // Validate not equal
        assertFalse("Should not be equal", tupleMessageId1.equals(tupleMessageId2));
        assertFalse("Should not be equal", tupleMessageId2.equals(tupleMessageId1));
    }

    /**
     * Verifies not equal.
     */
    @Test
    public void testNotEqualsWithNullOtherInstance() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedVirtualSpoutId = "MyVirtualSpoutId";
        final TupleMessageId tupleMessageId1 = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);
        final TupleMessageId tupleMessageId2 = null;

        // Validate not equal
        assertFalse("Should not be equal", tupleMessageId1.equals(tupleMessageId2));
    }
}