package com.salesforce.storm.spout.sideline;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Simple test over MessageId.
 */
public class MessageIdTest {

    /**
     * Simple test over constructor + getter methods.
     */
    @Test
    public void testConstructor() {
        // Define MessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final VirtualSpoutIdentifier expectedVirtualSpoutId = new VirtualSpoutIdentifier("MyVirtualSpoutId");
        final MessageId messageId = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);

        // Validate MessageId
        assertEquals("Got expected namespace", expectedTopic, messageId.getNamespace());
        assertEquals("Got expected partition", expectedPartition, messageId.getPartition());
        assertEquals("Got expected offset", expectedOffset, messageId.getOffset());
        assertEquals("Got expected virtual spout id", expectedVirtualSpoutId, messageId.getSrcVirtualSpoutId());
    }

    /**
     * Tests equality.
     */
    @Test
    public void testEqualsSameInstance() {
        // Define MessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final VirtualSpoutIdentifier expectedVirtualSpoutId = new VirtualSpoutIdentifier("MyVirtualSpoutId");
        final MessageId messageId = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);

        // Validate its equal to itself
        assertTrue("Should be equal", messageId.equals(messageId));
    }

    /**
     * Tests equality.
     */
    @Test
    public void testEqualsDifferentInstances() {
        // Define MessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final VirtualSpoutIdentifier expectedVirtualSpoutId = new VirtualSpoutIdentifier("MyVirtualSpoutId");
        final MessageId messageId1 = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);
        final MessageId messageId2 = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);

        // Validate its equal to itself
        assertTrue("Should be equal", messageId1.equals(messageId2));
        assertTrue("Should be equal", messageId2.equals(messageId1));
    }

    /**
     * Verifies not equal.
     */
    @Test
    public void testNotEqualsPartition() {
        // Define MessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final VirtualSpoutIdentifier expectedVirtualSpoutId = new VirtualSpoutIdentifier("MyVirtualSpoutId");
        final MessageId messageId1 = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);
        final MessageId messageId2 = new MessageId(expectedTopic, expectedPartition + 1, expectedOffset, expectedVirtualSpoutId);

        // Validate not equal
        assertFalse("Should not be equal", messageId1.equals(messageId2));
        assertFalse("Should not be equal", messageId2.equals(messageId1));
    }

    /**
     * Verifies not equal.
     */
    @Test
    public void testNotEqualsOffset() {
        // Define MessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final VirtualSpoutIdentifier expectedVirtualSpoutId = new VirtualSpoutIdentifier("MyVirtualSpoutId");
        final MessageId messageId1 = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);
        final MessageId messageId2 = new MessageId(expectedTopic, expectedPartition, expectedOffset + 1, expectedVirtualSpoutId);

        // Validate not equal
        assertFalse("Should not be equal", messageId1.equals(messageId2));
        assertFalse("Should not be equal", messageId2.equals(messageId1));
    }

    /**
     * Verifies not equal.
     */
    @Test
    public void testNotEqualsTopic() {
        // Define MessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final VirtualSpoutIdentifier expectedVirtualSpoutId = new VirtualSpoutIdentifier("MyVirtualSpoutId");
        final MessageId messageId1 = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);
        final MessageId messageId2 = new MessageId(expectedTopic + "A", expectedPartition, expectedOffset, expectedVirtualSpoutId);

        // Validate not equal
        assertFalse("Should not be equal", messageId1.equals(messageId2));
        assertFalse("Should not be equal", messageId2.equals(messageId1));
    }

    /**
     * Verifies not equal.
     */
    @Test
    public void testNotEqualsVirtualSpoutId() {
        // Define MessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final MessageId messageId1 = new MessageId(expectedTopic, expectedPartition, expectedOffset, new VirtualSpoutIdentifier("MyVirtualSpoutId"));
        final MessageId messageId2 = new MessageId(expectedTopic, expectedPartition, expectedOffset, new VirtualSpoutIdentifier("MyVirtualSpoutId+A"));

        // Validate not equal
        assertFalse("Should not be equal", messageId1.equals(messageId2));
        assertFalse("Should not be equal", messageId2.equals(messageId1));
    }

    /**
     * Verifies not equal.
     */
    @Test
    public void testNotEqualsWithNullOtherInstance() {
        // Define MessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final VirtualSpoutIdentifier expectedVirtualSpoutId = new VirtualSpoutIdentifier("MyVirtualSpoutId");
        final MessageId messageId1 = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);
        final MessageId messageId2 = null;

        // Validate not equal
        assertFalse("Should not be equal", messageId1.equals(messageId2));
    }
}