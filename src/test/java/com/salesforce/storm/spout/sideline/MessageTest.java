package com.salesforce.storm.spout.sideline;

import org.apache.storm.tuple.Values;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Simple test around Message.
 */
public class MessageTest {

    /**
     * Tests the constructor + getters.
     */
    @Test
    public void testConstructor() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedConsumerId = "MyConsumerId";
        final MessageId expectedMessageId = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);


        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;
        final Values expectedValues = new Values(expectedValue1, expectedValue2, expectedValue3);

        // Create Message
        final Message message = new Message(expectedMessageId, expectedValues);

        // Validate TupleMessageId
        assertEquals("Got expected TupleMessageId", expectedMessageId, message.getMessageId());
        assertEquals("Got expected namespace", expectedTopic, message.getNamespace());
        assertEquals("Got expected partition", expectedPartition, message.getPartition());
        assertEquals("Got expected offset", expectedOffset, message.getOffset());

        // Validate Values
        assertEquals("Got expected Values", expectedValues, message.getValues());
        assertEquals("Got expected Values count", 3, message.getValues().size());
        assertEquals("Got expected Value1", expectedValue1, message.getValues().get(0));
        assertEquals("Got expected Value2", expectedValue2, message.getValues().get(1));
        assertEquals("Got expected Value3", expectedValue3, message.getValues().get(2));
    }

    /**
     * Ensures that the same instance is equal to itself.
     */
    @Test
    public void testEqualsSameInstance() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedConsumerId = "MyConsumerId";
        final MessageId expectedMessageId = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);


        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;
        final Values expectedValues = new Values(expectedValue1, expectedValue2, expectedValue3);

        // Create Message
        final Message message = new Message(expectedMessageId, expectedValues);

        assertTrue("Should be equal", message.equals(message));
    }

    /**
     * Tests equality.
     */
    @Test
    public void testEqualsDifferentInstancesSameInnerObjects() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedConsumerId = "MyConsumerId";
        final MessageId expectedMessageId = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);


        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;
        final Values expectedValues = new Values(expectedValue1, expectedValue2, expectedValue3);

        // Create Message
        final Message message1 = new Message(expectedMessageId, expectedValues);
        final Message message2 = new Message(expectedMessageId, expectedValues);

        // Validate
        assertTrue("Should be equal", message1.equals(message2));
        assertTrue("Should be equal", message2.equals(message1));
    }

    /**
     * Tests equality.
     */
    @Test
    public void testEqualsDifferentInstancesSameInnerMessageId() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedConsumerId = "MyConsumerId";
        final MessageId expectedMessageId = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);

        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;

        // Create Message
        final Message message1 = new Message(expectedMessageId, new Values(expectedValue1, expectedValue2, expectedValue3));
        final Message message2 = new Message(expectedMessageId, new Values(expectedValue1, expectedValue2, expectedValue3));

        // Validate
        assertTrue("Should be equal", message1.equals(message2));
        assertTrue("Should be equal", message2.equals(message1));
    }

    /**
     * Tests equality.
     */
    @Test
    public void testEqualsDifferentInstancesSameValueInstances() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedConsumerId = "MyConsumerId";

        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;
        final Values expectedValues = new Values(expectedValue1, expectedValue2, expectedValue3);

        // Create Message
        final Message message1 = new Message(new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId), expectedValues);

        // Create Message
        final Message message2 = new Message(new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId), expectedValues);

        // Validate
        assertTrue("Should be equal", message1.equals(message2));
        assertTrue("Should be equal", message2.equals(message1));
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

        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;

        // Create Message
        final Message message1 = new Message(new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId), new Values(expectedValue1, expectedValue2, expectedValue3));

        // Create Message
        final Message message2 = new Message(new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId), new Values(expectedValue1, expectedValue2, expectedValue3));

        // Validate
        assertTrue("Should be equal", message1.equals(message2));
        assertTrue("Should be equal", message2.equals(message1));
    }

    /**
     * Tests equality when not equal tuple message Ids.
     */
    @Test
    public void testNotEqualsDifferentTupleMessageIds() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedConsumerId = "MyConsumerId";

        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;

        // Create messageIds that are different
        final MessageId messageId1 = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);
        final MessageId messageId2 = new MessageId(expectedTopic, expectedPartition, expectedOffset + 1, expectedConsumerId);

        // Create values that are the same
        final Values values1 = new Values(expectedValue1, expectedValue2, expectedValue3);
        final Values values2 = new Values(expectedValue1, expectedValue2, expectedValue3);

        // Create Message
        final Message message1 = new Message(messageId1, values1);

        // Create Message
        final Message message2 = new Message(messageId2, values2);

        // Validate
        assertFalse("Should NOT be equal", message1.equals(message2));
        assertFalse("Should NOT be equal", message2.equals(message1));
    }

    /**
     * Tests equality when not equal values.
     */
    @Test
    public void testNotEqualsDifferentValues() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedConsumerId = "MyConsumerId";

        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;

        // Create messageIds that are the same
        final MessageId messageId1 = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);
        final MessageId messageId2 = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);

        // Create values that are different
        final Values values1 = new Values(expectedValue1, expectedValue2, expectedValue3);
        final Values values2 = new Values(expectedValue1, expectedValue2);

        // Create Message
        final Message message1 = new Message(messageId1, values1);

        // Create Message
        final Message message2 = new Message(messageId2, values2);

        // Validate
        assertFalse("Should NOT be equal", message1.equals(message2));
        assertFalse("Should NOT be equal", message2.equals(message1));
    }

    /**
     * Tests equality when not equal values.
     */
    @Test
    public void testNotEqualsAgainstNull() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final String expectedConsumerId = "MyConsumerId";

        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;

        // Create messageId
        final MessageId messageId1 = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);

        // Create values
        final Values values1 = new Values(expectedValue1, expectedValue2, expectedValue3);

        // Create Message
        final Message message1 = new Message(messageId1, values1);

        // Create Message that is null
        final Message message2 = null;

        // Validate
        assertFalse("Should NOT be equal", message1.equals(message2));
    }
}