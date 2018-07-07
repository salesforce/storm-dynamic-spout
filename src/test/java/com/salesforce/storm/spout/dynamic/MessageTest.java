/*
 * Copyright (c) 2017, 2018, Salesforce.com, Inc.
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

package com.salesforce.storm.spout.dynamic;

import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");
        final MessageId expectedMessageId = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);

        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;
        final Values expectedValues = new Values(expectedValue1, expectedValue2, expectedValue3);

        // Create Message
        final Message message = new Message(expectedMessageId, expectedValues);

        // Validate TupleMessageId
        assertEquals(expectedMessageId, message.getMessageId(), "Got expected TupleMessageId");
        assertEquals(expectedTopic, message.getNamespace(), "Got expected namespace");
        assertEquals(expectedPartition, message.getPartition(), "Got expected partition");
        assertEquals(expectedOffset, message.getOffset(), "Got expected offset");
        assertFalse(message.isPermanentlyFailed(), "Should not be permanently failed");

        // Validate Values
        assertEquals(expectedValues, message.getValues(), "Got expected Values");
        assertEquals(3, message.getValues().size(), "Got expected Values count");
        assertEquals(expectedValue1, message.getValues().get(0), "Got expected Value1");
        assertEquals(expectedValue2, message.getValues().get(1), "Got expected Value2");
        assertEquals(expectedValue3, message.getValues().get(2), "Got expected Value3");
    }

    /**
     * Tests the constructor + getters.
     */
    @Test
    public void testCreatePermanentlyFailedMessage() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");
        final MessageId expectedMessageId = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);

        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;
        final Values expectedValues = new Values(expectedValue1, expectedValue2, expectedValue3);

        // Create Message
        final Message message = new Message(expectedMessageId, expectedValues);

        // Mark it as permanently failed
        final Message failedMessage = Message.createPermanentlyFailedMessage(message);

        // Validate TupleMessageId
        assertEquals(expectedMessageId, failedMessage.getMessageId(), "Got expected TupleMessageId");
        assertEquals(expectedTopic, failedMessage.getNamespace(), "Got expected namespace");
        assertEquals(expectedPartition, failedMessage.getPartition(), "Got expected partition");
        assertEquals(expectedOffset, failedMessage.getOffset(), "Got expected offset");
        assertTrue(failedMessage.isPermanentlyFailed(), "Should be permanently failed");

        // Validate Values
        assertEquals(expectedValues, failedMessage.getValues(), "Got expected Values");
        assertEquals(3, failedMessage.getValues().size(), "Got expected Values count");
        assertEquals(expectedValue1, failedMessage.getValues().get(0), "Got expected Value1");
        assertEquals(expectedValue2, failedMessage.getValues().get(1), "Got expected Value2");
        assertEquals(expectedValue3, failedMessage.getValues().get(2), "Got expected Value3");
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
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");
        final MessageId expectedMessageId = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);


        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;
        final Values expectedValues = new Values(expectedValue1, expectedValue2, expectedValue3);

        // Create Message
        final Message message = new Message(expectedMessageId, expectedValues);

        assertTrue(message.equals(message), "Should be equal");
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
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");
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
        assertTrue(message1.equals(message2), "Should be equal");
        assertTrue(message2.equals(message1), "Should be equal");
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
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");
        final MessageId expectedMessageId = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId);

        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;

        // Create Message
        final Message message1 = new Message(expectedMessageId, new Values(expectedValue1, expectedValue2, expectedValue3));
        final Message message2 = new Message(expectedMessageId, new Values(expectedValue1, expectedValue2, expectedValue3));

        // Validate
        assertTrue(message1.equals(message2), "Should be equal");
        assertTrue(message2.equals(message1), "Should be equal");
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
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;
        final Values expectedValues = new Values(expectedValue1, expectedValue2, expectedValue3);

        // Create Message
        final Message message1 = new Message(
            new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId),
            expectedValues
        );

        // Create Message
        final Message message2 = new Message(
            new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId),
            expectedValues
        );

        // Validate
        assertTrue(message1.equals(message2), "Should be equal");
        assertTrue(message2.equals(message1), "Should be equal");
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
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;

        // Create Message
        final Message message1 = new Message(
            new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId),
            new Values(expectedValue1, expectedValue2, expectedValue3)
        );

        // Create Message
        final Message message2 = new Message(
            new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId),
            new Values(expectedValue1, expectedValue2, expectedValue3)
        );

        // Validate
        assertTrue(message1.equals(message2), "Should be equal");
        assertTrue(message2.equals(message1), "Should be equal");
    }

    /**
     * Tests equality.
     */
    @Test
    public void testNotEqualsOneIsFailedDifferentInstances() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;

        // Create Message
        final Message message1 = new Message(
            new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId),
            new Values(expectedValue1, expectedValue2, expectedValue3)
        );

        // Create Message
        final Message message2 = Message.createPermanentlyFailedMessage(new Message(
            new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId),
            new Values(expectedValue1, expectedValue2, expectedValue3)
        ));

        // Validate
        assertFalse(message1.equals(message2), "Should not be equal");
        assertFalse(message2.equals(message1), "Should not be equal");
    }

    /**
     * Tests equality.
     */
    @Test
    public void testEqualsBothAreFailedDifferentInstances() {
        // Define TupleMessageId components
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 2;
        final long expectedOffset = 31337L;
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define expected values components
        final String expectedValue1 = "This is value 1";
        final String expectedValue2 = "This is value 2";
        final Long expectedValue3 = 42L;

        // Create Message
        final Message message1 = Message.createPermanentlyFailedMessage(new Message(
            new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId),
            new Values(expectedValue1, expectedValue2, expectedValue3)
        ));

        // Create Message
        final Message message2 = Message.createPermanentlyFailedMessage(new Message(
            new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId),
            new Values(expectedValue1, expectedValue2, expectedValue3)
        ));

        // Validate
        assertTrue(message1.equals(message2), "Should not be equal");
        assertTrue(message2.equals(message1), "Should not be equal");
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
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

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
        assertFalse(message1.equals(message2), "Should NOT be equal");
        assertFalse(message2.equals(message1), "Should NOT be equal");
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
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

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
        assertFalse(message1.equals(message2), "Should NOT be equal");
        assertFalse(message2.equals(message1), "Should NOT be equal");
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
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

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
        assertFalse(message1.equals(message2), "Should NOT be equal");
    }
}