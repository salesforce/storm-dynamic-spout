/*
 * Copyright (c) 2018, Salesforce.com, Inc.
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

import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
        final DefaultVirtualSpoutIdentifier expectedVirtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");
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
        final DefaultVirtualSpoutIdentifier expectedVirtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");
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
        final DefaultVirtualSpoutIdentifier expectedVirtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");
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
        final DefaultVirtualSpoutIdentifier expectedVirtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");
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
        final DefaultVirtualSpoutIdentifier expectedVirtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");
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
        final DefaultVirtualSpoutIdentifier expectedVirtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");
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
        final MessageId messageId1 = new MessageId(
            expectedTopic,
            expectedPartition,
            expectedOffset,
            new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId")
        );
        final MessageId messageId2 = new MessageId(
            expectedTopic,
            expectedPartition,
            expectedOffset,
            new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId+A")
        );

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
        final DefaultVirtualSpoutIdentifier expectedVirtualSpoutId = new DefaultVirtualSpoutIdentifier("MyVirtualSpoutId");
        final MessageId messageId1 = new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedVirtualSpoutId);
        final MessageId messageId2 = null;

        // Validate not equal
        assertFalse("Should not be equal", messageId1.equals(messageId2));
    }
}