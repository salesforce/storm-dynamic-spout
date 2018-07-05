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

package com.salesforce.storm.spout.dynamic.kafka.deserializer;

import com.google.common.base.Charsets;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test over Utf8StringDeserializer.
 */
public class Utf8StringDeserializerTest {

    /**
     * Validates that we can deserialize.
     */
    @Test
    public void testDeserializeWithConsumerRecord() {
        // Define inputs
        final String expectedKey = "This is My Key";
        @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
        final String expectedValue = "This is my message \uD83D\uDCA9";
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 34;
        final long expectedOffset = 31337L;

        // Attempt to deserialize.
        final Deserializer deserializer = new Utf8StringDeserializer();
        final Values deserializedValues = deserializer.deserialize(
            expectedTopic,
            expectedPartition,
            expectedOffset,
            expectedKey.getBytes(Charsets.US_ASCII),
            expectedValue.getBytes(Charsets.UTF_8)
        );

        assertEquals("Values has 2 entries", 2, deserializedValues.size());
        assertEquals("Got expected key", expectedKey, deserializedValues.get(0));
        assertEquals("Got expected value", expectedValue, deserializedValues.get(1));
    }

    /**
     * Validate that when a message has a null key it doesn't end violently with a NPE.
     */
    @Test
    public void testDeserializeWithNullKey() {
        final byte[] expectedKey = null;
        final String expectedValue = "Value";
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 34;
        final long expectedOffset = 31337L;

        // Attempt to deserialize.
        final Deserializer deserializer = new Utf8StringDeserializer();
        final Values deserializedValues = deserializer.deserialize(
            expectedTopic,
            expectedPartition,
            expectedOffset,
            expectedKey,
            expectedValue.getBytes(Charsets.UTF_8)
        );

        assertEquals("Values has 2 entries", 2, deserializedValues.size());
        assertEquals("Got expected key", expectedKey, deserializedValues.get(0));
        assertEquals("Got expected value", expectedValue, deserializedValues.get(1));
    }

    /**
     * Validate that when a message has a null value the whole thing returns null.
     */
    @Test
    public void testDeserializeWithNullValue() {
        final byte[] expectedKey = null;
        final byte[] expectedValue = null;
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 34;
        final long expectedOffset = 31337L;

        // Attempt to deserialize.
        final Deserializer deserializer = new Utf8StringDeserializer();
        final Values deserializedValues = deserializer.deserialize(
            expectedTopic,
            expectedPartition,
            expectedOffset,
            expectedKey,
            expectedValue
        );

        assertNull("Should have gotten null", deserializedValues);
    }
}