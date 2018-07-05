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

package com.salesforce.storm.spout.dynamic.filter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test that the serializer handles FilterChainStep instances correctly.
 */
public class SerializerTest {

    /**
     * Test that we can serialize a FilterChainStep and deserialize it successfully.
     */
    @Test
    public void serializeAndDeserialize() {
        final int number = 42;

        final FilterChainStep filterChainStep = new NumberFilter(number);

        final String serializedFilterChainStep = Serializer.serialize(
            filterChainStep
        );

        assertEquals(
            "FilterChainStep looks right serialized",
            "rO0ABXNyADZjb20uc2FsZXNmb3JjZS5zdG9ybS5zcG91dC5keW5hbWljLmZpbHRlci5OdW1iZXJGaWx0ZXKP3jjXh+gwbgIAAUkABm51bWJlcnhwAAAAKg==",
            serializedFilterChainStep
        );

        final FilterChainStep deserializedFilterChainStep = Serializer.deserialize(serializedFilterChainStep);

        assertTrue(
            "Deserialized FilterChainStep should be a NumberFilter",
            deserializedFilterChainStep instanceof NumberFilter
        );

        assertEquals(
            "Number we supplied should match the number in the FilterChainStep",
            number,
            ((NumberFilter) deserializedFilterChainStep).getNumber()
        );
    }

    /**
     * Tests that valid base64 encoded data that does not correspond to a FilterChainStep chokes at the right point.
     */
    @Test
    public void deserializeInvalidBase64String() {
        final String invalidSerializedFilterChainStep = Base64.getEncoder().encodeToString("FooBar".getBytes());

        Assertions.assertThrows(InvalidFilterChainStepException.class, () ->
            Serializer.deserialize(invalidSerializedFilterChainStep)
        );
    }

    /**
     * Tests that a non base64 string that does not correspond to a FilterChainStep chokes at the right point.
     */
    @Test
    public void deserializeInvalidJunkString() {
        // This will throw an IllegalArgumentException when passed to the base64 decoder, which is different then the previous test.
        final String invalidSerializedFilterChainStep = "FooBar";

        Assertions.assertThrows(InvalidFilterChainStepException.class, () ->
            Serializer.deserialize(invalidSerializedFilterChainStep)
        );
    }
}