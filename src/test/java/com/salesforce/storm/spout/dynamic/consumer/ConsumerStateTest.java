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

package com.salesforce.storm.spout.dynamic.consumer;

import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test that {@link ConsumerState} can be constructed, is immutable and reflects the data provided it.
 */
public class ConsumerStateTest {

    /**
     * Verifies the built ConsumerState instance cannot be modified after the fact via the builder.
     */
    @Test
    public void testImmutability() {
        final ConsumerPartition expectedTopicPartition = new ConsumerPartition("MyTopic", 12);
        final long expectedOffset = 3444L;

        final ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();

        final ConsumerState consumerState = builder
            .withPartition(expectedTopicPartition, expectedOffset)
            .build();

        // Sanity check
        assertEquals(expectedOffset, (long) consumerState.getOffsetForNamespaceAndPartition(expectedTopicPartition), "Has expected offset");
        assertEquals(1, consumerState.size(), "Size should be 1");

        // Now lets keep using the builder, should this even be legal?
        final ConsumerPartition topicPartition2 = new ConsumerPartition("DifferentTopic",23);
        final ConsumerPartition topicPartition3 = new ConsumerPartition("DifferentTopic",32);

        // Add two partitions
        builder.withPartition(topicPartition2, 23L);
        builder.withPartition(topicPartition3, 4423L);

        // Build new instance
        final ConsumerState consumerState2 = builder.build();

        // Verify the builder isn't not coupled to the built consumer state
        assertEquals(expectedOffset, (long) consumerState.getOffsetForNamespaceAndPartition(expectedTopicPartition), "Has expected offset");
        assertEquals(1, consumerState.size(), "Size should be 1");
        assertNull(consumerState.getOffsetForNamespaceAndPartition(topicPartition2), "Should be null");
        assertNull(consumerState.getOffsetForNamespaceAndPartition(topicPartition3), "Should be null");
        assertNotEquals(consumerState, consumerState2, "Shouldn't be equal");
        assertFalse(consumerState.equals(consumerState2), "Shouldn't be equal");

        // And just for completeness.
        assertEquals(
            expectedOffset,
            (long) consumerState2.getOffsetForNamespaceAndPartition(expectedTopicPartition),
            "Has expected offset"
        );
        assertEquals(23L, (long) consumerState2.getOffsetForNamespaceAndPartition(topicPartition2), "Has expected offset");
        assertEquals(4423L, (long) consumerState2.getOffsetForNamespaceAndPartition(topicPartition3), "Has expected offset");
        assertEquals(3, consumerState2.size(), "Size should be 3");
    }

    /**
     * Verifies you can't change the source long value and change the resulting ConsumerState.
     */
    @Test
    public void testImmutability_changeLongOffset() {
        ConsumerPartition expectedTopicPartition = new ConsumerPartition("MyTopic", 12);
        Long expectedOffset = 3444L;

        final ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();

        final ConsumerState consumerState = builder
            .withPartition(expectedTopicPartition, expectedOffset)
            .build();

        // Sanity check
        assertEquals(3444L, (long) consumerState.getOffsetForNamespaceAndPartition(expectedTopicPartition), "Has expected offset");
        assertEquals(1, consumerState.size(), "Size should be 1");

        // Now change our sourced Long
        expectedOffset = 2L;

        // It should still be 3444L
        assertEquals(3444L, (long) consumerState.getOffsetForNamespaceAndPartition(expectedTopicPartition), "Has expected offset");
    }

    /**
     * Tests various conditions of empty-ness.
     */
    @Test
    public void testIsEmpty() {
        final ConsumerState emptyConsumerState = ConsumerState.builder().build();

        // Newly constructed state is always empty
        assertTrue(emptyConsumerState.isEmpty(), "Should be empty");
        assertEquals(0, emptyConsumerState.size(), "Should equal 0");

        // Create consumer state with stored offset
        final ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();
        builder.withPartition(new ConsumerPartition("Topic", 0), 0L);

        final ConsumerState consumerState = builder.build();
        assertFalse(consumerState.isEmpty(), "Should NOT be empty");
        assertEquals(1, consumerState.size(), "Size should be 1");
    }

    /**
     * Test get value methods.
     */
    @Test
    public void testGet() {
        // Our happy case
        final ConsumerPartition topicPartition = new ConsumerPartition("MyTopic", 2);
        final long offset = 23L;

        // Our null case
        final ConsumerPartition topicPartition2 = new ConsumerPartition("MyTopic", 3);


        final ConsumerState consumerState = ConsumerState.builder()
            .withPartition(topicPartition, offset)
            .build();

        // Validate not null case
        assertNotNull(consumerState.get(topicPartition), "Should not be null");
        assertNotNull(consumerState.getOffsetForNamespaceAndPartition(topicPartition), "Should not be null");
        assertEquals((Long) offset, consumerState.get(topicPartition), "Should be our offset");
        assertEquals((Long) offset, consumerState.getOffsetForNamespaceAndPartition(topicPartition), "Should be our offset");
        assertTrue(consumerState.containsKey(topicPartition), "Should be false");

        // Validate null case
        assertNull(consumerState.get(topicPartition2), "Should be null");
        assertNull(consumerState.getOffsetForNamespaceAndPartition(topicPartition2), "Should be null");
        assertFalse(consumerState.containsKey(topicPartition2), "Should be false");

        // get Keyset
        assertNotNull(consumerState.keySet(), "Should not be null");
        assertEquals(1, consumerState.keySet().size(), "Should have 1 entry");
        assertTrue(consumerState.keySet().contains(topicPartition), "Should contain our expected namespace partition");

        // Get values
        assertNotNull(consumerState.values(), "Should not be null");
        assertEquals(1, consumerState.values().size(), "Should have 1 entry");
        assertTrue(consumerState.values().contains(offset), "Should contain our expected namespace partition");

        // Contains value
        assertTrue(consumerState.containsValue(offset), "Should contain offset");
        assertFalse(consumerState.containsValue(offset + 1), "Should NOT contain offset");
    }

    /**
     * Tests that entrySet() works as expected, and we are unable to modify state via it.
     */
    @Test
    public void testEntrySet() {
        // Our happy case
        final ConsumerPartition topicPartition = new ConsumerPartition("MyTopic", 2);
        final long offset = 23L;

        final ConsumerState consumerState = ConsumerState.builder()
            .withPartition(topicPartition, offset)
            .build();

        // EntrySet
        assertNotNull(consumerState.entrySet(), "Should not be null");
        assertEquals(1, consumerState.entrySet().size(), "Should have 1 entry");
        for (Map.Entry<ConsumerPartition, Long> entry : consumerState.entrySet()) {
            assertEquals(topicPartition, entry.getKey(), "Key is correct");
            assertEquals((Long) offset, entry.getValue(), "Value is correct");

            Assertions.assertThrows(UnsupportedOperationException.class, () ->
                // Verify cannot write, this should throw an exception
                entry.setValue(offset + 1)
            );
        }
    }

    /**
     * Verifies we cannot mutate ConsumerState.
     */
    @Test
    public void testCannotClear() {
        // Our happy case
        final ConsumerPartition topicPartition = new ConsumerPartition("MyTopic", 2);
        final long offset = 23L;

        final ConsumerState consumerState = ConsumerState.builder()
            .withPartition(topicPartition, offset)
            .build();

        Assertions.assertThrows(UnsupportedOperationException.class, () ->
            consumerState.clear()
        );
    }

    /**
     * Verifies we cannot mutate ConsumerState.
     */
    @Test
    public void testCannotPut() {
        // Our happy case
        final ConsumerPartition topicPartition = new ConsumerPartition("MyTopic", 2);
        final long offset = 23L;

        final ConsumerState consumerState = ConsumerState.builder()
            .withPartition(topicPartition, offset)
            .build();

        Assertions.assertThrows(UnsupportedOperationException.class, () ->
            consumerState.put(new ConsumerPartition("MyTopic", 3), 2L)
        );
    }

    /**
     * Verifies we cannot mutate ConsumerState.
     */
    @Test
    public void testCannotPutAll() {
        // Our happy case
        final ConsumerPartition topicPartition = new ConsumerPartition("MyTopic", 2);
        final long offset = 23L;

        final ConsumerState consumerState = ConsumerState.builder()
            .withPartition(topicPartition, offset)
            .build();

        Map<ConsumerPartition, Long> newMap = new HashMap<>();
        newMap.put(new ConsumerPartition("MyTopic", 3), 2L);

        Assertions.assertThrows(UnsupportedOperationException.class, () ->
            consumerState.putAll(newMap)
        );
    }

    /**
     * Verifies we cannot mutate ConsumerState.
     */
    @Test
    public void testCannotRemove() {
        // Our happy case
        final ConsumerPartition topicPartition = new ConsumerPartition("MyTopic", 2);
        final long offset = 23L;

        final ConsumerState consumerState = ConsumerState.builder()
            .withPartition(topicPartition, offset)
            .build();

        Assertions.assertThrows(UnsupportedOperationException.class, () ->
            consumerState.remove(topicPartition)
        );
    }

    /**
     * Verifies you can't modify values().
     */
    @Test
    public void testImmutabilityViaValues() {
        ConsumerPartition expectedTopicPartition = new ConsumerPartition("MyTopic", 12);
        Long expectedOffset = 3444L;

        final ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();

        final ConsumerState consumerState = builder
            .withPartition(expectedTopicPartition, expectedOffset)
            .build();

        // Sanity check
        assertEquals(3444L, (long) consumerState.getOffsetForNamespaceAndPartition(expectedTopicPartition), "Has expected offset");
        assertEquals(1, consumerState.size(), "Size should be 1");

        // Test using values
        Assertions.assertThrows(UnsupportedOperationException.class, () ->
            consumerState.values().remove(3444L)
        );
    }

    /**
     * Verifies you can't modify keySet().
     */
    @Test
    public void testImmutabilityViaKeySet() {
        ConsumerPartition expectedTopicPartition = new ConsumerPartition("MyTopic", 12);
        Long expectedOffset = 3444L;

        final ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();

        final ConsumerState consumerState = builder
            .withPartition(expectedTopicPartition, expectedOffset)
            .build();

        // Sanity check
        assertEquals(3444L, (long) consumerState.getOffsetForNamespaceAndPartition(expectedTopicPartition), "Has expected offset");
        assertEquals(1, consumerState.size(), "Size should be 1");

        // Test using values
        Assertions.assertThrows(UnsupportedOperationException.class, () ->
            consumerState.keySet().remove(expectedTopicPartition)
        );
    }
}