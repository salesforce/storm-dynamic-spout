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

package com.salesforce.storm.spout.dynamic.consumer;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
        assertEquals("Has expected offset", expectedOffset, (long) consumerState.getOffsetForNamespaceAndPartition(expectedTopicPartition));
        assertEquals("Size should be 1", 1, consumerState.size());

        // Now lets keep using the builder, should this even be legal?
        final ConsumerPartition topicPartition2 = new ConsumerPartition("DifferentTopic",23);
        final ConsumerPartition topicPartition3 = new ConsumerPartition("DifferentTopic",32);

        // Add two partitions
        builder.withPartition(topicPartition2, 23L);
        builder.withPartition(topicPartition3, 4423L);

        // Build new instance
        final ConsumerState consumerState2 = builder.build();

        // Verify the builder isn't not coupled to the built consumer state
        assertEquals("Has expected offset", expectedOffset, (long) consumerState.getOffsetForNamespaceAndPartition(expectedTopicPartition));
        assertEquals("Size should be 1", 1, consumerState.size());
        assertNull("Should be null", consumerState.getOffsetForNamespaceAndPartition(topicPartition2));
        assertNull("Should be null", consumerState.getOffsetForNamespaceAndPartition(topicPartition3));
        assertNotEquals("Shouldn't be equal", consumerState, consumerState2);
        assertFalse("Shouldn't be equal", consumerState.equals(consumerState2));

        // And just for completeness.
        assertEquals(
            "Has expected offset",
            expectedOffset, (long)
            consumerState2.getOffsetForNamespaceAndPartition(expectedTopicPartition)
        );
        assertEquals("Has expected offset", 23L, (long) consumerState2.getOffsetForNamespaceAndPartition(topicPartition2));
        assertEquals("Has expected offset", 4423L, (long) consumerState2.getOffsetForNamespaceAndPartition(topicPartition3));
        assertEquals("Size should be 3", 3, consumerState2.size());
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
        assertEquals("Has expected offset", 3444L, (long) consumerState.getOffsetForNamespaceAndPartition(expectedTopicPartition));
        assertEquals("Size should be 1", 1, consumerState.size());

        // Now change our sourced Long
        expectedOffset = 2L;

        // It should still be 3444L
        assertEquals("Has expected offset", 3444L, (long) consumerState.getOffsetForNamespaceAndPartition(expectedTopicPartition));
    }

    /**
     * Tests various conditions of empty-ness.
     */
    @Test
    public void testIsEmpty() {
        final ConsumerState emptyConsumerState = ConsumerState.builder().build();

        // Newly constructed state is always empty
        assertTrue("Should be empty", emptyConsumerState.isEmpty());
        assertEquals("Should equal 0", 0, emptyConsumerState.size());

        // Create consumer state with stored offset
        final ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();
        builder.withPartition(new ConsumerPartition("Topic", 0), 0L);

        final ConsumerState consumerState = builder.build();
        assertFalse("Should NOT be empty", consumerState.isEmpty());
        assertEquals("Size should be 1", 1, consumerState.size());
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
        assertNotNull("Should not be null", consumerState.get(topicPartition));
        assertNotNull("Should not be null", consumerState.getOffsetForNamespaceAndPartition(topicPartition));
        assertEquals("Should be our offset", (Long) offset, consumerState.get(topicPartition));
        assertEquals("Should be our offset", (Long) offset, consumerState.getOffsetForNamespaceAndPartition(topicPartition));
        assertTrue("Should be false", consumerState.containsKey(topicPartition));

        // Validate null case
        assertNull("Should be null", consumerState.get(topicPartition2));
        assertNull("Should be null", consumerState.getOffsetForNamespaceAndPartition(topicPartition2));
        assertFalse("Should be false", consumerState.containsKey(topicPartition2));

        // get Keyset
        assertNotNull("Should not be null", consumerState.keySet());
        assertEquals("Should have 1 entry", 1, consumerState.keySet().size());
        assertTrue("Should contain our expected namespace partition", consumerState.keySet().contains(topicPartition));

        // Get values
        assertNotNull("Should not be null", consumerState.values());
        assertEquals("Should have 1 entry", 1, consumerState.values().size());
        assertTrue("Should contain our expected namespace partition", consumerState.values().contains(offset));

        // Contains value
        assertTrue("Should contain offset", consumerState.containsValue(offset));
        assertFalse("Should NOT contain offset", consumerState.containsValue(offset + 1));
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
        assertNotNull("Should not be null", consumerState.entrySet());
        assertEquals("Should have 1 entry", 1, consumerState.entrySet().size());
        for (Map.Entry<ConsumerPartition, Long> entry : consumerState.entrySet()) {
            assertEquals("Key is correct", topicPartition, entry.getKey());
            assertEquals("Value is correct", (Long) offset, entry.getValue());

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

        Map<ConsumerPartition, Long> newMap = Maps.newHashMap();
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
        assertEquals("Has expected offset", 3444L, (long) consumerState.getOffsetForNamespaceAndPartition(expectedTopicPartition));
        assertEquals("Size should be 1", 1, consumerState.size());

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
        assertEquals("Has expected offset", 3444L, (long) consumerState.getOffsetForNamespaceAndPartition(expectedTopicPartition));
        assertEquals("Size should be 1", 1, consumerState.size());

        // Test using values
        Assertions.assertThrows(UnsupportedOperationException.class, () ->
            consumerState.keySet().remove(expectedTopicPartition)
        );
    }
}