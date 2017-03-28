package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConsumerStateTest {
    /**
     * By default, no exceptions should be thrown.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Verifies the built ConsumerState instance cannot be modified after the fact via the builder.
     */
    @Test
    public void testImmutability() {
        final TopicPartition expectedTopicPartition = new TopicPartition("MyTopic", 12);
        final long expectedOffset = 3444L;

        final ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();

        final ConsumerState consumerState = builder
                .withPartition(expectedTopicPartition, expectedOffset)
                .build();

        // Sanity check
        assertEquals("Has expected offset", expectedOffset, (long) consumerState.getOffsetForTopicAndPartition(expectedTopicPartition));
        assertEquals("Size should be 1", 1, consumerState.size());

        // Now lets keep using the builder, should this even be legal?
        final TopicPartition topicPartition2 = new TopicPartition("DifferentTopic",23);
        final TopicPartition topicPartition3 = new TopicPartition("DifferentTopic",32);

        // Add two partitions
        builder.withPartition(topicPartition2, 23L);
        builder.withPartition(topicPartition3, 4423L);

        // Build new instance
        final ConsumerState consumerState2 = builder.build();

        // Verify the builder isn't not coupled to the built consumer state
        assertEquals("Has expected offset", expectedOffset, (long) consumerState.getOffsetForTopicAndPartition(expectedTopicPartition));
        assertEquals("Size should be 1", 1, consumerState.size());
        assertNull("Should be null", consumerState.getOffsetForTopicAndPartition(topicPartition2));
        assertNull("Should be null", consumerState.getOffsetForTopicAndPartition(topicPartition3));
        assertNotEquals("Shouldn't be equal", consumerState, consumerState2);
        assertFalse("Shouldn't be equal", consumerState.equals(consumerState2));

        // And just for completeness.
        assertEquals("Has expected offset", expectedOffset, (long) consumerState2.getOffsetForTopicAndPartition(expectedTopicPartition));
        assertEquals("Has expected offset", 23L, (long) consumerState2.getOffsetForTopicAndPartition(topicPartition2));
        assertEquals("Has expected offset", 4423L, (long) consumerState2.getOffsetForTopicAndPartition(topicPartition3));
        assertEquals("Size should be 3", 3, consumerState2.size());
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
        builder.withPartition(new TopicPartition("Topic", 0), 0L);

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
        final TopicPartition topicPartition = new TopicPartition("MyTopic", 2);
        final long offset = 23L;

        // Our null case
        final TopicPartition topicPartition2 = new TopicPartition("MyTopic", 3);


        final ConsumerState consumerState = ConsumerState.builder()
            .withPartition(topicPartition, offset)
            .build();

        // Validate not null case
        assertNotNull("Should not be null", consumerState.get(topicPartition));
        assertNotNull("Should not be null", consumerState.getOffsetForTopicAndPartition(topicPartition));
        assertEquals("Should be our offset", (Long) offset, consumerState.get(topicPartition));
        assertEquals("Should be our offset", (Long) offset, consumerState.getOffsetForTopicAndPartition(topicPartition));
        assertTrue("Should be false", consumerState.containsKey(topicPartition));

        // Validate null case
        assertNull("Should be null", consumerState.get(topicPartition2));
        assertNull("Should be null", consumerState.getOffsetForTopicAndPartition(topicPartition2));
        assertFalse("Should be false", consumerState.containsKey(topicPartition2));

        // get Keyset
        assertNotNull("Should not be null", consumerState.keySet());
        assertEquals("Should have 1 entry", 1, consumerState.keySet().size());
        assertTrue("Should contain our expected topic partition", consumerState.keySet().contains(topicPartition));

        // Get values
        assertNotNull("Should not be null", consumerState.values());
        assertEquals("Should have 1 entry", 1, consumerState.values().size());
        assertTrue("Should contain our expected topic partition", consumerState.values().contains(offset));

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
        final TopicPartition topicPartition = new TopicPartition("MyTopic", 2);
        final long offset = 23L;

        // Our null case
        final TopicPartition topicPartition2 = new TopicPartition("MyTopic", 3);


        final ConsumerState consumerState = ConsumerState.builder()
                .withPartition(topicPartition, offset)
                .build();

        // EntrySet
        assertNotNull("Should not be null", consumerState.entrySet());
        assertEquals("Should have 1 entry", 1, consumerState.entrySet().size());
        for (Map.Entry<TopicPartition, Long> entry: consumerState.entrySet()) {
            assertEquals("Key is correct", topicPartition, entry.getKey());
            assertEquals("Value is correct", (Long) offset, entry.getValue());

            // Verify cannot write, this should throw an exception
            expectedException.expect(UnsupportedOperationException.class);
            entry.setValue(offset + 1);
        }
    }

    /**
     * Verifies we cannot mutate ConsumerState.
     */
    @Test
    public void testCannotClear() {
        // Our happy case
        final TopicPartition topicPartition = new TopicPartition("MyTopic", 2);
        final long offset = 23L;

        final ConsumerState consumerState = ConsumerState.builder()
                .withPartition(topicPartition, offset)
                .build();

        expectedException.expect(UnsupportedOperationException.class);
        consumerState.clear();
    }

    /**
     * Verifies we cannot mutate ConsumerState.
     */
    @Test
    public void testCannotPut() {
        // Our happy case
        final TopicPartition topicPartition = new TopicPartition("MyTopic", 2);
        final long offset = 23L;

        final ConsumerState consumerState = ConsumerState.builder()
                .withPartition(topicPartition, offset)
                .build();

        expectedException.expect(UnsupportedOperationException.class);
        consumerState.put(new TopicPartition("MyTopic", 3), 2L);
    }

    /**
     * Verifies we cannot mutate ConsumerState.
     */
    @Test
    public void testCannotPutAll() {
        // Our happy case
        final TopicPartition topicPartition = new TopicPartition("MyTopic", 2);
        final long offset = 23L;

        final ConsumerState consumerState = ConsumerState.builder()
                .withPartition(topicPartition, offset)
                .build();

        Map<TopicPartition, Long> newMap = Maps.newHashMap();
        newMap.put(new TopicPartition("MyTopic", 3), 2L);

        expectedException.expect(UnsupportedOperationException.class);
        consumerState.putAll(newMap);
    }

    /**
     * Verifies we cannot mutate ConsumerState.
     */
    @Test
    public void testCannotRemove() {
        // Our happy case
        final TopicPartition topicPartition = new TopicPartition("MyTopic", 2);
        final long offset = 23L;

        final ConsumerState consumerState = ConsumerState.builder()
                .withPartition(topicPartition, offset)
                .build();

        expectedException.expect(UnsupportedOperationException.class);
        consumerState.remove(topicPartition);
    }
}