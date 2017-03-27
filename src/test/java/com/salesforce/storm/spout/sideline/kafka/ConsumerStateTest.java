package com.salesforce.storm.spout.sideline.kafka;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.*;

public class ConsumerStateTest {

    /**
     * Tests various conditions of empty-ness.
     */
    @Test
    public void testIsEmpty() {
        final ConsumerState emptyConsumerState = ConsumerState.builder().build();

        // Newly constructed state is always empty
        assertTrue("Should be empty", emptyConsumerState.isEmpty());

        // Create consumer state with stored offset
        final ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();
        builder.withPartition(new TopicPartition("Topic", 0), 0L);

        final ConsumerState consumerState = builder.build();
        assertFalse("Should NOT be empty", consumerState.isEmpty());

        // Now remove the topic partition
        consumerState.getState().clear();
        assertTrue("Should be empty", consumerState.isEmpty());
    }

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
}