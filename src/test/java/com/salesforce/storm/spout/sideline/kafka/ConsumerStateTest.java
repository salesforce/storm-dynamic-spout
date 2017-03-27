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
}