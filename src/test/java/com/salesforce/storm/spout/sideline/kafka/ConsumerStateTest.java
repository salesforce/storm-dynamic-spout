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
        // Newly constructed state is always empty
        assertTrue("Should be empty", new ConsumerState().isEmpty());

        // Create consumer state with stored offset
        ConsumerState consumerState = new ConsumerState();
        consumerState.setOffset(new TopicPartition("Topic", 0), 0L);
        assertFalse("Should NOT be empty", consumerState.isEmpty());

        // Now remove the topic partition
        consumerState.getState().clear();
        assertTrue("Should be empty", consumerState.isEmpty());
    }
}