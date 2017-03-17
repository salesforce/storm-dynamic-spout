package com.salesforce.storm.spout.sideline.kafka;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class PartitionOffsetManagerTest {

    /**
     * This test tracks offsets and acks them in order, then verifies that the last  last finished offset is correct.
     */
    @Test
    public void inOrderTrackAndAck() {
        final long maxOffset = 5;

        // Create our manager we want to test
        final PartitionOffsetManager offsetManager = new PartitionOffsetManager("Test Topic", 1, 0L);

        // Loop thru some offsets
        for (int currentOffset=0; currentOffset<maxOffset; currentOffset++) {
            // Start the current offset
            offsetManager.startOffset(currentOffset);

            // Finish the offset
            offsetManager.finishOffset(currentOffset);

            // Should have remained at the current offset?
            assertEquals("[" + currentOffset + "] Should be the last finished offset", currentOffset, offsetManager.lastFinishedOffset());
        }
    }

    /**
     * This test tracks offsets and acks them in order, then verifies that the last  last finished offset is correct.
     */
    @Test
    public void outOfOrderAck() {
        // Create our manager we want to test
        final PartitionOffsetManager offsetManager = new PartitionOffsetManager("Test Topic", 1, 0L);

        // Finish offset 0,  last finished offset should be 0
        offsetManager.startOffset(0L);
        assertEquals("[A]  last finished offset should be 0", 0L, offsetManager.lastFinishedOffset());

        // Finish offset 1,  last finished offset should be 0
        offsetManager.startOffset(1L);
        assertEquals("[B]  last finished offset should be 0", 0L, offsetManager.lastFinishedOffset());

        // Finish offset 2,  last finished offset should be 0
        offsetManager.startOffset(2L);
        assertEquals("[C]  last finished offset should be 0", 0L, offsetManager.lastFinishedOffset());

        // Start offset 2,  last finished offset should be 0
        offsetManager.finishOffset(2L);
        assertEquals("[D]  last finished offset should be 0", 0L, offsetManager.lastFinishedOffset());

        // Start offset 1,  last finished offset should be 0
        offsetManager.finishOffset(1L);
        assertEquals("[E]  last finished offset should be 0", 0L, offsetManager.lastFinishedOffset());

        // Start offset 0,  last finished offset should be 2
        offsetManager.finishOffset(0L);
        assertEquals("[F]  last finished offset should be 2", 2L, offsetManager.lastFinishedOffset());

        // Finish offset 3,  last finished offset should be 2
        offsetManager.startOffset(3L);
        assertEquals("[G]  last finished offset should be 2", 2L, offsetManager.lastFinishedOffset());

        // Finish offset 4,  last finished offset should be 2
        offsetManager.startOffset(4L);
        assertEquals("[H]  last finished offset should be 2", 2L, offsetManager.lastFinishedOffset());

        // Finish offset 5,  last finished offset should be 2
        offsetManager.startOffset(5L);
        assertEquals("[I]  last finished offset should be 2", 2L, offsetManager.lastFinishedOffset());

        // Start offset 3,  last finished offset should be 3
        offsetManager.finishOffset(3L);
        assertEquals("[J]  last finished offset should be 3", 3L, offsetManager.lastFinishedOffset());

        // Start offset 4,  last finished offset should be 4
        offsetManager.finishOffset(4L);
        assertEquals("[K]  last finished offset should be 4", 4L, offsetManager.lastFinishedOffset());

        // Finish offset 5,  last finished offset should be 4
        offsetManager.startOffset(5L);
        assertEquals("[L]  last finished offset should be 4", 4L, offsetManager.lastFinishedOffset());

        // Start offset 5,  last finished offset should be 5,
        offsetManager.finishOffset(5L);
        assertEquals("[M]  last finished offset should be 4", 5L, offsetManager.lastFinishedOffset());

        // Start offset 4,  last finished offset should be 5
        offsetManager.finishOffset(4L);
        assertEquals("[N]  last finished offset should be 5", 5L, offsetManager.lastFinishedOffset());

        // Start offset 6,  last finished offset should be 5
        offsetManager.startOffset(6L);
        assertEquals("[O]  last finished offset should be 5", 5L, offsetManager.lastFinishedOffset());

        // Start offset 7,  last finished offset should be 5
        offsetManager.startOffset(7L);
        assertEquals("[P]  last finished offset should be 5", 5L, offsetManager.lastFinishedOffset());

        // Start offset 8,  last finished offset should be 5
        offsetManager.startOffset(8L);
        assertEquals("[Q]  last finished offset should be 5", 5L, offsetManager.lastFinishedOffset());

        // Finish offset 6,  last finished offset should be 5
        offsetManager.finishOffset(8L);
        assertEquals("[R]  last finished offset should be 8", 5L, offsetManager.lastFinishedOffset());

        // Finish offset 6,  last finished offset should be 6
        offsetManager.finishOffset(6L);
        assertEquals("[S]  last finished offset should be 6", 6L, offsetManager.lastFinishedOffset());

        // Finish offset 7,  last finished offset should be 8
        offsetManager.finishOffset(7L);
        assertEquals("[S]  last finished offset should be 8", 8L, offsetManager.lastFinishedOffset());

    }
}