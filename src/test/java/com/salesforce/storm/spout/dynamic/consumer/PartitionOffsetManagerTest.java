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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test that {@link PartitionOffsetManager} accurately tracks messages by partition.
 */
public class PartitionOffsetManagerTest {

    private static final Logger logger = LoggerFactory.getLogger(PartitionOffsetManagerTest.class);

    /**
     * This test tracks offsets and will ack them in order, then verifies that the last finished offset is correct.
     */
    @Test
    public void inOrderTrackAndAck() {
        final long maxOffset = 5;

        // Create our manager we want to test
        final PartitionOffsetManager offsetManager = new PartitionOffsetManager("Test Topic", 1, 0L);

        // Loop through some offsets
        for (int currentOffset = 0; currentOffset < maxOffset; currentOffset++) {
            // Start the current offset
            offsetManager.startOffset(currentOffset);

            // Finish the offset
            offsetManager.finishOffset(currentOffset);

            // Should have remained at the current offset?
            assertEquals(currentOffset, offsetManager.lastFinishedOffset(), "[" + currentOffset + "] Should be the last finished offset");
        }
    }

    /**
     * This test tracks offsets and will ack them in order, then verifies that the last finished offset is correct.
     */
    @Test
    public void outOfOrderAck() {
        // Create our manager we want to test
        final PartitionOffsetManager offsetManager = new PartitionOffsetManager("Test Topic", 1, 0L);

        // Finish offset 0,  last finished offset should be 0
        offsetManager.startOffset(0L);
        assertEquals(0L, offsetManager.lastFinishedOffset(), "[A]  last finished offset should be 0");

        // Finish offset 1,  last finished offset should be 0
        offsetManager.startOffset(1L);
        assertEquals(0L, offsetManager.lastFinishedOffset(), "[B]  last finished offset should be 0");

        // Finish offset 2,  last finished offset should be 0
        offsetManager.startOffset(2L);
        assertEquals(0L, offsetManager.lastFinishedOffset(), "[C]  last finished offset should be 0");

        // Start offset 2,  last finished offset should be 0
        offsetManager.finishOffset(2L);
        assertEquals(0L, offsetManager.lastFinishedOffset(), "[D]  last finished offset should be 0");

        // Start offset 1,  last finished offset should be 0
        offsetManager.finishOffset(1L);
        assertEquals(0L, offsetManager.lastFinishedOffset(), "[E]  last finished offset should be 0");

        // Start offset 0,  last finished offset should be 2
        offsetManager.finishOffset(0L);
        assertEquals(2L, offsetManager.lastFinishedOffset(), "[F]  last finished offset should be 2");

        // Finish offset 3,  last finished offset should be 2
        offsetManager.startOffset(3L);
        assertEquals(2L, offsetManager.lastFinishedOffset(), "[G]  last finished offset should be 2");

        // Finish offset 4,  last finished offset should be 2
        offsetManager.startOffset(4L);
        assertEquals(2L, offsetManager.lastFinishedOffset(), "[H]  last finished offset should be 2");

        // Finish offset 5,  last finished offset should be 2
        offsetManager.startOffset(5L);
        assertEquals(2L, offsetManager.lastFinishedOffset(), "[I]  last finished offset should be 2");

        // Start offset 3,  last finished offset should be 3
        offsetManager.finishOffset(3L);
        assertEquals(3L, offsetManager.lastFinishedOffset(), "[J]  last finished offset should be 3");

        // Start offset 4,  last finished offset should be 4
        offsetManager.finishOffset(4L);
        assertEquals(4L, offsetManager.lastFinishedOffset(), "[K]  last finished offset should be 4");

        // Finish offset 5,  last finished offset should be 4
        offsetManager.startOffset(5L);
        assertEquals(4L, offsetManager.lastFinishedOffset(), "[L]  last finished offset should be 4");

        // Start offset 5,  last finished offset should be 5,
        offsetManager.finishOffset(5L);
        assertEquals(5L, offsetManager.lastFinishedOffset(), "[M]  last finished offset should be 4");

        // Start offset 4,  last finished offset should be 5
        offsetManager.finishOffset(4L);
        assertEquals(5L, offsetManager.lastFinishedOffset(), "[N]  last finished offset should be 5");

        // Start offset 6,  last finished offset should be 5
        offsetManager.startOffset(6L);
        assertEquals(5L, offsetManager.lastFinishedOffset(), "[O]  last finished offset should be 5");

        // Start offset 7,  last finished offset should be 5
        offsetManager.startOffset(7L);
        assertEquals(5L, offsetManager.lastFinishedOffset(), "[P]  last finished offset should be 5");

        // Start offset 8,  last finished offset should be 5
        offsetManager.startOffset(8L);
        assertEquals(5L, offsetManager.lastFinishedOffset(), "[Q]  last finished offset should be 5");

        // Finish offset 6,  last finished offset should be 5
        offsetManager.finishOffset(8L);
        assertEquals(5L, offsetManager.lastFinishedOffset(), "[R]  last finished offset should be 8");

        // Finish offset 6,  last finished offset should be 6
        offsetManager.finishOffset(6L);
        assertEquals(6L, offsetManager.lastFinishedOffset(), "[S]  last finished offset should be 6");

        // Finish offset 7,  last finished offset should be 8
        offsetManager.finishOffset(7L);
        assertEquals(8L, offsetManager.lastFinishedOffset(), "[S]  last finished offset should be 8");
    }

    /**
     * This test verifies what happens if you call lastTrackedOffset() when we have nothing being tracked.
     * It should return the last finished offset + 1.
     */
    @Test
    public void testLastStartedOffsetWhenHasNone() {
        // Create our manager we want to test with starting offset set to 0
        long startingOffset = 0L;
        PartitionOffsetManager offsetManager = new PartitionOffsetManager("Test Topic", 1, startingOffset);
        assertEquals(startingOffset, offsetManager.lastStartedOffset(), "Should be startingOffset");

        // Create our manager we want to test with starting offset set to 100
        startingOffset = 100L;
        offsetManager = new PartitionOffsetManager("Test Topic", 1, startingOffset);
        assertEquals(startingOffset, offsetManager.lastStartedOffset(), "Should be startingOffset + 1");
    }

    /**
     * This test verifies what happens if you call lastTrackedOffset() when we have been tracking some offsets.
     * It should return the largest value tracked.
     */
    @Test
    public void testLastStartedOffset() {
        // Create our manager we want to test with starting offset set to 0
        long startingOffset = 0L;
        PartitionOffsetManager offsetManager = new PartitionOffsetManager("Test Topic", 1, startingOffset);

        // Start some offsets
        offsetManager.startOffset(1L);
        offsetManager.startOffset(2L);
        offsetManager.startOffset(3L);
        offsetManager.startOffset(4L);

        // Validate its 4L
        assertEquals(4L, offsetManager.lastStartedOffset(), "Should be 4L");

        // Now finish some offsets
        offsetManager.finishOffset(1L);
        long result = offsetManager.lastStartedOffset();
        assertEquals(4L, result, "Should be 4L");

        offsetManager.finishOffset(3L);
        result = offsetManager.lastStartedOffset();
        assertEquals(4L, result, "Should be 4L");

        offsetManager.finishOffset(4L);
        result = offsetManager.lastStartedOffset();
        assertEquals(4L, result, "Should be 4L");

        offsetManager.finishOffset(2L);
        result = offsetManager.lastStartedOffset();
        assertEquals(4L, result, "Should be 4L => 4L");
    }

    /**
     * Disabled Test.
     * Rudimentary benchmark test against PartitionOffsetManager.
     *
     * @param totalNumbers - total number of offsets to add to the manager.
     */
    @MethodSource("provideSizes")
    public void doPerformanceBenchmark(final int totalNumbers) throws InterruptedException {
        final int spread = 100;

        // Generate out of order numbers
        Random random = new Random();
        int[] randomNumbers = new int[totalNumbers];
        for (int x = 0; x < totalNumbers; x++) {
            int nextNumber = random.nextInt(spread);
            randomNumbers[x] = x + nextNumber;
        }

        // Now create our manager
        final PartitionOffsetManager offsetManager = new PartitionOffsetManager("Test Topic", 1, 0L);

        // Now create a sorted array
        int[] sortedNumbers = Arrays.copyOf(randomNumbers, randomNumbers.length);
        Arrays.sort(sortedNumbers);

        // Start tracking from ordered ist
        long start = System.currentTimeMillis();
        for (int x = 0; x < totalNumbers; x++) {
            offsetManager.startOffset(sortedNumbers[x]);
        }
        logger.info("Finished starting {} in {} ms ", totalNumbers, (System.currentTimeMillis() - start));

        // Now start acking
        start = System.currentTimeMillis();
        for (int x = 0; x < totalNumbers; x++) {
            offsetManager.finishOffset(randomNumbers[x]);
        }
        logger.info("Finished acking {} in {} ms ", totalNumbers, (System.currentTimeMillis() - start));
    }

    /**
     * Provides various tuple buffer implementation.
     */
    public static Object[][] provideSizes() throws InstantiationException, IllegalAccessException {
        return new Object[][]{
                { 10 },
                { 100 },
                { 1_000 },
                { 10_000 },
                { 20_000 },
                { 40_000 },
                { 80_000 },
                { 160_000 },
                { 320_000 },
                { 640_000 },
                { 1_280_000 },
        };
    }
}
