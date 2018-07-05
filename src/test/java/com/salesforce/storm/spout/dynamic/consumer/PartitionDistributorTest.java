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

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.Assert.assertArrayEquals;

/**
 * Test that {@link PartitionDistributor} properly distributes partitions across a set of instances.
 */
public class PartitionDistributorTest {

    /**
     * Test that given a number of consumer instances the current instance gets distributed the correct set of partition ids.
     * @param totalConsumers Total number of consumers
     * @param consumerIndex Current consumer instance index
     * @param allPartitionIds All partition ids to be distributed from
     * @param expectedPartitionIds Expected partition ids for the given consumer
     */
    @ParameterizedTest
    @MethodSource("dataProvider")
    public void testCalculatePartitionAssignment(int totalConsumers, int consumerIndex, int[] allPartitionIds, int[] expectedPartitionIds) {
        final int[] actualPartitionIds = PartitionDistributor.calculatePartitionAssignment(
            // Number of consumer instances
            totalConsumers,
            // Current instance index
            consumerIndex,
            // Partition ids to distribute
            allPartitionIds
        );

        assertArrayEquals(
            "Partition ids match",
            expectedPartitionIds,
            actualPartitionIds
        );
    }

    /**
     * Provide test data.
     * @return test data.
     */
    public static Object[][] dataProvider() {
        return new Object[][]{
            // Two instances, first instance, two partitions, first partition
            { 2, 0, new int[]{ 0 , 1 }, new int[]{ 0 } },
            // Two instances, second instance, two partitions, second partition
            { 2, 1, new int[]{ 0 , 1 }, new int[]{ 1 } },
            // Two instances, first instance, two partitions not in order, first partition
            { 2, 0, new int[]{ 1 , 0 }, new int[]{ 0 } },
            // One instance, first instance, three partitions, all partitions
            { 1, 0, new int[]{ 0, 1, 2 }, new int[]{ 0, 1, 2 } },
            // One instance, first instance, three partitions not in order, all partitions
            { 1, 0, new int[]{ 2, 0, 1 }, new int[]{ 0, 1, 2 } },
            // Three instances, first instance, four partitions, first two partitions
            { 3, 0, new int[]{ 0, 1, 2, 3 }, new int[]{ 0, 1 } },
            // Three instances, second instance, four partitions, third partition
            { 3, 1, new int[]{ 0, 1, 2, 3 }, new int[]{ 2 } },
            // Three instances, third instance, four partitions, fourth partition
            { 3, 2, new int[]{ 0, 1, 2, 3 }, new int[]{ 3 } },
        };
    }

    /**
     * Test that when we have more consumer instances than partition ids that an exception is thrown.
     */
    @Test
    public void testCalculatePartitionAssignmentWithMorePartitionsThanInstances() {
        final Throwable thrown = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            PartitionDistributor.calculatePartitionAssignment(
                // Number of consumer instances
                4,
                // Current instance index
                0,
                // Partition ids to distribute
                new int[] { 0, 1, 2 }
            );
        });

        MatcherAssert.assertThat(thrown.getMessage(), Matchers.containsString("partitions"));
    }


    /**
     * Test that when we have an invalid consumerIndex, we toss an exception.
     */
    @Test
    public void testCalculatePartitionAssignmentWithConsumerIndexHigherThanTotalConsumers() {
        final Throwable thrown = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            PartitionDistributor.calculatePartitionAssignment(
                    // Number of consumer instances
                    4,
                    // Current instance index
                    5,
                    // Partition ids to distribute
                    new int[]{0, 1, 2, 3}
            );
        });

        MatcherAssert.assertThat(thrown.getMessage(), Matchers.containsString("consumerIndex"));
    }

    /**
     * Test that when we have a negative consumerIndex value we toss an exception.
     */
    @Test
    public void testCalculatePartitionAssignmentWithConsumerIndexBelowZero() {
        final Throwable thrown = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            PartitionDistributor.calculatePartitionAssignment(
                // Number of consumer instances
                4,
                // Current instance index
                -2,
                // Partition ids to distribute
                new int[] { 0, 1, 2, 3 }
            );
        });

        MatcherAssert.assertThat(thrown.getMessage(), Matchers.containsString("consumerIndex"));
    }

    /**
     * Test that when we have a zero total consumers value that we toss an exception.
     */
    @Test
    public void testCalculatePartitionAssignmentWithTotalConsumersZero() {
        final Throwable thrown = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            PartitionDistributor.calculatePartitionAssignment(
                // Number of consumer instances
                0,
                // Current instance index
                2,
                // Partition ids to distribute
                new int[] { 0, 1, 2, 3 }
            );
        });

        MatcherAssert.assertThat(thrown.getMessage(), Matchers.containsString("totalConsumers"));
    }

    /**
     * Test that when we have a negative total consumers value that we toss an exception.
     */
    @Test
    public void testCalculatePartitionAssignmentWithTotalConsumersNegative() {
        final Throwable thrown = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            PartitionDistributor.calculatePartitionAssignment(
                // Number of consumer instances
                -2,
                // Current instance index
                2,
                // Partition ids to distribute
                new int[] { 0, 1, 2, 3 }
            );
        });

        MatcherAssert.assertThat(thrown.getMessage(), Matchers.containsString("totalConsumers"));
    }
}
