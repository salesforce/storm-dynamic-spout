package com.salesforce.storm.spout.sideline.kafka;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertArrayEquals;

@RunWith(DataProviderRunner.class)
public class PartitionDistributorTest {

    /**
     * By default, no exceptions should be thrown.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Test that given a number of consumer instances the current instance gets distributed the correct set of partition ids
     * @param totalConsumers Total number of consumers
     * @param consumerIndex Current consumer instance index
     * @param allPartitionIds All partition ids to be distributed from
     * @param expectedPartitionIds Expected partition ids for the given consumer
     */
    @Test
    @UseDataProvider("dataProvider")
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

    @DataProvider
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
        };
    }

    /**
     * Test that when we have more consumer instances than partition ids that an exception is thrown.
     */
    @Test
    public void testCalculatePartitionAssignmentWithMorePartitionsThanInstances() {

        // We expect exceptions on this one.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("partitions");

        PartitionDistributor.calculatePartitionAssignment(
            // Number of consumer instances
            4,
            // Current instance index
            0,
            // Partition ids to distribute
            new int[] { 0, 1, 2 }
        );
    }

    /**
     * Test that when we have more consumer instances than partition ids that an exception is thrown.
     */
    @Test
    public void testCalculatePartitionAssignmentWithConsumerIndexHigherThanTotalConsumers() {

        // We expect exceptions on this one.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("consumerIndex");

        PartitionDistributor.calculatePartitionAssignment(
                // Number of consumer instances
                4,
                // Current instance index
                5,
                // Partition ids to distribute
                new int[] { 0, 1, 2, 3 }
        );
    }

    /**
     * Test that when we have more consumer instances than partition ids that an exception is thrown.
     */
    @Test
    public void testCalculatePartitionAssignmentWithConsumerIndexBelowZero() {

        // We expect exceptions on this one.
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("consumerIndex");

        PartitionDistributor.calculatePartitionAssignment(
                // Number of consumer instances
                4,
                // Current instance index
                -2,
                // Partition ids to distribute
                new int[] { 0, 1, 2, 3 }
        );
    }
}
