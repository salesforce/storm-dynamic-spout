package com.salesforce.storm.spout.sideline.kafka;

import org.apache.storm.shade.org.apache.commons.lang.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utility for calculating the distribution of a set of partition ids to a given consumer
 */
public class PartitionDistributor {

    /**
     * Get partition ids for assignment based upon the number of consumers give one of those consumers
     * @return List of partition ids for assignment
     */
    public static int[] calculatePartitionAssignment(final int totalConsumers, final int consumerIndex, final int[] allPartitionIds) {
        // Sort our partitions
        Arrays.sort(allPartitionIds);

        // We have more instances than partitions, we don't want that!
        if (totalConsumers > allPartitionIds.length) {
            throw new RuntimeException("You have more instances than partitions, trying toning it back a bit!");
        }

        // Determine the maximum number of partitions that a given consumer instance should have
        final int partitionsPerInstance = (int) Math.ceil((double) allPartitionIds.length / totalConsumers);

        // Determine our starting point in the list of instances
        final int startingPartition = consumerIndex == 0 ? 0 : partitionsPerInstance * consumerIndex;

        // Determine our ending point in the list of instances
        final int endingPartition = startingPartition + partitionsPerInstance > allPartitionIds.length ?
            allPartitionIds.length : startingPartition + partitionsPerInstance;

        // Make a new array of integers for the partition ids
        List<Integer> partitionIds = new ArrayList<>();

        // Loop over our segment of all the partitions and add just the ones we need to our array
        for (int i = startingPartition; i < endingPartition; i++) {
            partitionIds.add(allPartitionIds[i]);
        }

        // Convert to an array of primitive ints and return them
        return ArrayUtils.toPrimitive(partitionIds.toArray(new Integer[0]));
    }
}
