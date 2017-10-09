/**
 * Copyright (c) 2017, Salesforce.com, Inc.
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

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

/**
 * Utility for calculating the distribution of a set of partition ids to a given consumer.
 */
public class PartitionDistributor {

    /**
     * Get partition ids for assignment based upon the number of consumers give one of those consumers.
     * @return List of partition ids for assignment
     */
    public static int[] calculatePartitionAssignment(final int totalConsumers, final int consumerIndex, final int[] allPartitionIds) {
        // If total consumers is 0, that's not possible!
        if (totalConsumers <= 0) {
            throw new IllegalArgumentException("You cannot have less than 1 totalConsumers!");
        }

        // We have more instances than partitions, we don't want that!
        if (totalConsumers > allPartitionIds.length) {
            throw new IllegalArgumentException("You have more instances than partitions, trying toning it back a bit!");
        }
        // We have a consumer index that's invalid
        if (consumerIndex >= totalConsumers || consumerIndex < 0) {
            throw new IllegalArgumentException("Your consumerIndex is invalid! Range should be [0 -> " + (totalConsumers - 1) + "]");
        }

        // Sort our partitions
        Arrays.sort(allPartitionIds);

        // Determine the maximum number of partitions that a given consumer instance should have
        final int partitionsPerInstance = (int) Math.ceil((double) allPartitionIds.length / totalConsumers);

        // Determine our starting point in the list of instances
        final int startingPartition = consumerIndex == 0 ? 0 : partitionsPerInstance * consumerIndex;

        // Determine our ending point in the list of instances
        final int endingPartition = startingPartition + partitionsPerInstance > allPartitionIds.length
                ? allPartitionIds.length : startingPartition + partitionsPerInstance;

        // Make a new array of integers for the partition ids
        List<Integer> partitionIds = Lists.newArrayList();

        // Loop over our segment of all the partitions and add just the ones we need to our array
        for (int i = startingPartition; i < endingPartition; i++) {
            partitionIds.add(allPartitionIds[i]);
        }

        // Convert to an array of primitive ints and return them
        return partitionIds.stream().mapToInt(i -> i).toArray();
    }
}
