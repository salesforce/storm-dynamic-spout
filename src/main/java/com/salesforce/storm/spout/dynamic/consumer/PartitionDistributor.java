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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Utility for calculating the distribution of a set of partition ids to a given consumer.
 */
public class PartitionDistributor {

    /**
     * Get partition ids for assignment based upon the number of consumers give one of those consumers.
     *
     * @param totalConsumers number of consumer total across all instances
     * @param consumerIndex current consumers index.
     * @param allPartitionIds number of partitions to distribute across.
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

        // Determine the maximum number of partitions that a given consumer instance could have
        final int maxPartitionsPerInstance = (int) Math.ceil((double) allPartitionIds.length / totalConsumers);

        // We are going to sort our partitions across all consumer indexes
        final Map<Integer,List<Integer>> partitionByConsumer = Maps.newHashMap();

        // Add a list for each consumer
        for (int i = 0; i < totalConsumers; i++) {
            partitionByConsumer.put(i, Lists.newArrayList());
        }

        // Tracks the current consumer instance we are handing partitions to
        int consumerInstance = 0;
        // Number of remaining partitions, we'll start with all of them
        int remainingPartitions = allPartitionIds.length;

        for (final int partitionId : allPartitionIds) {
            // Add our current partition to the current consumer instance
            partitionByConsumer.get(consumerInstance).add(partitionId);

            // We now have one less partition to dole out
            remainingPartitions--;

            // If we've reached our maximum number of partitions per instance, advance to the next instance
            // This means that an instance gets partitions in order, eg. [0, 1] and [2, 3], etc.
            if (partitionByConsumer.get(consumerInstance).size() >= maxPartitionsPerInstance) {
                consumerInstance++;
            }

            // How many consumer instances remain?
            final int remainingConsumerSlots = totalConsumers - consumerInstance;

            // We we have more slots than remaining partitions we advance to the next consumer.
            // This usually means that we have a weird number of partitions to balance across instances, like four partitions
            // across three instances, which means that last two instances will have only one partition, though the maximum
            // could be two instances. If this part confuses you, check out the test for this class.
            if (remainingConsumerSlots > remainingPartitions) {
                consumerInstance++;
            }
        }

        // Grab the partitions for our current consumer and make an array of primitives of them
        return partitionByConsumer.get(consumerIndex).stream().mapToInt(i -> i).toArray();
    }
}
