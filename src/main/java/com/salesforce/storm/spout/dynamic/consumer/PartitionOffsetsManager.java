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

import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Utility class to help with managing offsets across multiple ConsumerPartitions from a single Consumer instance.
 *
 * Since offsets are managed on a per partition basis, each ConsumerPartition has its own PartitionStateManager
 * instance to track its own offset.
 */
public class PartitionOffsetsManager {

    private static final Logger logger = LoggerFactory.getLogger(PartitionOffsetsManager.class);

    /**
     * Since offsets are managed on a per partition basis, each namespace/partition has its own ConsumerPartitionStateManagers
     * instance to track its own offset.  The state of these are what gets persisted via the ConsumerStateManager.
     */
    private final Map<ConsumerPartition, PartitionOffsetManager> partitionStateManagers = new HashMap<>();

    /**
     * Replaces/Adds tracking offsets for a new ConsumerPartition.
     * @param consumerPartition ConsumerPartition to begin tracking offsets for.
     * @param offset The last known COMPLETED offset.
     */
    public void replaceEntry(final ConsumerPartition consumerPartition, final long offset) {
        // Add new entry
        partitionStateManagers.put(
            consumerPartition,
            new PartitionOffsetManager(consumerPartition.namespace(), consumerPartition.partition(), offset)
        );
    }

    /**
     * For the given ConsumerPartition, begin tracking a new offset.
     * @param consumerPartition The ConsumerPartition to begin tracking a new offset for.
     * @param offset The offset to begin tracking.
     */
    public void startOffset(final ConsumerPartition consumerPartition, final long offset) {
        if (!partitionStateManagers.containsKey(consumerPartition)) {
            logger.info("Attempted to start an offset without a consumer partition in the state mananger {} {}", consumerPartition, offset);
            replaceEntry(consumerPartition, offset);
        } else {
            partitionStateManagers.get(consumerPartition).startOffset(offset);
        }
    }

    /**
     * Give the given ConsumerPartition, mark the offset as having completed processing.
     * @param consumerPartition The ConsumerPartition to begin tracking a new offset for
     * @param offset The offset to mark as completed
     */
    public void finishOffset(final ConsumerPartition consumerPartition, final long offset) {
        partitionStateManagers.get(consumerPartition).finishOffset(offset);
    }

    /**
     * Get the last finished offset for the given ConsumerPartition.
     * @param consumerPartition The ConsumerPartition to retrieve the last finished offset for
     * @return last finished offset for the given ConsumerPartition
     */
    public long getLastFinishedOffset(final ConsumerPartition consumerPartition) {
        if (!partitionStateManagers.containsKey(consumerPartition)) {
            throw new IllegalStateException("Invalid ConsumerPartition [" + consumerPartition + "]");
        }
        return partitionStateManagers.get(consumerPartition).lastFinishedOffset();
    }

    /**
     * Get a map of all ConsumerPartitions and their last finished offsets.
     * @return map of all ConsumerPartitions and their last finished offsets
     */
    public Map<ConsumerPartition, Long> getLastFinishedOffsets() {
        Map<ConsumerPartition, Long> entries = new HashMap<>();

        for (Map.Entry<ConsumerPartition, PartitionOffsetManager> entry : partitionStateManagers.entrySet()) {
            final ConsumerPartition consumerPartition = entry.getKey();
            final long lastFinishedOffset = entry.getValue().lastFinishedOffset();

            // Add to map
            entries.put(consumerPartition, lastFinishedOffset);
        }
        // return immutable map
        return Collections.unmodifiableMap(entries);
    }

    /**
     * Get the largest offset we have started tracking for the given ConsumerPartition.
     *
     * This is NOT the same as the "Last Finished Offset".
     *
     * @param consumerPartition ConsumerPartition to retrieve the last started offset for
     * @return largest offset we have started tracking for the given ConsumerPartition
     */
    public long getLastStartedOffset(final ConsumerPartition consumerPartition) {
        if (!partitionStateManagers.containsKey(consumerPartition)) {
            throw new IllegalStateException("Invalid ConsumerPartition [" + consumerPartition + "]");
        }
        return partitionStateManagers.get(consumerPartition).lastStartedOffset();
    }

    /**
     * Get a map of all ConsumerPartitions to the largest offset we have started tracking on it.
     *
     * This is NOT the same as the "Last Finished Offset".
     *
     * @return map of all ConsumerPartitions to the largest offset we have started tracking on it
     *
     */
    public Map<ConsumerPartition, Long> getLastStartedOffsets() {
        Map<ConsumerPartition, Long> entries = new HashMap<>();

        for (Map.Entry<ConsumerPartition, PartitionOffsetManager> entry : partitionStateManagers.entrySet()) {
            final ConsumerPartition consumerPartition = entry.getKey();
            final long lastFinishedOffset = entry.getValue().lastStartedOffset();

            // Add to map
            entries.put(consumerPartition, lastFinishedOffset);
        }
        // return immutable map
        return Collections.unmodifiableMap(entries);
    }

    /**
     * Returns what the consumer considers its current "finished" state to be.  This means the highest
     * offsets for all partitions its consuming that it has tracked as having been complete.
     *
     * @return consumer's current state
     */
    public ConsumerState getCurrentState() {
        final ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();

        for (Map.Entry<ConsumerPartition, PartitionOffsetManager> entry : partitionStateManagers.entrySet()) {
            builder.withPartition(entry.getKey(), entry.getValue().lastFinishedOffset());
        }
        return builder.build();
    }

    /**
     * Get all ConsumerPartitions managed by this instance.
     * @return all ConsumerPartitions managed by this instance
     */
    public Set<ConsumerPartition> getAllManagedConsumerPartitions() {
        return partitionStateManagers.keySet();
    }

    @Override
    public String toString() {
        return "PartitionOffsetsManager{" + partitionStateManagers.toString() + '}';
    }
}
