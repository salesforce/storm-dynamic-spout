package com.salesforce.storm.spout.sideline.consumer;

import com.salesforce.storm.spout.sideline.ConsumerPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Utility class to help with managing offsets across multiple ConsumerPartitions from a single Consumer instance.
 */
public class OffsetsManager {
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
            new PartitionOffsetManager(
                consumerPartition.namespace(),
                consumerPartition.partition(),
                offset
            )
        );
    }

    /**
     * For the given ConsumerPartition, begin tracking a new offset.
     * @param consumerPartition The ConsumerPartition to begin tracking a new offset for.
     * @param offset The offset to begin tracking.
     */
    public void startOffset(final ConsumerPartition consumerPartition, final long offset) {
        partitionStateManagers.get(consumerPartition).startOffset(offset);
    }

    /**
     * Give the given ConsumerPartition, mark the offset as having completed processing.
     * @param consumerPartition The ConsumerPartition to begin tracking a new offset for.
     * @param offset The offset to mark as completed.
     */
    public void finishOffset(final ConsumerPartition consumerPartition, final long offset) {
        partitionStateManagers.get(consumerPartition).finishOffset(offset);
    }

    /**
     * @param consumerPartition The ConsumerPartition to retrieve the last finished offset for.
     * @return Last finished offset for the given ConsumerPartition.
     */
    public long getLastFinishedOffset(final ConsumerPartition consumerPartition) {
        if (!partitionStateManagers.containsKey(consumerPartition)) {
            throw new IllegalStateException("Invalid ConsumerPartition [" + consumerPartition + "]");
        }
        return partitionStateManagers.get(consumerPartition).lastFinishedOffset();
    }

    /**
     * @return Map of all ConsumerPartitions and their last finished offsets.
     */
    public Map<ConsumerPartition, Long> getLastFinishedOffsets() {
        Map<ConsumerPartition, Long> entries = new HashMap<>();

        for (Map.Entry<ConsumerPartition, PartitionOffsetManager> entry: partitionStateManagers.entrySet()) {
            final ConsumerPartition consumerPartition = entry.getKey();
            final long lastFinishedOffset = entry.getValue().lastFinishedOffset();

            // Add to map
            entries.put(consumerPartition, lastFinishedOffset);
        }
        // return immutable map
        return Collections.unmodifiableMap(entries);
    }

    /**
     * @param consumerPartition The ConsumerPartition to retrieve the last started offset for.
     * @return return the largest offset we have started tracking for the given ConsumerPartition.
     *           This is NOT the same as the "Last Finished Offset"
     */
    public long getLastStartedOffset(final ConsumerPartition consumerPartition) {
        if (!partitionStateManagers.containsKey(consumerPartition)) {
            throw new IllegalStateException("Invalid ConsumerPartition [" + consumerPartition + "]");
        }
        return partitionStateManagers.get(consumerPartition).lastStartedOffset();
    }

    /**
     * @return Map of all ConsumerPartitions to the largest offset we have started tracking on it.
     *           This is NOT the same as the "Last Finished Offset"
     */
    public Map<ConsumerPartition, Long> getLastStartedOffsets() {
        Map<ConsumerPartition, Long> entries = new HashMap<>();

        for (Map.Entry<ConsumerPartition, PartitionOffsetManager> entry: partitionStateManagers.entrySet()) {
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
     * @return The Consumer's current state.
     */
    public ConsumerState getCurrentState() {
        final ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();

        for (Map.Entry<ConsumerPartition, PartitionOffsetManager> entry : partitionStateManagers.entrySet()) {
            builder.withPartition(entry.getKey(), entry.getValue().lastFinishedOffset());
        }
        return builder.build();
    }

    /**
     * @return All ConsumerPartitions managed by this instance.
     */
    public Set<ConsumerPartition> getAllManagedConsumerPartitions() {
        return partitionStateManagers.keySet();
    }

    @Override
    public String toString() {
        return "OffsetsManager{" + partitionStateManagers.toString() + '}';
    }
}
