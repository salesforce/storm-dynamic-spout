package com.salesforce.storm.spout.sideline.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.TreeSet;

/**
 * Tracks which offsets for a specific topic/partition have been processed.
 */
public class PartitionOffsetManager {
    private static final Logger logger = LoggerFactory.getLogger(PartitionOffsetManager.class);
    private final String topic;
    private final int partitionId;

    // Offsets where processing has been started
    private final TreeSet<Long> trackedOffsets = new TreeSet<>();

    // Offsets that have been finished, but are pending the advancement of the last finished offset
    private final TreeSet<Long> finishedOffsets = new TreeSet<>();

    private long lastFinishedOffset = 0;
    private long lastStartedOffset = 0;

    public PartitionOffsetManager(final String topic, final int partitionId, final long lastFinishedOffset) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.lastFinishedOffset = lastFinishedOffset;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Start the filter of tracking this offset
     *
     * Not thread safe
     *
     * @param offset
     */
    public void startOffset(long offset) {
        trackedOffsets.add(offset);

        if (offset >= lastStartedOffset) {
            lastStartedOffset = offset;
        } else {
            logger.warn("Starting offsets out of order? {} >= {}", lastStartedOffset, offset);
        }
    }

    /**
     * Finish the filter of tracking this offset
     *
     * Not thread safe.
     *
     * @param offset
     */
    public void finishOffset(final long offset) {
        if (!trackedOffsets.contains(offset)) {
            logger.warn("[{}-{}] - Tried to ack unknown offset {}", getTopic(), getPartitionId(), offset);
            return;
        }

        final Long earliestOffset = trackedOffsets.first();

        // If our set is empty
        if (earliestOffset.equals(offset)) {
            // No longer track this offset
            trackedOffsets.remove(offset);

            // If we have no finished offsets, our last is the one we're handling right now
            if (finishedOffsets.isEmpty()) {
                lastFinishedOffset = offset;
            } else {
                // We need to walk through our finished offsets and look for skips
                int increment = 1;

                for (final long finishedOffset : (TreeSet<Long>) finishedOffsets.clone()) {
                    // The next offset has finished, so we filter it and move onto the next
                    if (offset == finishedOffset - increment) {
                        lastFinishedOffset = finishedOffset;
                        finishedOffsets.remove(finishedOffset);

                        increment++;
                    } else {
                        // When there is a skip, we just stick with the offset we're currently handling since it's the most recent
                        lastFinishedOffset = offset;
                        trackedOffsets.remove(offset);
                        break;
                    }
                }
            }
        } else {
            // Since it is finished we no longer need to track it
            trackedOffsets.remove(offset);
            // This is not the earliest offset, so we'll track it as a finished one to deal with later
            finishedOffsets.add(offset);
        }
    }

    /**
     * Not thread safe.
     * @return - return the last offset considered "finished".
     * Here a "finished" offset is the highest continuous offset.
     */
    public long lastFinishedOffset() {
        return lastFinishedOffset;
    }

    /**
     * @return - return the largest offset we have started tracking.
     * This is NOT the same as the "Last Finished Offset"
     */
    public long lastStartedOffset() {
        // If the last finished offset happens to be higher, which is the case
        // if we haven't started tracking anything yet.
        if ((lastFinishedOffset + 1) > lastStartedOffset) {
            // return last finished offset + 1,
            return (lastFinishedOffset + 1);
        }
        return lastStartedOffset;
    }
}
