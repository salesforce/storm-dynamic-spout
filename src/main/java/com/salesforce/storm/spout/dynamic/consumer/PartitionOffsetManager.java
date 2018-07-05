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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.TreeSet;

/**
 * Tracks which offsets for a specific namespace/partition have been started or processed.
 *
 * An offset is considered "started" when it has been emitted into the topology, but not yet confirmed
 * as being fully completed.
 *
 * An offset is considered "finished" when it has been acked by the topology confirming it has been fully processed.
 *
 * The "last started offset" is defined as being the LOWEST "started" and NOT "finished" offset.
 *
 * The "last finished offset" is defined as being the HIGHEST continuous offset that has been "finished".
 *
 * Example:
 *   We have started offsets:  [0,1,2,3,4,5]
 *   We have finished offsets: [0,1,3,5]
 *
 *   The "last started offset" would be 5
 *   The "last finished offset" would be 3, since 0-3 have been finished, but 4 has not.
 */
public class PartitionOffsetManager {
    private static final Logger logger = LoggerFactory.getLogger(PartitionOffsetManager.class);
    private final String namespace;
    private final int partitionId;

    // Offsets where processing has been started
    private final TreeSet<Long> trackedOffsets = new TreeSet<>();

    // Offsets that have been finished, but are pending the advancement of the last finished offset
    private final TreeSet<Long> finishedOffsets = new TreeSet<>();

    private long lastFinishedOffset = 0;
    private long lastStartedOffset = -1;

    /**
     * Constructor.
     * @param namespace - What namespace this Partition belongs to.
     * @param partitionId - What partition this instance represents.
     * @param lastFinishedOffset - What offset should be considered the last "completed" offset.
     */
    public PartitionOffsetManager(final String namespace, final int partitionId, final long lastFinishedOffset) {
        this.namespace = namespace;
        this.partitionId = partitionId;
        this.lastFinishedOffset = lastFinishedOffset;
    }

    /**
     * Get the namespace associated with this instance.
     * @return namespace associated with this instance
     */
    private String getNamespace() {
        return namespace;
    }

    /**
     * Get the partition this instance represents.
     * @return partition this instance represents
     */
    private int getPartitionId() {
        return partitionId;
    }

    /**
     * Mark this offset as being emitted into the topology, but not yet confirmed/completed.
     *
     * Not thread safe.
     *
     * @param offset offset to start tracking
     */
    public void startOffset(final long offset) {
        trackedOffsets.add(offset);

        if (offset >= lastStartedOffset) {
            lastStartedOffset = offset;
        } else {
            logger.warn("Starting offsets out of order? {} >= {}", lastStartedOffset, offset);
        }
    }

    /**
     * Mark this offset as having completed processing.
     *
     * Not thread safe.
     *
     * @param offset offset to mark as completed
     */
    public void finishOffset(final long offset) {
        if (!trackedOffsets.contains(offset)) {
            logger.warn("[{}-{}] - Tried to ack unknown offset {}", getNamespace(), getPartitionId(), offset);
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

                Iterator<Long> iterator = finishedOffsets.iterator();
                while (iterator.hasNext()) {
                    final long finishedOffset = iterator.next();
                    // The next offset has finished, so we filter it and move onto the next
                    if (offset == finishedOffset - increment) {
                        lastFinishedOffset = finishedOffset;

                        // Remove this entry from the tree
                        iterator.remove();

                        // Increment and continue;
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
     * Get the last finished offset.
     *
     * Here a "finished" offset is the highest continuous offset.
     *
     * @return last offset considered "finished"
     */
    public long lastFinishedOffset() {
        return lastFinishedOffset;
    }

    /**
     * Get the largest offset whose tracking has been started.
     *
     * This is NOT the same as the "Last Finished Offset"
     *
     * @return largest offset whose tracking has been started
     */
    public long lastStartedOffset() {
        // If the last started offset is -1 that means we haven't started tracking any offsets yet.
        if (lastStartedOffset == -1) {
            // So we'll return the last finished offset...
            return lastFinishedOffset;
        }
        return lastStartedOffset;
    }
}
