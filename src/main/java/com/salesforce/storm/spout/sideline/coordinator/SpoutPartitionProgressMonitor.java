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
package com.salesforce.storm.spout.sideline.coordinator;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.ConsumerPartition;
import com.salesforce.storm.spout.sideline.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.consumer.ConsumerState;
import com.salesforce.storm.spout.sideline.DelegateSpout;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.persistence.SidelinePayload;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * Attempts to monitor VirtualSpout progress.
 */
public class SpoutPartitionProgressMonitor {
    private static final Logger logger = LoggerFactory.getLogger(SpoutPartitionProgressMonitor.class);

    final PersistenceAdapter persistenceAdapter;
    final Map<ConsumerPartition, PartitionProgress> mainProgressMap = Maps.newHashMap();

    public SpoutPartitionProgressMonitor(PersistenceAdapter persistenceAdapter) {
        this.persistenceAdapter = persistenceAdapter;
    }

    public void open(Map spoutConfig) {
        persistenceAdapter.open(spoutConfig);
    }

    public void close() {
        persistenceAdapter.close();
    }

    public Map<ConsumerPartition, PartitionProgress> getStatus(final DelegateSpout spout) {
        final SidelineRequestIdentifier sidelineIdentifier = getSidelineRequestIdentifier(spout);

        if (sidelineIdentifier == null) {
            // This is the main
            return handleMainVirtualSpout(spout);
        } else {
            // This is a sideline request virtual spout
            return handleSidelineVirtualSpout(spout);
        }
    }

    private SidelineRequestIdentifier getSidelineRequestIdentifier(final DelegateSpout spout) {
        final VirtualSpoutIdentifier virtualSpoutId = spout.getVirtualSpoutId();

        // TODO: Revisit this after more work has been done to the vspoutid object
        // Parse out the SidelineRequestId, this is not ideal.
        final String[] bits = virtualSpoutId.toString().split(":");
        if (bits.length != 2) {
            logger.error("Unable to parse virtualSpoutId: {}", virtualSpoutId);
            return null;
        }

        // Grab 2nd bit.
        final String sidelineRequestIdString = bits[1];

        if (sidelineRequestIdString.equals("main")) {
            return null;
        } else {
            return new SidelineRequestIdentifier(sidelineRequestIdString);
        }

    }

    private Map<ConsumerPartition, PartitionProgress> handleMainVirtualSpout(final DelegateSpout spout) {
        final ConsumerState currentState = spout.getCurrentState();

        // Max lag is the MAX lag across all partitions
        // This is not ideal...since its not tied to any specific partition.
        // At best you'll get the MAX lag for all of the partitions your instance is consuming from :/
        final Double maxLag = spout.getMaxLag();

        for (final ConsumerPartition consumerPartition : currentState.getConsumerPartitions()) {
            final PartitionProgress previousProgress = mainProgressMap.get(consumerPartition);
            final long currentOffset = currentState.getOffsetForNamespaceAndPartition(consumerPartition);

            // "Calculate" ending offset by adding currentOffset + maxLag
            final long endingOffset = currentOffset + maxLag.longValue();

            if (previousProgress == null) {
                mainProgressMap.put(consumerPartition, new PartitionProgress(currentOffset, currentOffset, endingOffset));
                continue;
            }

            // Build new progress
            mainProgressMap.put(consumerPartition, new PartitionProgress(previousProgress.getStartingOffset(), currentOffset, endingOffset));
        }

        return Collections.unmodifiableMap(mainProgressMap);
    }

    private Map<ConsumerPartition, PartitionProgress> handleSidelineVirtualSpout(final DelegateSpout spout) {
        // Create return map
        Map<ConsumerPartition, PartitionProgress> progressMap = Maps.newHashMap();

        final VirtualSpoutIdentifier virtualSpoutId = spout.getVirtualSpoutId();
        final SidelineRequestIdentifier sidelineRequestIdentifier = getSidelineRequestIdentifier(spout);
        final ConsumerState currentState = spout.getCurrentState();

        for (final ConsumerPartition consumerPartition : currentState.getConsumerPartitions()) {
            // Retrieve status
            final SidelinePayload payload = getPersistenceAdapter().retrieveSidelineRequest(
                sidelineRequestIdentifier,
                consumerPartition.partition()
            );

            if (payload == null) {
                // Nothing to do?
                logger.error("Could not find SidelineRequest for Id {}", sidelineRequestIdentifier);
                return null;
            }

            // Get the state
            Long currentOffset = getPersistenceAdapter().retrieveConsumerState(virtualSpoutId.toString(), consumerPartition.partition());
            if (currentOffset == null) {
                logger.info("Could not find Current State for Id {}, assuming consumer has no previous state", virtualSpoutId);
                continue;
            }

            // Make sure no nulls
            boolean hasError = false;
            if (payload.startingOffset == null) {
                logger.warn("No starting state found for {}", consumerPartition);
                hasError = true;
            }
            if (payload.endingOffset == null) {
                logger.warn("No end state found for {}", consumerPartition);
                hasError = true;
            }
            // Skip errors
            if (hasError) {
                continue;
            }

            final PartitionProgress partitionProgress = new PartitionProgress(
                payload.startingOffset,
                currentOffset,
                payload.endingOffset
            );

            progressMap.put(consumerPartition, partitionProgress);
        }

        return Collections.unmodifiableMap(progressMap);
    }

    /**
     * @return - Return the persistence adapter.
     */
    private PersistenceAdapter getPersistenceAdapter() {
        return persistenceAdapter;
    }

    public static class PartitionProgress {
        private final long startingOffset;
        private final long endingOffset;
        private final long currentOffset;
        private final long totalMessages;
        private final long totalUnprocessed;
        private final long totalProcessed;
        private final float percentageComplete;


        public PartitionProgress(long startingOffset, long currentOffset, long endingOffset) {
            // Calculate total number of messages between starting and ending
            totalMessages = (endingOffset - startingOffset);

            // Calculate total un-processed
            totalUnprocessed = (endingOffset - currentOffset);

            // Calculate total processed
            totalProcessed = (currentOffset - startingOffset);

            // Calculate percentage we've worked through
            if (totalMessages == 0) {
                percentageComplete = 0;
            } else {
                percentageComplete = ((float) totalProcessed / totalMessages) * 100;
            }

            this.startingOffset = startingOffset;
            this.currentOffset = currentOffset;
            this.endingOffset = endingOffset;
        }

        public long getTotalMessages() {
            return totalMessages;
        }

        public long getTotalUnprocessed() {
            return totalUnprocessed;
        }

        public long getTotalProcessed() {
            return totalProcessed;
        }

        public float getPercentageComplete() {
            return percentageComplete;
        }

        public long getStartingOffset() {
            return startingOffset;
        }

        public long getCurrentOffset() {
            return currentOffset;
        }

        public long getEndingOffset() {
            return endingOffset;
        }

        @Override
        public String toString() {
            return "PartitionProgress{"
                + "startingOffset=" + startingOffset
                + ", totalMessages=" + totalMessages
                + ", totalUnprocessed=" + totalUnprocessed
                + ", totalProcessed=" + totalProcessed
                + ", percentageComplete=" + percentageComplete
                + '}';
        }
    }
}
