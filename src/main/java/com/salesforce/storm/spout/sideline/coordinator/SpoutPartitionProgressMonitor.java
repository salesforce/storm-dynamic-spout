package com.salesforce.storm.spout.sideline.coordinator;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.ConsumerPartition;
import com.salesforce.storm.spout.sideline.kafka.ConsumerState;
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
        final String virtualSpoutId = spout.getVirtualSpoutId();

        // Parse out the SidelineRequestId, this is not ideal.
        final String[] bits = virtualSpoutId.split(":");
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

        final String virtualSpoutId = spout.getVirtualSpoutId();
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
            Long currentOffset = getPersistenceAdapter().retrieveConsumerState(virtualSpoutId, consumerPartition.partition());
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
