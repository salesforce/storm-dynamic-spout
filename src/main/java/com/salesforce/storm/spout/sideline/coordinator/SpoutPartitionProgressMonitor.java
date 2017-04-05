package com.salesforce.storm.spout.sideline.coordinator;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.kafka.ConsumerState;
import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.persistence.SidelinePayload;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * Extremely hacky way to monitor VirtualSpout progress.
 */
public class SpoutPartitionProgressMonitor {
    private static final Logger logger = LoggerFactory.getLogger(SpoutPartitionProgressMonitor.class);

    final PersistenceAdapter persistenceAdapter;
    final Map<TopicPartition, PartitionProgress> mainProgressMap = Maps.newHashMap();

    public SpoutPartitionProgressMonitor(PersistenceAdapter persistenceAdapter) {
        this.persistenceAdapter = persistenceAdapter;
    }

    public void open(Map topologyConfig) {
        persistenceAdapter.open(topologyConfig);
    }

    public void close() {
        persistenceAdapter.close();
    }

    public Map<TopicPartition, PartitionProgress> getStatus(final DelegateSidelineSpout spout) {
        final SidelineRequestIdentifier sidelineIdentifier = getSidelineRequestIdentifier(spout);

        if (sidelineIdentifier == null) {
            // This is the main
            return handleMainVirtualSpout(spout);
        } else {
            // This is a sideline request virtual spout
            return handleSidelineVirtualSpout(spout);
        }
    }

    private SidelineRequestIdentifier getSidelineRequestIdentifier(final DelegateSidelineSpout spout) {
        final String virtualSpoutId = spout.getVirtualSpoutId();

        // Parse out the SidelineRequestId, this is hacky
        final String[] bits = virtualSpoutId.split(":");
        if (bits.length != 2) {
            logger.warn("Unable to parse virtualSpoutId: {}", virtualSpoutId);
            return null;
        }

        // Grab 2nd bit.
        final String sidelineRequestIdString = bits[1];

        if (sidelineRequestIdString.equals("main")) {
            return null;
        } else {
            return new SidelineRequestIdentifier(UUID.fromString(sidelineRequestIdString));
        }

    }

    private Map<TopicPartition, PartitionProgress> handleMainVirtualSpout(final DelegateSidelineSpout spout) {
        final ConsumerState currentState = spout.getCurrentState();

        for (TopicPartition topicPartition : currentState.getTopicPartitions()) {
            final PartitionProgress previousProgress = mainProgressMap.get(topicPartition);
            final long currentOffset = currentState.getOffsetForTopicAndPartition(topicPartition);

            if (previousProgress == null) {
                mainProgressMap.put(topicPartition, new PartitionProgress(currentOffset, currentOffset, currentOffset));
                continue;
            }

            // Build new progress
            mainProgressMap.put(topicPartition, new PartitionProgress(previousProgress.getStartingOffset(), currentOffset, currentOffset));
        }

        return Collections.unmodifiableMap(mainProgressMap);
    }

    private Map<TopicPartition, PartitionProgress> handleSidelineVirtualSpout(final DelegateSidelineSpout spout) {
        // Create return map
        Map<TopicPartition, PartitionProgress> progressMap = Maps.newHashMap();

        final String virtualSpoutId = spout.getVirtualSpoutId();
        final SidelineRequestIdentifier sidelineRequestIdentifier = getSidelineRequestIdentifier(spout);
        final ConsumerState currentState = spout.getCurrentState();

        for (TopicPartition topicPartition : currentState.getTopicPartitions()) {
            // Retrieve status
            final SidelinePayload payload = getPersistenceAdapter().retrieveSidelineRequest(
                sidelineRequestIdentifier,
                topicPartition.partition()
            );

            if (payload == null) {
                // Nothing to do?
                logger.error("Could not find SidelineRequest for Id {}", sidelineRequestIdentifier);
                return null;
            }

            // Get the state
            Long currentOffset = getPersistenceAdapter().retrieveConsumerState(virtualSpoutId, topicPartition.partition());
            if (currentOffset == null) {
                logger.error("Could not find Current State for Id {}, assuming consumer has no previous state", virtualSpoutId);
                continue;
            }

            // Make sure no nulls
            boolean hasError = false;
            if (payload.startingOffset == null) {
                logger.warn("No starting state found for {}", topicPartition);
                hasError = true;
            }
            if (payload.endingOffset == null) {
                logger.warn("No end state found for {}", topicPartition);
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

            progressMap.put(topicPartition, partitionProgress);
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
