package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.persistence.PersistenceManager;
import com.salesforce.storm.spout.sideline.persistence.SidelinePayload;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

public class SidelineConsumerMonitor {
    private static final Logger logger = LoggerFactory.getLogger(SidelineConsumerMonitor.class);

    final PersistenceManager persistenceManager;

    public SidelineConsumerMonitor(PersistenceManager persistenceManager) {
        this.persistenceManager = persistenceManager;
    }

    public Map<TopicPartition, PartitionProgress> getStatus(final String virtualSpoutId) {
        // Parse out the SidelineRequestId, this is hacky
        final String[] bits = virtualSpoutId.split("_");
        if (bits.length < 2) {
            // Silently fail
            //logger.warn("Unable to parse virtualSpoutId: {}", virtualSpoutId);
            return null;
        }
        final String sidelineRequestIdStr = bits[bits.length - 1];
        final SidelineRequestIdentifier sidelineRequestIdentifier = new SidelineRequestIdentifier(UUID.fromString(sidelineRequestIdStr));

        // Retrieve status
        final SidelinePayload payload = getPersistenceManager().retrieveSidelineRequest(sidelineRequestIdentifier);
        if (payload == null) {
            // NOthing to do?
            logger.error("Could not find SidelineRequest for Id {}", sidelineRequestIdentifier);
            return null;
        }
        final ConsumerState startingState = payload.startingState;
        final ConsumerState endingState = payload.endingState;

        // Get the state
        ConsumerState currentState = getPersistenceManager().retrieveConsumerState(virtualSpoutId);
        if (currentState == null) {
            logger.error("Could not find Current State for Id {}, assuming consumer unstarted", virtualSpoutId);
            currentState = endingState;
        }

        // Create return map
        Map<TopicPartition, PartitionProgress> progressMap = Maps.newHashMap();

        // Calculate the progress
        for (TopicPartition topicPartition : startingState.getState().keySet()) {
            // Make sure no nulls
            boolean hasError = false;
            if (startingState.getOffsetForTopicAndPartition(topicPartition) == null) {
                logger.warn("No starting state found for {}", topicPartition);
                hasError = true;
            }
            if (currentState.getOffsetForTopicAndPartition(topicPartition) == null) {
                logger.warn("No current state found for {}", topicPartition);
                hasError = true;
            }
            if (endingState.getOffsetForTopicAndPartition(topicPartition) == null) {
                logger.warn("No end state found for {}", topicPartition);
                hasError = true;
            }
            // Skip errors
            if (hasError) {
                continue;
            }

            final PartitionProgress partitionProgress = new PartitionProgress(
                    startingState.getOffsetForTopicAndPartition(topicPartition),
                    currentState.getOffsetForTopicAndPartition(topicPartition),
                    endingState.getOffsetForTopicAndPartition(topicPartition)
            );

            progressMap.put(topicPartition, partitionProgress);
        }

        return Collections.unmodifiableMap(progressMap);

    }

    public void printStatus(final String virtualSpoutId) {
        Map<TopicPartition, PartitionProgress> progressMap = getStatus(virtualSpoutId);
        if (progressMap == null) {
            return;
        }

        // Calculate the progress
        for (Map.Entry<TopicPartition,PartitionProgress> entry : progressMap.entrySet()) {
            final TopicPartition topicPartition = entry.getKey();
            final PartitionProgress partitionProgress = entry.getValue();

            logger.info("Partition: {} => {}% complete [{} of {} processed, {} remaining]",
                topicPartition,
                partitionProgress.getPercentageComplete(),
                partitionProgress.getTotalProcessed(),
                partitionProgress.getTotalMessages(),
                partitionProgress.getTotalUnprocessed()
            );
        }
    }

    private PersistenceManager getPersistenceManager() {
        return persistenceManager;
    }

    public static class PartitionProgress {
        final long totalMessages;
        final long totalUnprocessed;
        final long totalProcessed;
        final float percentageComplete;

        public PartitionProgress(long totalMessages, long totalUnprocessed, long totalProcessed, float percentageComplete) {
            this.totalMessages = totalMessages;
            this.totalUnprocessed = totalUnprocessed;
            this.totalProcessed = totalProcessed;
            this.percentageComplete = percentageComplete;
        }

        public PartitionProgress(long startingOffset, long currentOffset, long endingOffset) {
            // Calculate total number of messages between starting and ending
            totalMessages = (endingOffset - startingOffset);

            // Calculate total un-processed
            totalUnprocessed = (endingOffset - currentOffset);

            // Calculate total processed
            totalProcessed = (currentOffset - startingOffset);

            // Calculate percentage we've worked thru
            if (totalUnprocessed == 0) {
                percentageComplete = 0;
            } else {
                percentageComplete = ((float) totalProcessed / totalUnprocessed) * 100;
            }
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

        @Override
        public String toString() {
            return "PartitionProgress{"
                    + "totalMessages=" + totalMessages
                    + ", totalUnprocessed=" + totalUnprocessed
                    + ", totalProcessed=" + totalProcessed
                    + ", percentageComplete=" + percentageComplete
                    + '}';
        }
    }
}
