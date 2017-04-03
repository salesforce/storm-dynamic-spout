package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Maps;
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
public class SidelineConsumerMonitor {
    private static final Logger logger = LoggerFactory.getLogger(SidelineConsumerMonitor.class);

    final PersistenceAdapter persistenceAdapter;
    final Map<TopicPartition, PartitionProgress> mainProgressMap = Maps.newHashMap();

    public SidelineConsumerMonitor(PersistenceAdapter persistenceAdapter) {
        this.persistenceAdapter = persistenceAdapter;
    }

    public void open(Map topologyConfig) {
        persistenceAdapter.open(topologyConfig);
    }

    public void close() {
        persistenceAdapter.close();
    }

    public Map<TopicPartition, PartitionProgress> getStatus(final String virtualSpoutId) {
        // Parse out the SidelineRequestId, this is hacky
        final String[] bits = virtualSpoutId.split(":");
        if (bits.length != 2) {
            logger.warn("Unable to parse virtualSpoutId: {}", virtualSpoutId);
            return null;
        }

        // Grab 2nd bit.
        final String sidelineRequestIdStr = bits[1];

        if (sidelineRequestIdStr.equals("main")) {
            // This is the main
            return handleMainVirtualSpout(virtualSpoutId);
        } else {
            // This is a sideline request virtual spout
            return handleSidelineVirtualSpout(virtualSpoutId, new SidelineRequestIdentifier(UUID.fromString(sidelineRequestIdStr)));
        }
    }

    private Map<TopicPartition, PartitionProgress> handleMainVirtualSpout(final String virtualSpoutId) {
        // TODO: This is super hacky and should be changed
        // We have no idea how many partitions there are... so start at 0 and go up to a max value
        // until we get a null back.  Kind of hacky.
        for (int partitionId = 0; partitionId < 100; partitionId++) {
            final Long currentOffset = getPersistenceAdapter().retrieveConsumerState(virtualSpoutId, partitionId);
            if (currentOffset == null) {
                // break out of loop;
                break;
            }

            final TopicPartition topicPartition = new TopicPartition("Topic", partitionId);

            // Get previous progress
            PartitionProgress previousProgress = mainProgressMap.get(topicPartition);
            if (previousProgress == null) {
                mainProgressMap.put(topicPartition, new PartitionProgress(currentOffset, currentOffset, currentOffset));
                continue;
            }
            // Build new progress
            mainProgressMap.put(topicPartition, new PartitionProgress(previousProgress.getStartingOffset(), currentOffset, currentOffset));
        }

        return Collections.unmodifiableMap(mainProgressMap);
    }

    private Map<TopicPartition, PartitionProgress> handleSidelineVirtualSpout(final String virtualSpoutId, final SidelineRequestIdentifier sidelineRequestIdentifier) {
        // Create return map
        Map<TopicPartition, PartitionProgress> progressMap = Maps.newHashMap();

        // Retrieve status
        /*
        final SidelinePayload payload = getPersistenceAdapter().retrieveSidelineRequest(sidelineRequestIdentifier, );
        if (payload == null) {
            // Nothing to do?
            logger.error("Could not find SidelineRequest for Id {}", sidelineRequestIdentifier);
            return null;
        }

        // Calculate the progress
        for (TopicPartition topicPartition : startingState.getTopicPartitions()) {
            final SidelinePayload payload = getPersistenceAdapter().retrieveSidelineRequest(sidelineRequestIdentifier, topicPartition.partition());

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
            if (payload.endingOffset) {
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
        */

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

            logger.info("{} => {}% complete [{} of {} processed, {} remaining]",
                topicPartition,
                partitionProgress.getPercentageComplete(),
                partitionProgress.getTotalProcessed(),
                partitionProgress.getTotalMessages(),
                partitionProgress.getTotalUnprocessed()
            );
        }
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
