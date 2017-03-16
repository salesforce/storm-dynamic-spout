package com.salesforce.storm.spout.sideline.persistence;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import com.salesforce.storm.spout.sideline.trigger.SidelineIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import kafka.api.GroupCoordinatorRequest;
import kafka.cluster.BrokerEndPoint;
import kafka.common.OffsetMetadataAndError;
import kafka.javaapi.GroupCoordinatorResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadata;
import kafka.common.TopicAndPartition;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.network.BlockingChannel;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class KafkaPersistenceManager implements PersistenceManager {
    // Logger
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperPersistenceManager.class);

    // Kafka Configuration
    private int readTimeoutMS = 5000;
    private int correlationId = 0;
    private BlockingChannel channel;

    // Configuration
    private String consumerId = "myConsumerId";
    private String topicName;
    private List<TopicAndPartition> availablePartitions;

    @Override
    public void open(Map topologyConfig) {
        // Grab topic name
        topicName = (String) topologyConfig.get(SidelineSpoutConfig.KAFKA_TOPIC);

        // Grab first brokerHost
        final String[] brokerBits = ((List<String>)topologyConfig.get(SidelineSpoutConfig.KAFKA_BROKERS)).get(0).split(":");
        final String brokerHost = brokerBits[0];
        final int brokerPort;
        if (brokerBits.length == 2) {
            brokerPort = Integer.valueOf(brokerBits[1]);
        } else {
            brokerPort = 9092;
        }

        // Sometimes it takes a few to load the data?
        while (loadConsumerMetadata(brokerHost, brokerPort) == false) {
            logger.error("Failed to load consumer metadata...retrying");
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean loadConsumerMetadata(final String brokerHost, final int brokerPort) {
        // Connect to static kafka broker,
        channel = new BlockingChannel(
                brokerHost, brokerPort,
                BlockingChannel.UseDefaultBufferSize(),
                BlockingChannel.UseDefaultBufferSize(),
                readTimeoutMS);
        channel.connect();

        logger.info("Status {}", channel.isConnected());

        // Issue request asking for which broker contains the metadata/offset data we need
        // Build group coord request & send
        GroupCoordinatorRequest request = new GroupCoordinatorRequest(consumerId, GroupCoordinatorRequest.CurrentVersion(), correlationId++, consumerId);
        channel.send(request);

        // Parse the response
        GroupCoordinatorResponse response = GroupCoordinatorResponse.readFrom(channel.receive().payload());
        logger.info("Response: {}", response);
        logger.info("Coordinator: {}", response.coordinator());

        // This means not available / nothing stored yet?
        if (response.errorCode() == ErrorMapping.ConsumerCoordinatorNotAvailableCode()) {
            // Dunno what to do here... stay connected?
            logger.error("Consumer Coordinator Not Available?");
            return false;
        } else if (response.errorCode() == ErrorMapping.NoError()) {
            BrokerEndPoint offsetManager = response.coordinator();
            logger.info("Reconnecting to correct broker: {}", offsetManager);

            // if the coordinator is different, from the above channel's host then reconnect
            channel.disconnect();
            channel = new BlockingChannel(offsetManager.host(), offsetManager.port(),
                    BlockingChannel.UseDefaultBufferSize(),
                    BlockingChannel.UseDefaultBufferSize(),
                    5000 /* read timeout in millis */);
            channel.connect();
        }

        // Now determine which partitions are available.
        List<TopicAndPartition> partitionIdsFound = Lists.newArrayList();
        channel.send(new TopicMetadataRequest(Lists.newArrayList(topicName), correlationId++));
        TopicMetadataResponse topicMetadataResponse = new TopicMetadataResponse(kafka.api.TopicMetadataResponse.readFrom(channel.receive().payload()));
        logger.info("Available: {}", topicMetadataResponse.topicsMetadata());
        for (TopicMetadata topicMetadata : topicMetadataResponse.topicsMetadata()) {
            for (PartitionMetadata partitionMetadata: topicMetadata.partitionsMetadata()) {
                partitionIdsFound.add(new TopicAndPartition(topicName, partitionMetadata.partitionId()));
            }
        }
        availablePartitions = Collections.unmodifiableList(partitionIdsFound);

        return true;
    }

    @Override
    public void close() {
        if (channel != null && channel.isConnected()) {
            channel.disconnect();
        }
    }

    @Override
    public void persistConsumerState(String consumerId, ConsumerState consumerState) {
        long now = System.currentTimeMillis();
        final Map<TopicAndPartition, OffsetAndMetadata> offsets = Maps.newHashMap();

        // Build Topic And Partitions
        for (TopicPartition topicPartition : consumerState.getTopicPartitions()) {
            final long offset = consumerState.getOffsetForTopicAndPartition(topicPartition);
            final TopicAndPartition topicAndPartition = new TopicAndPartition(topicPartition.topic(), topicPartition.partition());
            logger.info("Committing offset {} => {}", topicPartition, offset);
            offsets.put(topicAndPartition, new OffsetAndMetadata(new OffsetMetadata(offset, "my-metadata"), now, now + 1000000L));
        }
        OffsetCommitRequest commitRequest = new OffsetCommitRequest(
                consumerId,
                offsets,
                correlationId++,
                consumerId,
                (short) 1
        );

        channel.send(commitRequest.underlying());
        OffsetCommitResponse commitResponse = OffsetCommitResponse.readFrom(channel.receive().payload());
        logger.info("Commit Response has errors? {}", commitResponse.hasError());
        if (commitResponse.hasError()) {
            for (Object partitionErrorCode: commitResponse.errors().values()) {
                logger.error("Error Code {}", partitionErrorCode);

                if ((short) partitionErrorCode == ErrorMapping.OffsetMetadataTooLargeCode()) {
                    // You must reduce the size of the metadata if you wish to retry
                    logger.error("Metadata value too large!");
                } else if ((short) partitionErrorCode == ErrorMapping.NotCoordinatorForConsumerCode() || (short) partitionErrorCode == ErrorMapping.ConsumerCoordinatorNotAvailableCode()) {
                    channel.disconnect();
                    // Go to step 1 (offset manager has moved) and then retry the commit to the new offset manager
                    logger.error("Offsetmanager has moved :(");
                } else {
                    // log and retry the commit
                    logger.error("Some other error :/");
                }
            }
        }
    }

    @Override
    public ConsumerState retrieveConsumerState(String consumerId) {
        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(
                consumerId,
                availablePartitions,
                (short) 1 /* version */, // version 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
                correlationId++,
                consumerId);

        channel.send(fetchRequest.underlying());
        OffsetFetchResponse fetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload());

        for (TopicAndPartition partition: availablePartitions) {
            OffsetMetadataAndError result = fetchResponse.offsets().get(partition);

            short offsetFetchErrorCode = result.error();
            if (offsetFetchErrorCode == ErrorMapping.NotCoordinatorForConsumerCode()) {
                logger.error("No coordinator for consumer code");
                channel.disconnect();
                // Go to step 1 and retry the offset fetch
            } else if (offsetFetchErrorCode == ErrorMapping.OffsetsLoadInProgressCode()) {
                // retry the offset fetch (after backoff)
                logger.error("Load in progress?");
            } else {
                long retrievedOffset = result.offset();
                String retrievedMetadata = result.metadata();

                logger.info("Partition {} => offset {} (metadata: {})", partition.partition(), retrievedOffset, retrievedMetadata);
            }
        }

        return null;
    }

    @Override
    public void clearConsumerState(String consumerId) {
        // Not implemented?
    }

    @Override
    public void persistSidelineRequestState(SidelineType type, SidelineIdentifier id, SidelineRequest request, ConsumerState startingState, ConsumerState endingState) {
        // Can this be stored into kafka?
    }

    @Override
    public SidelinePayload retrieveSidelineRequest(SidelineIdentifier id) {
        // Can this be stored into kafka?
        return null;
    }

    @Override
    public List<SidelineIdentifier> listSidelineRequests() {
        // Can this be stored into kafka?
        return null;
    }
}
