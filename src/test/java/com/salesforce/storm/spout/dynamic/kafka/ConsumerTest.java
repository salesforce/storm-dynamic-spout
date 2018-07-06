/*
 * Copyright (c) 2017, 2018, Salesforce.com, Inc.
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

package com.salesforce.storm.spout.dynamic.kafka;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.ProducedKafkaRecord;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerPeerContext;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerState;
import com.salesforce.storm.spout.dynamic.consumer.Record;
import com.salesforce.storm.spout.dynamic.kafka.deserializer.NullDeserializer;
import com.salesforce.storm.spout.dynamic.kafka.deserializer.Utf8StringDeserializer;
import com.salesforce.storm.spout.dynamic.metrics.LogRecorder;
import com.salesforce.storm.spout.dynamic.persistence.InMemoryPersistenceAdapter;
import com.salesforce.storm.spout.dynamic.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.dynamic.persistence.ZookeeperPersistenceAdapter;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Validates that our Kafka Consumer works as we expect under various scenarios.
 */
public class ConsumerTest {
    // TODO: these test cases
    // test calling open() w/ a starting state.

    private static final Logger logger = LoggerFactory.getLogger(ConsumerTest.class);

    /**
     * We generate a unique topic name for every test case.
     */
    private String topicName;

    /**
     * Create shared kafka test server.
     */
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();

    /**
     * This happens once before every test method.
     * Create a new empty namespace with randomly generated name.
     */
    @BeforeEach
    public void beforeTest() {
        // Generate unique namespace name
        topicName = ConsumerTest.class.getSimpleName() + Clock.systemUTC().millis();

        // Create namespace
        getKafkaTestUtils().createTopic(topicName, 1, (short) 1);
    }

    /**
     * Tests that open() saves off instances of things passed into it properly.
     */
    @Test
    public void testOpenSetsProperties() {
        // Create config
        final Map<String, Object> config = getDefaultConfig(topicName);

        // Create instance of a StateConsumer, we'll just use a dummy instance.
        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(new HashMap<>());

        // Generate a VirtualSpoutIdentifier
        final VirtualSpoutIdentifier virtualSpoutIdentifier = getDefaultVSpoutId();

        // Generate a consumer cohort def
        final ConsumerPeerContext consumerPeerContext = getDefaultConsumerCohortDefinition();

        // Define expected kafka brokers
        final String expectedKafkaBrokers = sharedKafkaTestResource.getKafkaConnectString();

        // Call constructor
        final Consumer consumer = new Consumer();
        consumer.open(config, virtualSpoutIdentifier, consumerPeerContext, persistenceAdapter, new LogRecorder(), null);

        // Validate our instances got set
        assertNotNull(consumer.getConsumerConfig(), "Config is not null");

        // Deeper validation of generated ConsumerConfig
        final KafkaConsumerConfig foundConsumerConfig = consumer.getConsumerConfig();
        assertEquals(virtualSpoutIdentifier.toString(), foundConsumerConfig.getConsumerId(), "ConsumerIdSet as expected");
        assertEquals(topicName, foundConsumerConfig.getTopic(), "Topic set correctly");
        assertEquals(
            expectedKafkaBrokers,
            foundConsumerConfig.getKafkaConsumerProperties().getProperty(BOOTSTRAP_SERVERS_CONFIG),
            "KafkaBrokers set correctly"
        );
        assertEquals(
            consumerPeerContext.getTotalInstances(),
            consumer.getConsumerConfig().getNumberOfConsumers(),
            "Set Number of Consumers as expected"
        );
        assertEquals(
            consumerPeerContext.getInstanceNumber(),
            consumer.getConsumerConfig().getIndexOfConsumer(),
            "Set Index of OUR Consumer is set as expected"
        );

        // Additional properties set correctly
        assertNotNull(consumer.getPersistenceAdapter(), "PersistenceAdapter is not null");
        assertEquals(persistenceAdapter, consumer.getPersistenceAdapter());
        assertNotNull(consumer.getDeserializer(), "Deserializer is not null");
        assertTrue(consumer.getDeserializer() instanceof Utf8StringDeserializer);

        consumer.close();
    }

    /**
     * test calling connect twice throws exception.
     * This test uses mocks so we don't have dangling connections things sticking around
     * after the exception.
     */
    @Test
    public void testCallConnectMultipleTimes() {
        // Define our ConsumerId
        final String consumerId = "MyConsumerId";

        // Create re-usable TopicPartition instance
        final TopicPartition partition0 = new TopicPartition(topicName, 0);

        // Define partition 0's earliest position at 1000L
        final long earliestPosition = 1000L;

        // Setup our config
        final Map<String, Object> config = getDefaultConfig();

        // Create mock KafkaConsumer instance
        @SuppressWarnings("unchecked")
        final KafkaConsumer<byte[], byte[]> mockKafkaConsumer = (KafkaConsumer<byte[], byte[]>) mock(KafkaConsumer.class);

        // When we call partitionsFor(), we should return a single partition number 0 for our namespace.
        final List<PartitionInfo> mockPartitionInfos = Collections.singletonList(
            new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0])
        );
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // When we ask for the position of partition 0, we should return 1000L
        when(mockKafkaConsumer.position(partition0)).thenReturn(earliestPosition);

        // Create instance of a StateConsumer, we'll just use a dummy instance.
        final PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);

        // When getState is called, return the following state
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), anyInt())).thenReturn(null);

        // Call constructor injecting our mocks
        final Consumer consumer = new Consumer(mockKafkaConsumer);

        // Now call open
        consumer.open(
            config,
            getDefaultVSpoutId(),
            getDefaultConsumerCohortDefinition(),
            mockPersistenceAdapter,
            new LogRecorder(),
            null
        );

        final Throwable thrown = Assertions.assertThrows(IllegalStateException.class, () ->
            // Now call open again, we expect this to throw an exception
            consumer.open(
                config,
                getDefaultVSpoutId(),
                getDefaultConsumerCohortDefinition(),
                mockPersistenceAdapter,
                new LogRecorder(),
                null
            )
        );

        MatcherAssert.assertThat(thrown.getMessage(), Matchers.containsString("open more than once"));
    }

    /**
     * Verifies that when we call connect that it makes the appropriate calls
     * to ConsumerStateManager to initialize.
     *
     * This test has the ConsumerStateManager (a mock) return an empty ConsumerState.
     * We verify that our internal kafka client then knows to start reading every partition from
     * the earliest available offset.
     */
    @Test
    public void testConnectWithSinglePartitionOnTopicWithNoStateSaved() {
        // Define our ConsumerId
        final String consumerId = "MyConsumerId";

        // Create re-usable TopicPartition instance
        final TopicPartition kafkaTopicPartition0 = new TopicPartition(topicName, 0);
        final ConsumerPartition partition0 = new ConsumerPartition(topicName, 0);

        // Define partition 0's earliest position at 1000L
        final long earliestPosition = 1000L;

        // Setup our config
        final Map<String, Object> config = getDefaultConfig();

        // Create mock KafkaConsumer instance
        @SuppressWarnings("unchecked")
        final KafkaConsumer<byte[], byte[]> mockKafkaConsumer = (KafkaConsumer<byte[], byte[]>) mock(KafkaConsumer.class);

        // When we call partitionsFor(), we should return a single partition number 0 for our namespace.
        final List<PartitionInfo> mockPartitionInfos = Arrays.asList(
            new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0])
        );
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // When we ask for the position of partition 0, we should return 1000L
        when(mockKafkaConsumer.position(kafkaTopicPartition0)).thenReturn(earliestPosition);

        // Create instance of a StateConsumer, we'll just use a dummy instance.
        final PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);

        // When getState is called, return the following state
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), anyInt())).thenReturn(null);

        // Call constructor injecting our mocks
        final Consumer consumer = new Consumer(mockKafkaConsumer);
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), mockPersistenceAdapter, new LogRecorder(), null);

        // For every partition returned by mockKafkaConsumer.partitionsFor(), we should subscribe to them via the
        // mockKafkaConsumer.assign() call
        verify(mockKafkaConsumer, times(1)).assign(eq(Collections.singletonList(kafkaTopicPartition0)));

        // Since ConsumerStateManager has no state for partition 0, we should call seekToBeginning on that partition
        verify(mockKafkaConsumer, times(1)).seekToBeginning(eq(Collections.singletonList(kafkaTopicPartition0)));

        // Verify position was asked for
        verify(mockKafkaConsumer, times(1)).position(kafkaTopicPartition0);

        // Verify ConsumerStateManager returns the correct values
        final ConsumerState currentState = consumer.getCurrentState();
        assertNotNull(currentState, "Should be non-null");

        // State should have one entry
        assertEquals(1, currentState.size(), "Should have 1 entry");

        // Offset should have offset 1000L - 1 for completed offset.
        assertEquals(
            (earliestPosition - 1),
            (long) currentState.getOffsetForNamespaceAndPartition(partition0),
            "Expected value should be 999"
        );
    }

    /**
     * Verifies that when we call connect that it makes the appropriate calls
     * to ConsumerStateManager to initialize.
     *
     * This test has the ConsumerStateManager (a mock) return an empty ConsumerState.
     * We verify that our internal kafka client then knows to start reading every partition from
     * the earliest available offset.
     */
    @Test
    public void testConnectWithMultiplePartitionsOnTopicWithNoStateSaved() {
        // Define our ConsumerId
        final String consumerId = "MyConsumerId";

        // Some re-usable TopicPartition objects
        final TopicPartition kafkaTopicPartition0 = new TopicPartition(topicName, 0);
        final TopicPartition kafkaTopicPartition1 = new TopicPartition(topicName, 1);
        final TopicPartition kafkaTopicPartition2 = new TopicPartition(topicName, 2);

        final ConsumerPartition partition0 = new ConsumerPartition(topicName, 0);
        final ConsumerPartition partition1 = new ConsumerPartition(topicName, 1);
        final ConsumerPartition partition2 = new ConsumerPartition(topicName, 2);

        // Define earliest positions for each partition
        final long earliestPositionPartition0 = 1000L;
        final long earliestPositionPartition1 = 0L;
        final long earliestPositionPartition2 = 2324L;

        // Setup our config
        final Map<String, Object> config = getDefaultConfig();

        // Create mock KafkaConsumer instance
        @SuppressWarnings("unchecked")
        final KafkaConsumer<byte[], byte[]> mockKafkaConsumer = (KafkaConsumer<byte[], byte[]>) mock(KafkaConsumer.class);

        // When we call partitionsFor(), We return partitions 0,1, and 2.
        final List<PartitionInfo> mockPartitionInfos = Arrays.asList(
            new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 1, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 2, new Node(0, "localhost", 9092), new Node[0], new Node[0])
        );
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // Create instance of a PersistenceAdapter, we'll just use a dummy instance.
        final PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);

        // When getState is called, return the following state
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), anyInt())).thenReturn(null);

        // When we ask for the positions for each partition return mocked values
        when(mockKafkaConsumer.position(kafkaTopicPartition0)).thenReturn(earliestPositionPartition0);
        when(mockKafkaConsumer.position(kafkaTopicPartition1)).thenReturn(earliestPositionPartition1);
        when(mockKafkaConsumer.position(kafkaTopicPartition2)).thenReturn(earliestPositionPartition2);

        // Call constructor injecting our mocks
        final Consumer consumer = new Consumer(mockKafkaConsumer);
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), mockPersistenceAdapter, new LogRecorder(), null);

        // For every partition returned by mockKafkaConsumer.partitionsFor(), we should subscribe to them via the
        // mockKafkaConsumer.assign() call
        verify(mockKafkaConsumer, times(1)).assign(eq(Arrays.asList(
            new TopicPartition(topicName, 0),
            new TopicPartition(topicName, 1),
            new TopicPartition(topicName, 2)
        )));

        // Since ConsumerStateManager has no state for partition 0, we should call seekToBeginning on that partition
        verify(mockKafkaConsumer, times(1)).seekToBeginning(eq(Collections.singletonList(
            new TopicPartition(topicName, 0)
        )));
        verify(mockKafkaConsumer, times(1)).seekToBeginning(eq(Collections.singletonList(
            new TopicPartition(topicName, 1)
        )));
        verify(mockKafkaConsumer, times(1)).seekToBeginning(eq(Collections.singletonList(
            new TopicPartition(topicName, 2)
        )));

        // Validate we got our calls for the current position
        verify(mockKafkaConsumer, times(1)).position(kafkaTopicPartition0);
        verify(mockKafkaConsumer, times(1)).position(kafkaTopicPartition1);
        verify(mockKafkaConsumer, times(1)).position(kafkaTopicPartition2);

        // Verify ConsumerStateManager returns the correct values
        final ConsumerState currentState = consumer.getCurrentState();
        assertNotNull(currentState, "Should be non-null");

        // State should have one entry
        assertEquals(3, currentState.size(), "Should have 3 entries");

        // Offsets should be the earliest position - 1
        assertEquals(
            (earliestPositionPartition0 - 1),
            (long) currentState.getOffsetForNamespaceAndPartition(partition0),
            "Expected value for partition0"
        );
        assertEquals(
            (earliestPositionPartition1 - 1),
            (long) currentState.getOffsetForNamespaceAndPartition(partition1),
            "Expected value for partition1"
        );
        assertEquals(
            (earliestPositionPartition2 - 1),
            (long) currentState.getOffsetForNamespaceAndPartition(partition2),
            "Expected value for partition2"
        );
    }

    /**
     * Verifies that when we call connect that it makes the appropriate calls
     * to ConsumerStateManager to initialize.
     *
     * This test has the ConsumerStateManager (a mock) return ConsumerState.
     * We verify that our internal kafka client then knows to start reading its partition from the previously saved off
     * consumer state returned from ConsumerStateManager.
     */
    @Test
    public void testConnectWithSinglePartitionOnTopicWithStateSaved() {
        // Define our ConsumerId and expected offset.
        final String consumerId = "MyConsumerId";

        // Some re-usable TopicPartition objects
        final TopicPartition partition0 = new TopicPartition(topicName, 0);

        // This defines the last offset that was committed.
        final long lastCommittedOffset = 12345L;

        // This defines what offset we're expected to start consuming from, which should be the lastCommittedOffset + 1.
        final long expectedOffsetToStartConsumeFrom = lastCommittedOffset + 1;

        // Setup our config
        final Map<String, Object> config = getDefaultConfig();

        // Create mock KafkaConsumer instance
        @SuppressWarnings("unchecked")
        final KafkaConsumer<byte[], byte[]> mockKafkaConsumer = (KafkaConsumer<byte[], byte[]>) mock(KafkaConsumer.class);

        // When we call partitionsFor(), we should return a single partition number 0 for our namespace.
        final List<PartitionInfo> mockPartitionInfos = Collections.singletonList(
            new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0])
        );
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // Create instance of a StateConsumer, we'll just use a dummy instance.
        final PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);

        // When getState is called, return the following state
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(0))).thenReturn(lastCommittedOffset);

        // Call constructor injecting our mocks
        final Consumer consumer = new Consumer(mockKafkaConsumer);
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), mockPersistenceAdapter, new LogRecorder(), null);

        // For every partition returned by mockKafkaConsumer.partitionsFor(), we should subscribe to them via the
        // mockKafkaConsumer.assign() call
        verify(mockKafkaConsumer, times(1)).assign(eq(Collections.singletonList(partition0)));

        // Since ConsumerStateManager has state for partition 0, we should NEVER call seekToBeginning on that partition
        // Or any partition really.
        verify(mockKafkaConsumer, never()).seekToBeginning(anyCollection());

        // Instead since there is state, we should call seek on that partition
        verify(mockKafkaConsumer, times(1)).seek(eq(new TopicPartition(topicName, 0)), eq(expectedOffsetToStartConsumeFrom));
    }

    /**
     * Verifies that when we call connect that it makes the appropriate calls
     * to ConsumerStateManager to initialize.
     *
     * This test has the ConsumerStateManager (a mock) return ConsumerState for every partition on the namespace.
     * We verify that our internal kafka client then knows to start reading from the previously saved consumer state
     * offsets
     */
    @Test
    public void testConnectWithMultiplePartitionsOnTopicWithAllPreviouslySavedState() {
        // Define our ConsumerId
        final String consumerId = "MyConsumerId";

        // Define partitionTopics so we can re-use em
        final TopicPartition partition0 = new TopicPartition(topicName, 0);
        final TopicPartition partition1 = new TopicPartition(topicName, 1);
        final TopicPartition partition2 = new TopicPartition(topicName, 2);

        // Define last committed offsets per partition
        final long lastCommittedOffsetPartition0 = 1234L;
        final long lastCommittedOffsetPartition1 = 4321L;
        final long lastCommittedOffsetPartition2 = 1337L;

        // Define the offsets for each partition we expect the consumer to start consuming from, which should
        // be last committed offset + 1
        final long expectedPartition0Offset = lastCommittedOffsetPartition0 + 1;
        final long expectedPartition1Offset = lastCommittedOffsetPartition1 + 1;
        final long expectedPartition2Offset = lastCommittedOffsetPartition2 + 1;

        // Setup our config
        final Map<String, Object> config = getDefaultConfig();

        // Create mock KafkaConsumer instance
        @SuppressWarnings("unchecked")
        final KafkaConsumer<byte[], byte[]> mockKafkaConsumer = (KafkaConsumer<byte[], byte[]>) mock(KafkaConsumer.class);

        // When we call partitionsFor(), We return partitions 0,1, and 2.
        final List<PartitionInfo> mockPartitionInfos = Arrays.asList(
            new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 1, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 2, new Node(0, "localhost", 9092), new Node[0], new Node[0])
        );
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // Create instance of a StateConsumer, we'll just use a dummy instance.
        final PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);

        // When getState is called, return the following state
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(partition0.partition())))
            .thenReturn(lastCommittedOffsetPartition0);
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(partition1.partition())))
            .thenReturn(lastCommittedOffsetPartition1);
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(partition2.partition())))
            .thenReturn(lastCommittedOffsetPartition2);

        // Call constructor injecting our mocks
        final Consumer consumer = new Consumer(mockKafkaConsumer);

        // Now call open
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), mockPersistenceAdapter, new LogRecorder(), null);

        // For every partition returned by mockKafkaConsumer.partitionsFor(), we should subscribe to them via the
        // mockKafkaConsumer.assign() call
        verify(mockKafkaConsumer, times(1)).assign(eq(Arrays.asList(
            partition0,
            partition1,
            partition2
        )));

        // Since ConsumerStateManager has state for all partitions, we should never call seekToBeginning on any partitions
        verify(mockKafkaConsumer, never()).seekToBeginning(anyList());

        // Instead since there is state, we should call seek on each partition
        final InOrder inOrderVerification = inOrder(mockKafkaConsumer);
        inOrderVerification.verify(mockKafkaConsumer, times(1)).seek(eq(partition0), eq(expectedPartition0Offset));
        inOrderVerification.verify(mockKafkaConsumer, times(1)).seek(eq(partition1), eq(expectedPartition1Offset));
        inOrderVerification.verify(mockKafkaConsumer, times(1)).seek(eq(partition2), eq(expectedPartition2Offset));
    }

    /**
     * Verifies that when we call connect that it makes the appropriate calls
     * to ConsumerStateManager to initialize.
     *
     * This test has the ConsumerStateManager (a mock) return ConsumerState for every partition on the namespace.
     * We verify that our internal kafka client then knows to start reading from the previously saved consumer state
     * offsets
     *
     * We setup this test with 4 partitions in our namespace, 0 through 3
     *
     * We setup Partitions 0 and 2 to have previously saved state -- We should resume from this previous state
     * We setup Partitions 1 and 3 to have no previously saved state -- We should resume from the earliest offset in the partition.
     */
    @Test
    public void testConnectWithMultiplePartitionsOnTopicWithSomePreviouslySavedState() {
        // Define our ConsumerId
        final String consumerId = "MyConsumerId";

        // Define partitionTopics so we can re-use em
        final TopicPartition kafkaTopicPartition0 = new TopicPartition(topicName, 0);
        final TopicPartition kafkaTopicPartition1 = new TopicPartition(topicName, 1);
        final TopicPartition kafkaTopicPartition2 = new TopicPartition(topicName, 2);
        final TopicPartition kafkaTopicPartition3 = new TopicPartition(topicName, 3);

        final ConsumerPartition partition0 = new ConsumerPartition(topicName, 0);
        final ConsumerPartition partition1 = new ConsumerPartition(topicName, 1);
        final ConsumerPartition partition2 = new ConsumerPartition(topicName, 2);
        final ConsumerPartition partition3 = new ConsumerPartition(topicName, 3);

        // Define last committed offsets per partition, partitions 1 and 3 have no previously saved state.
        final long lastCommittedOffsetPartition0 = 1234L;
        final long lastCommittedOffsetPartition2 = 1337L;

        // Define what offsets we expect to start consuming from, which should be our last committed offset + 1
        final long expectedPartition0Offset = lastCommittedOffsetPartition0 + 1;
        final long expectedPartition2Offset = lastCommittedOffsetPartition2 + 1;

        // Define earliest positions for partitions 1 and 3
        final long earliestOffsetPartition1 = 444L;
        final long earliestOffsetPartition3 = 0L;

        // And the offsets we expect to see in our consumerState
        final long expectedStateOffsetPartition0 = lastCommittedOffsetPartition0;
        final long expectedStateOffsetPartition1 = earliestOffsetPartition1 - 1;
        final long expectedStateOffsetPartition2 = lastCommittedOffsetPartition2;
        final long expectedStateOffsetPartition3 = earliestOffsetPartition3 - 1;

        // Setup our config
        final Map<String, Object> config = getDefaultConfig(topicName);

        // Create mock KafkaConsumer instance
        @SuppressWarnings("unchecked")
        final KafkaConsumer<byte[], byte[]> mockKafkaConsumer = (KafkaConsumer<byte[], byte[]>) mock(KafkaConsumer.class);

        // When we call partitionsFor(), We return partitions 0,1,2,3
        final List<PartitionInfo> mockPartitionInfos = Arrays.asList(
            new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 1, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 2, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
            new PartitionInfo(topicName, 3, new Node(0, "localhost", 9092), new Node[0], new Node[0])
        );
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // Create instance of a StateConsumer, we'll just use a dummy instance.
        final PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);

        // When getState is called, return the following state for partitions 0 and 2.
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(kafkaTopicPartition0.partition())))
            .thenReturn(lastCommittedOffsetPartition0);
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(kafkaTopicPartition1.partition())))
            .thenReturn(null);
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(kafkaTopicPartition2.partition())))
            .thenReturn(lastCommittedOffsetPartition2);
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(kafkaTopicPartition3.partition())))
            .thenReturn(null);

        // Define values returned for partitions without state
        when(mockKafkaConsumer.position(kafkaTopicPartition1)).thenReturn(earliestOffsetPartition1);
        when(mockKafkaConsumer.position(kafkaTopicPartition3)).thenReturn(earliestOffsetPartition3);

        // Call constructor injecting our mocks
        final Consumer consumer = new Consumer(mockKafkaConsumer);

        // Now call open
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), mockPersistenceAdapter, new LogRecorder(), null);

        // For every partition returned by mockKafkaConsumer.partitionsFor(), we should subscribe to them via the
        // mockKafkaConsumer.assign() call
        verify(mockKafkaConsumer, times(1)).assign(eq(Arrays.asList(
            kafkaTopicPartition0,
            kafkaTopicPartition1,
            kafkaTopicPartition2,
            kafkaTopicPartition3
        )));

        // Since ConsumerStateManager has state for only 2 partitions, we should call seekToBeginning on partitions 1 and 3.
        verify(mockKafkaConsumer, times(1)).seekToBeginning(eq(Collections.singletonList(
            kafkaTopicPartition1
        )));
        verify(mockKafkaConsumer, times(1)).seekToBeginning(eq(Collections.singletonList(
            kafkaTopicPartition3
        )));

        // Verify we asked for the positions for 2 unknown state partitions
        verify(mockKafkaConsumer, times(1)).position(kafkaTopicPartition1);
        verify(mockKafkaConsumer, times(1)).position(kafkaTopicPartition3);

        // For the partitions with state, we should call seek on each partition
        final InOrder inOrderVerification = inOrder(mockKafkaConsumer);
        inOrderVerification.verify(mockKafkaConsumer, times(1)).seek(eq(kafkaTopicPartition0), eq(expectedPartition0Offset));
        inOrderVerification.verify(mockKafkaConsumer, times(1)).seek(eq(kafkaTopicPartition2), eq(expectedPartition2Offset));

        // Never seeked for partitions without any state
        verify(mockKafkaConsumer, never()).seek(eq(kafkaTopicPartition1), anyLong());
        verify(mockKafkaConsumer, never()).seek(eq(kafkaTopicPartition3), anyLong());

        // Now validate the consumer state
        final ConsumerState resultingConsumerState = consumer.getCurrentState();

        assertNotNull(resultingConsumerState, "Should be non-null");

        // State should have one entry
        assertEquals(4, resultingConsumerState.size(), "Should have 4 entries");

        // Offsets should be set to what we expected.
        assertEquals(
            expectedStateOffsetPartition0,
            (long) resultingConsumerState.getOffsetForNamespaceAndPartition(partition0),
            "Expected value for partition0"
        );
        assertEquals(
            expectedStateOffsetPartition1,
            (long) resultingConsumerState.getOffsetForNamespaceAndPartition(partition1),
            "Expected value for partition1"
        );
        assertEquals(
            expectedStateOffsetPartition2,
            (long) resultingConsumerState.getOffsetForNamespaceAndPartition(partition2),
            "Expected value for partition2"
        );
        assertEquals(
            expectedStateOffsetPartition3,
            (long) resultingConsumerState.getOffsetForNamespaceAndPartition(partition3),
            "Expected value for partition2"
        );
    }

    /**
     * Tests that the getAssignedPartitions() works as we expect.
     * This test uses a namespace with a single partition that we are subscribed to.
     */
    @Test
    public void testGetAssignedPartitionsWithSinglePartition() {
        final int expectedPartitionId = 0;

        // Setup our config
        final Map<String, Object> config = getDefaultConfig();

        // Create our consumer
        final Consumer consumer = getDefaultConsumerInstanceAndOpen();

        // Ask the underlying consumer for our assigned partitions.
        final Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate it
        assertNotNull(assignedPartitions, "Should be non-null");
        assertFalse(assignedPartitions.isEmpty(), "Should not be empty");
        assertEquals(1, assignedPartitions.size(), "Should contain 1 entry");
        assertTrue(
            assignedPartitions.contains(new ConsumerPartition(topicName, expectedPartitionId)),
            "Should contain our expected namespace/partition"
        );
    }

    /**
     * Tests that the getAssignedPartitions() works as we expect.
     * This test uses a namespace with a single partition that we are subscribed to.
     */
    @Test
    public void testGetAssignedPartitionsWithMultiplePartitions() {
        // Define and create our namespace
        final String expectedTopicName = "testGetAssignedPartitionsWithMultiplePartitions" + System.currentTimeMillis();
        final int expectedNumberOfPartitions = 5;
        getKafkaTestUtils().createTopic(expectedTopicName, expectedNumberOfPartitions, (short) 1);

        // Create our consumer
        final Consumer consumer = getDefaultConsumerInstanceAndOpen(expectedTopicName);

        // Ask the underlying consumer for our assigned partitions.
        final Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate it
        assertNotNull(assignedPartitions, "Should be non-null");
        assertFalse(assignedPartitions.isEmpty(), "Should not be empty");
        assertEquals(expectedNumberOfPartitions, assignedPartitions.size(), "Should contain 5 entries");
        for (int x = 0; x < expectedNumberOfPartitions; x++) {
            assertTrue(
                assignedPartitions.contains(new ConsumerPartition(expectedTopicName, x)),
                "Should contain our expected namespace/partition " + x
            );
        }
    }

    /**
     * Tests that the unsubscribeConsumerPartition() works as we expect.
     * This test uses a namespace with a single partition that we are subscribed to.
     */
    @Test
    public void testUnsubscribeTopicPartitionSinglePartition() {
        // Define our expected namespace/partition
        final ConsumerPartition expectedTopicPartition = new ConsumerPartition(topicName, 0);

        // Create our consumer
        final Consumer consumer = getDefaultConsumerInstanceAndOpen(topicName);

        // Grab persistence adapter instance.
        final PersistenceAdapter persistenceAdapter = consumer.getPersistenceAdapter();

        // Ask the underlying consumer for our assigned partitions.
        Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull(assignedPartitions, "Should be non-null");
        assertFalse(assignedPartitions.isEmpty(), "Should not be empty");
        assertEquals(1, assignedPartitions.size(), "Should contain 1 entries");
        assertTrue(assignedPartitions.contains(expectedTopicPartition), "Should contain our expected namespace/partition 0");

        // Now unsub from our namespace partition
        final boolean result = consumer.unsubscribeConsumerPartition(expectedTopicPartition);
        assertTrue(result, "Should have returned true");

        // Now ask for assigned partitions, should have none
        // Ask the underlying consumer for our assigned partitions.
        assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull(assignedPartitions, "Should be non-null");
        assertTrue(assignedPartitions.isEmpty(), "Should be empty");
        assertEquals(0, assignedPartitions.size(), "Should contain 0 entries");
        assertFalse(assignedPartitions.contains(expectedTopicPartition), "Should NOT contain our expected namespace/partition 0");

        // Now we want to validate that removeConsumerState() removes all state, even for unassigned partitions.

        // Call flush and ensure we have persisted state on partition 0
        consumer.flushConsumerState();
        assertNotNull(
            persistenceAdapter.retrieveConsumerState(consumer.getConsumerId(), 0),
            "Should have state persisted for this partition"
        );

        // If we call removeConsumerState, it should remove all state from the persistence layer
        consumer.removeConsumerState();

        // Validate no more state persisted for partition 0
        assertNull(persistenceAdapter.retrieveConsumerState(consumer.getConsumerId(), 0), "Should have null state");
    }

    /**
     * Tests that the unsubscribeConsumerPartition() works as we expect.
     * This test uses a namespace with multiple partitions that we are subscribed to.
     */
    @Test
    public void testUnsubscribeTopicPartitionMultiplePartitions() {
        // Define and create our namespace
        final String expectedTopicName = "testUnsubscribeTopicPartitionMultiplePartitions" + System.currentTimeMillis();
        final int expectedNumberOfPartitions = 5;
        getKafkaTestUtils().createTopic(expectedTopicName, expectedNumberOfPartitions, (short) 1);

        // Create our consumer
        final Consumer consumer = getDefaultConsumerInstanceAndOpen(expectedTopicName);

        // Grab persistence adapter instance.
        final PersistenceAdapter persistenceAdapter = consumer.getPersistenceAdapter();

        // Ask the underlying consumer for our assigned partitions.
        Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull(assignedPartitions, "Should be non-null");
        assertFalse(assignedPartitions.isEmpty(), "Should not be empty");
        assertEquals(expectedNumberOfPartitions, assignedPartitions.size(), "Should contain entries");
        for (int x = 0; x < expectedNumberOfPartitions; x++) {
            assertTrue(
                assignedPartitions.contains(new ConsumerPartition(expectedTopicName, x)),
                "Should contain our expected namespace/partition " + x
            );
        }

        // Now unsub from our namespace partition
        final int expectedRemovePartition = 2;
        final ConsumerPartition toRemoveTopicPartition = new ConsumerPartition(expectedTopicName, expectedRemovePartition);

        final boolean result = consumer.unsubscribeConsumerPartition(toRemoveTopicPartition);
        assertTrue(result, "Should have returned true");

        // Now ask for assigned partitions, should have none
        // Ask the underlying consumer for our assigned partitions.
        assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull(assignedPartitions, "Should be non-null");
        assertFalse(assignedPartitions.isEmpty(), "Should be not empty");
        assertEquals((expectedNumberOfPartitions - 1), assignedPartitions.size(), "Should contain entries");
        assertFalse(assignedPartitions.contains(toRemoveTopicPartition), "Should NOT contain our removed namespace/partition 0");

        // Attempt to remove the same topicPartitionAgain, it should return false
        final boolean result2 = consumer.unsubscribeConsumerPartition(toRemoveTopicPartition);
        assertFalse(result2, "Should return false the second time");

        // Now remove another namespace/partition
        final int expectedRemovePartition2 = 4;
        final ConsumerPartition toRemoveTopicPartition2 = new ConsumerPartition(expectedTopicName, expectedRemovePartition2);

        final boolean result3 = consumer.unsubscribeConsumerPartition(toRemoveTopicPartition2);
        assertTrue(result3, "Should have returned true");

        // Now ask for assigned partitions, should have none
        // Ask the underlying consumer for our assigned partitions.
        assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull(assignedPartitions, "Should be non-null");
        assertFalse(assignedPartitions.isEmpty(), "Should be not empty");
        assertEquals((expectedNumberOfPartitions - 2), assignedPartitions.size(), "Should contain entries");
        assertFalse(assignedPartitions.contains(toRemoveTopicPartition), "Should NOT contain our removed namespace/partition 1");
        assertFalse(assignedPartitions.contains(toRemoveTopicPartition2), "Should NOT contain our removed namespace/partition 2");

        // Now we want to validate that removeConsumerState() removes all state, even for unassigned partitions.

        // Call flush and ensure we have persisted state on all partitions
        consumer.flushConsumerState();
        for (int partitionId = 0; partitionId < expectedNumberOfPartitions; partitionId++) {
            assertNotNull(
                persistenceAdapter.retrieveConsumerState(consumer.getConsumerId(), partitionId),
                "Should have state persisted for this partition"
            );
        }

        // If we call removeConsumerState, it should remove all state from the persistence layer
        consumer.removeConsumerState();

        // Validate we dont have state on any partitions now
        for (int partitionId = 0; partitionId < expectedNumberOfPartitions; partitionId++) {
            assertNull(
                persistenceAdapter.retrieveConsumerState(consumer.getConsumerId(), partitionId),
                "Should not have state persisted for this partition"
            );
        }
    }

    /**
     * We attempt to consume from the namespace and get our expected messages.
     * We will NOT acknowledge any of the messages as being processed, so it should have no state saved.
     */
    @Test
    public void testSimpleConsumeFromTopicWithNoStateSaved() {
        // Define how many records to produce
        final int numberOfRecordsToProduce = 5;

        // Produce 5 entries to the namespace.
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(numberOfRecordsToProduce, 0);

        // Create our consumer
        final Consumer consumer = getDefaultConsumerInstanceAndOpen();

        // Read from namespace, verify we get what we expect
        for (int x = 0; x < numberOfRecordsToProduce; x++) {
            // Get the next record
            final Record foundRecord = consumer.nextRecord();

            // Compare to what we expected
            final ProducedKafkaRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);

            // Validate em
            validateRecordMatchesInput(expectedRecord, foundRecord);
        }

        // Next one should return null
        final Record foundRecord = consumer.nextRecord();
        assertNull(foundRecord, "Should have nothing new to consume and be null");

        // Close out consumer
        consumer.close();
    }

    /**
     * We attempt to consume from the namespace and get our expected messages.
     * We ack the messages each as we get it, in order, one by one.
     */
    @Test
    public void testSimpleConsumeFromTopicWithAckingInOrderOneByOne() {
        // Define how many records to produce
        final int numberOfRecordsToProduce = 5;

        // Define our topicPartition
        final ConsumerPartition partition0 = new ConsumerPartition(topicName, 0);

        // Produce 5 entries to the namespace.
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(numberOfRecordsToProduce, 0);

        // Create our consumer
        final Consumer consumer = getDefaultConsumerInstanceAndOpen();

        // Read from namespace, verify we get what we expect
        for (int x = 0; x < numberOfRecordsToProduce; x++) {
            // Get next record from consumer
            final Record foundRecord = consumer.nextRecord();

            // Compare to what we expected
            final ProducedKafkaRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);

            // Validate we got what we expected
            validateRecordMatchesInput(expectedRecord, foundRecord);

            // Ack this message
            consumer.commitOffset(foundRecord.getNamespace(), foundRecord.getPartition(), foundRecord.getOffset());

            // Verify it got updated to our current offset
            validateConsumerState(consumer.flushConsumerState(), partition0, foundRecord.getOffset());
        }
        logger.info("Consumer State {}", consumer.flushConsumerState());

        // Close out consumer
        consumer.close();
    }

    /**
     * We attempt to consume from the namespace and get our expected messages.
     * We ack the messages each as we get it, in order, one by one.
     */
    @Test
    public void testSimpleConsumeFromTopicWithAckingInOrderAllAtTheEnd() {
        // Define how many records to produce
        final int numberOfRecordsToProduce = 5;

        // Define our topicPartition
        final ConsumerPartition partition0 = new ConsumerPartition(topicName, 0);

        // Produce 5 entries to the namespace.
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(numberOfRecordsToProduce, 0);

        // Create our consumer
        final Consumer consumer = getDefaultConsumerInstanceAndOpen();

        // Read from namespace, verify we get what we expect
        final List<Record> foundRecords = new ArrayList<>();
        for (int x = 0; x < numberOfRecordsToProduce; x++) {
            // Get next record from consumer
            final Record foundRecord = consumer.nextRecord();

            // Get the next produced record that we expect
            final ProducedKafkaRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);

            // Validate we got what we expected
            validateRecordMatchesInput(expectedRecord, foundRecord);

            // Add to our found records
            foundRecords.add(foundRecord);
        }
        logger.info("Consumer State {}", consumer.flushConsumerState());

        // Verify state is still -1 (meaning it hasn't acked/completed ANY offsets yet)
        validateConsumerState(consumer.flushConsumerState(), partition0, -1L);

        // Now ack them one by one
        for (final Record foundRecord : foundRecords) {
            consumer.commitOffset(foundRecord.getNamespace(), foundRecord.getPartition(), foundRecord.getOffset());
        }

        // Now validate state.
        final ConsumerState consumerState = consumer.flushConsumerState();

        // Verify it got updated to our current offset
        validateConsumerState(consumerState, partition0, (numberOfRecordsToProduce - 1));
        logger.info("Consumer State {}", consumer.flushConsumerState());

        // Close out consumer
        consumer.close();
    }

    /**
     * We attempt to consume from the namespace and get our expected messages.
     * We ack the messages each as we get it, in order, one by one.
     */
    @Test
    public void testSimpleConsumeFromTopicWithAckingOutOfOrderAllAtTheEnd() {
        // Define how many records to produce
        final int numberOfRecordsToProduce = 9;

        // Define our namespace/partition.
        final ConsumerPartition partition0 = new ConsumerPartition(topicName, 0);

        // Produce 5 entries to the namespace.
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(numberOfRecordsToProduce, 0);

        // Create our consumer
        final Consumer consumer = getDefaultConsumerInstanceAndOpen();

        // Read from namespace, verify we get what we expect
        final List<Record> foundRecords = new ArrayList<>();
        for (int x = 0; x < numberOfRecordsToProduce; x++) {
            // Get next record from consumer
            final Record foundRecord = consumer.nextRecord();

            // Get the next produced record that we expect
            final ProducedKafkaRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);

            // Validate we got what we expected
            validateRecordMatchesInput(expectedRecord, foundRecord);

            // Add to our found records list
            foundRecords.add(foundRecord);
        }
        logger.info("Consumer State {}", consumer.flushConsumerState());

        // Verify state is still -1 (meaning it hasn't acked/completed ANY offsets yet)
        validateConsumerState(consumer.flushConsumerState(), partition0, -1L);

        // Now ack in the following order:
        // commit offset 2 => offset should be 0 still
        consumer.commitOffset(partition0.namespace(), partition0.partition(), 2L);
        validateConsumerState(consumer.flushConsumerState(), partition0, -1L);

        // commit offset 1 => offset should be 0 still
        consumer.commitOffset(partition0.namespace(), partition0.partition(), 1L);
        validateConsumerState(consumer.flushConsumerState(), partition0, -1L);

        // commit offset 0 => offset should be 2 now
        consumer.commitOffset(partition0.namespace(), partition0.partition(), 0L);
        validateConsumerState(consumer.flushConsumerState(), partition0, 2L);

        // commit offset 3 => offset should be 3 now
        consumer.commitOffset(partition0.namespace(), partition0.partition(), 3L);
        validateConsumerState(consumer.flushConsumerState(), partition0, 3L);

        // commit offset 4 => offset should be 4 now
        consumer.commitOffset(partition0.namespace(), partition0.partition(), 4L);
        validateConsumerState(consumer.flushConsumerState(), partition0, 4L);

        // commit offset 5 => offset should be 5 now
        consumer.commitOffset(partition0.namespace(), partition0.partition(), 5L);
        validateConsumerState(consumer.flushConsumerState(), partition0, 5L);

        // commit offset 7 => offset should be 5 still
        consumer.commitOffset(partition0.namespace(), partition0.partition(), 7L);
        validateConsumerState(consumer.flushConsumerState(), partition0, 5L);

        // commit offset 8 => offset should be 5 still
        consumer.commitOffset(partition0.namespace(), partition0.partition(), 8L);
        validateConsumerState(consumer.flushConsumerState(), partition0, 5L);

        // commit offset 6 => offset should be 8 now
        consumer.commitOffset(partition0.namespace(), partition0.partition(), 6L);
        validateConsumerState(consumer.flushConsumerState(), partition0, 8L);

        // Now validate state.
        logger.info("Consumer State {}", consumer.flushConsumerState());

        // Close out consumer
        consumer.close();
    }

    /**
     * Tests what happens when you call nextRecord(), and the deserializer fails to
     * deserialize (returns null), then nextRecord() should return null.
     * Additionally it should mark the message as completed.
     */
    @Test
    public void testNextTupleWhenSerializerFailsToDeserialize() {
        // Define how many records to produce
        final int numberOfRecordsToProduce = 5;

        final ConsumerPartition partition0 = new ConsumerPartition(topicName, 0);

        // Produce 5 entries to the namespace.
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(numberOfRecordsToProduce, partition0.partition());

        // Create config
        final Map<String, Object> config = getDefaultConfig(topicName);

        // Set deserializer instance to our null deserializer
        config.put(KafkaConsumerConfig.DESERIALIZER_CLASS, NullDeserializer.class.getName());

        // Create our Persistence Manager
        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(new HashMap<>());

        // Create our consumer
        final Consumer consumer = new Consumer();
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), persistenceAdapter, new LogRecorder(), null);

        // Read from namespace, verify we get what we expect
        for (int x = 0; x < numberOfRecordsToProduce; x++) {
            // Get the next record
            final Record foundRecord = consumer.nextRecord();

            // It should be null
            assertNull(foundRecord, "Null Deserializer produced null return value");

            // Validate our consumer position should increase.
            validateConsumerState(consumer.flushConsumerState(), partition0, x);
        }

        // Next one should return null
        final Record foundRecord = consumer.nextRecord();
        assertNull(foundRecord, "Should have nothing new to consume and be null");

        // Close out consumer
        consumer.close();
    }

    /**
     * Produce 10 messages into a kafka topic: offsets [0-9]
     * Setup our Consumer such that its pre-existing state says to start at offset 4
     * Consume using the Consumer, verify we only get the last 5 messages back.
     */
    @Test
    public void testConsumerWithInitialStateToSkipMessages() {
        // Define how many records to produce
        final int numberOfRecordsToProduce = 10;

        // Produce entries to the namespace.
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(numberOfRecordsToProduce, 0);

        // Create a list of the records we expect to get back from the consumer, this should be the last 5 entries.
        final List<ProducedKafkaRecord<byte[], byte[]>> expectedProducedRecords = producedRecords.subList(5,10);

        // Setup our config
        final Map<String, Object> config = getDefaultConfig();

        // Create virtualSpoutId
        final VirtualSpoutIdentifier virtualSpoutIdentifier = getDefaultVSpoutId();

        // Create our Persistence Manager
        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(new HashMap<>());

        // Create a state in which we have already acked the first 5 messages
        // 5 first msgs marked completed (0,1,2,3,4) = Committed Offset = 4.
        persistenceAdapter.persistConsumerState(virtualSpoutIdentifier.toString(), 0, 4L);

        // Create our consumer
        final Consumer consumer = new Consumer();
        consumer.open(config, virtualSpoutIdentifier, getDefaultConsumerCohortDefinition(), persistenceAdapter, new LogRecorder(), null);

        // Read from namespace, verify we get what we expect, we should only get the last 5 records.
        final List<Record> consumedRecords = asyncConsumeMessages(consumer, 5);
        final Iterator<ProducedKafkaRecord<byte[], byte[]>> expectedProducedRecordsIterator = expectedProducedRecords.iterator();
        for (final Record foundRecord : consumedRecords) {
            // Get the produced record we expected to get back.
            final ProducedKafkaRecord<byte[], byte[]> expectedRecord = expectedProducedRecordsIterator.next();

            // Validate we got what we expected
            validateRecordMatchesInput(expectedRecord, foundRecord);
        }

        // Additional calls to nextRecord() should return null
        for (int x = 0; x < 2; x++) {
            final Record foundRecord = consumer.nextRecord();
            assertNull(foundRecord, "Should have nothing new to consume and be null");
        }

        // Close out consumer
        consumer.close();
    }

    /**
     * 1. Setup a consumer to consume from a topic with 2 partitions.
     * 2. Produce several messages into both partitions
     * 3. Consume all of the msgs from the topic.
     * 4. Ack in various orders for the msgs
     * 5. Validate that the state is correct.
     */
    @Test
    public void testConsumeFromTopicWithMultiplePartitionsWithAcking() {
        this.topicName = "testConsumeFromTopicWithMultiplePartitionsWithAcking" + System.currentTimeMillis();
        final int expectedNumberOfPartitions = 2;

        // Create our multi-partition namespace.
        getKafkaTestUtils().createTopic(topicName, expectedNumberOfPartitions, (short) 1);

        // Define our expected namespace/partitions
        final ConsumerPartition partition0 = new ConsumerPartition(topicName, 0);
        final ConsumerPartition partition1 = new ConsumerPartition(topicName, 1);

        // Create our consumer
        final Consumer consumer = getDefaultConsumerInstanceAndOpen(topicName);

        // Ask the underlying consumer for our assigned partitions.
        final Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull(assignedPartitions, "Should be non-null");
        assertFalse(assignedPartitions.isEmpty(), "Should not be empty");
        assertEquals(expectedNumberOfPartitions, assignedPartitions.size(), "Should contain 2 entries");
        assertTrue(assignedPartitions.contains(partition0), "Should contain our expected namespace/partition 0");
        assertTrue(assignedPartitions.contains(partition1), "Should contain our expected namespace/partition 1");

        // Now produce 5 msgs to each namespace (10 msgs total)
        final int expectedNumberOfMsgsPerPartition = 5;
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecordsPartition0 = produceRecords(expectedNumberOfMsgsPerPartition, 0);
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecordsPartition1 = produceRecords(expectedNumberOfMsgsPerPartition, 1);

        // Attempt to consume them
        // Read from namespace, verify we get what we expect
        int partition0Index = 0;
        int partition1Index = 0;
        for (int x = 0; x <  (expectedNumberOfMsgsPerPartition * 2); x++) {
            final Record foundRecord = consumer.nextRecord();
            assertNotNull(foundRecord);

            // Determine which partition its from
            final int partitionSource = foundRecord.getPartition();
            assertTrue(partitionSource == 0 || partitionSource == 1, "Should be partition 0 or 1");

            ProducedKafkaRecord<byte[], byte[]> expectedRecord;
            if (partitionSource == 0) {
                expectedRecord = producedRecordsPartition0.get(partition0Index);
                partition0Index++;
            } else {
                expectedRecord = producedRecordsPartition1.get(partition1Index);
                partition1Index++;
            }

            // Compare to what we expected
            validateRecordMatchesInput(expectedRecord, foundRecord);
        }
        // Next one should return null
        final Record foundRecord = consumer.nextRecord();
        assertNull(foundRecord, "Should have nothing new to consume and be null");

        logger.info("Consumer State {}", consumer.flushConsumerState());

        // Verify state is still -1 for partition 0, meaning not acked offsets yet,
        validateConsumerState(consumer.flushConsumerState(), partition0, -1L);

        // Verify state is still -1 for partition 1, meaning not acked offsets yet,
        validateConsumerState(consumer.flushConsumerState(), partition1, -1L);

        // Ack offset 1 on partition 0, state should be: [partition0: -1, partition1: -1]
        consumer.commitOffset(partition0.namespace(), partition0.partition(), 1L);
        validateConsumerState(consumer.flushConsumerState(), partition0, -1L);
        validateConsumerState(consumer.flushConsumerState(), partition1, -1L);

        // Ack offset 0 on partition 0, state should be: [partition0: 1, partition1: -1]
        consumer.commitOffset(partition0.namespace(), partition0.partition(), 0L);
        validateConsumerState(consumer.flushConsumerState(), partition0, 1L);
        validateConsumerState(consumer.flushConsumerState(), partition1, -1L);

        // Ack offset 2 on partition 0, state should be: [partition0: 2, partition1: -1]
        consumer.commitOffset(partition0.namespace(), partition0.partition(), 2L);
        validateConsumerState(consumer.flushConsumerState(), partition0, 2L);
        validateConsumerState(consumer.flushConsumerState(), partition1, -1L);

        // Ack offset 0 on partition 1, state should be: [partition0: 2, partition1: 0]
        consumer.commitOffset(partition1.namespace(), partition1.partition(), 0L);
        validateConsumerState(consumer.flushConsumerState(), partition0, 2L);
        validateConsumerState(consumer.flushConsumerState(), partition1, 0L);

        // Ack offset 2 on partition 1, state should be: [partition0: 2, partition1: 0]
        consumer.commitOffset(partition1.namespace(), partition1.partition(), 2L);
        validateConsumerState(consumer.flushConsumerState(), partition0, 2L);
        validateConsumerState(consumer.flushConsumerState(), partition1, 0L);

        // Ack offset 0 on partition 1, state should be: [partition0: 2, partition1: 0]
        consumer.commitOffset(partition1.namespace(), partition1.partition(), 0L);
        validateConsumerState(consumer.flushConsumerState(), partition0, 2L);
        validateConsumerState(consumer.flushConsumerState(), partition1, 0L);

        // Ack offset 1 on partition 1, state should be: [partition0: 2, partition1: 2]
        consumer.commitOffset(partition1.namespace(), partition1.partition(), 1L);
        validateConsumerState(consumer.flushConsumerState(), partition0, 2L);
        validateConsumerState(consumer.flushConsumerState(), partition1, 2L);

        // Ack offset 3 on partition 1, state should be: [partition0: 2, partition1: 3]
        consumer.commitOffset(partition1.namespace(), partition1.partition(), 3L);
        validateConsumerState(consumer.flushConsumerState(), partition0, 2L);
        validateConsumerState(consumer.flushConsumerState(), partition1, 3L);

        // Close out consumer
        consumer.close();
    }

    /**
     * This is an integration test of multiple Consumers.
     * We stand up a topic with 4 partitions.
     * We then have a consumer size of 2.
     * We run the test once using consumerIndex 0
     *   - Verify we only consume from partitions 0 and 1
     * We run the test once using consumerIndex 1
     *   - Verify we only consume from partitions 2 and 3
     * @param consumerIndex What consumerIndex to run the test with.
     */
    @ParameterizedTest
    @MethodSource("providerOfConsumerIndexes")
    public void testConsumeWithConsumerGroupEvenNumberOfPartitions(final int consumerIndex) {
        final int numberOfMsgsPerPartition = 10;

        // Create a namespace with 4 partitions
        topicName = "testConsumeWithConsumerGroupEvenNumberOfPartitions" + Clock.systemUTC().millis();
        getKafkaTestUtils().createTopic(topicName, 4, (short) 1);

        // Define some topicPartitions
        final TopicPartition kafkaTopicPartition0 = new TopicPartition(topicName, 0);
        final TopicPartition kafkaTopicPartition1 = new TopicPartition(topicName, 1);
        final TopicPartition kafkaTopicPartition2 = new TopicPartition(topicName, 2);
        final TopicPartition kafkaTopicPartition3 = new TopicPartition(topicName, 3);

        final ConsumerPartition partition0 = new ConsumerPartition(topicName, 0);
        final ConsumerPartition partition1 = new ConsumerPartition(topicName, 1);
        final ConsumerPartition partition2 = new ConsumerPartition(topicName, 2);
        final ConsumerPartition partition3 = new ConsumerPartition(topicName, 3);

        // produce 10 msgs into even partitions, 11 into odd partitions
        produceRecords(numberOfMsgsPerPartition, 0);
        produceRecords(numberOfMsgsPerPartition + 1, 1);
        produceRecords(numberOfMsgsPerPartition, 2);
        produceRecords(numberOfMsgsPerPartition + 1, 3);

        // Some initial setup
        final List<ConsumerPartition> expectedPartitions;
        if (consumerIndex == 0) {
            // If we're consumerIndex 0, we expect partitionIds 0 or 1
            expectedPartitions = Arrays.asList(partition0 , partition1);
        } else if (consumerIndex == 1) {
            // If we're consumerIndex 0, we expect partitionIds 2 or 3
            expectedPartitions = Arrays.asList(partition2 , partition3);
        } else {
            throw new RuntimeException("Invalid input to test");
        }

        // Setup our config
        final Map<String, Object> config = getDefaultConfig(topicName);

        // Adjust the config so that we have 2 consumers, and we are consumer index that was passed in.
        final ConsumerPeerContext consumerPeerContext = new ConsumerPeerContext(2, consumerIndex);

        // create our vspout id
        final VirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Create our Persistence Manager
        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(new HashMap<>());

        // Create our consumer
        final Consumer consumer = new Consumer();
        consumer.open(config, virtualSpoutIdentifier, consumerPeerContext, persistenceAdapter, new LogRecorder(), null);

        // Ask the underlying consumer for our assigned partitions.
        final Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate we are assigned 2 partitions, and its partitionIds 0 and 1
        assertNotNull(assignedPartitions, "Should be non-null");
        assertFalse(assignedPartitions.isEmpty(), "Should not be empty");
        assertEquals(2, assignedPartitions.size(), "Should be assigned 2 partitions");
        assertTrue(assignedPartitions.contains(expectedPartitions.get(0)), "Should contain first expected partition");
        assertTrue(assignedPartitions.contains(expectedPartitions.get(1)), "Should contain 2nd partition");

        // Attempt to consume 21 records
        // Read from topic, verify we get what we expect
        for (int x = 0; x < (numberOfMsgsPerPartition * 2) + 1; x++) {
            final Record foundRecord = consumer.nextRecord();

            // Validate its from partition 0 or 1
            final int foundPartitionId = foundRecord.getPartition();
            assertTrue(
                foundPartitionId == expectedPartitions.get(0).partition() || foundPartitionId == expectedPartitions.get(1).partition(),
                "Should be from one of our expected partitions"
            );

            // Lets ack the tuple as we go
            consumer.commitOffset(foundRecord.getNamespace(), foundRecord.getPartition(), foundRecord.getOffset());
        }

        // Validate next calls all return null, as there is nothing left in those topics on partitions 0 and 1 to consume.
        for (int x = 0; x < 2; x++) {
            final Record foundRecord = consumer.nextRecord();
            assertNull(foundRecord, "Should have nothing new to consume and be null");
        }

        // Now lets flush state
        final ConsumerState consumerState = consumer.flushConsumerState();

        // validate the state only has data for our partitions
        assertNotNull(consumerState, "Should not be null");
        assertEquals(2, consumerState.size(), "Should only have 2 entries");
        assertTrue(consumerState.containsKey(expectedPartitions.get(0)), "Should contain for first expected partition");
        assertTrue(consumerState.containsKey(expectedPartitions.get(1)), "Should contain for 2nd expected partition");
        assertEquals(
            (Long) 9L, consumerState.getOffsetForNamespaceAndPartition(expectedPartitions.get(0)),
            "Offset stored should be 9 on first expected partition"
        );
        assertEquals(
            (Long) 10L, consumerState.getOffsetForNamespaceAndPartition(expectedPartitions.get(1)),
            "Offset stored should be 10 on 2nd expected partition"
        );

        // And double check w/ persistence manager directly
        final Long partition0Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 0);
        final Long partition1Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 1);
        final Long partition2Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 2);
        final Long partition3Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 3);

        if (consumerIndex == 0) {
            assertNotNull(partition0Offset, "Partition0 offset should be not null");
            assertNotNull(partition1Offset, "Partition1 offset should be not null");
            assertNull(partition2Offset, "Partition2 offset should be null");
            assertNull(partition3Offset, "Partition3 offset should be null");

            assertEquals((Long) 9L, partition0Offset, "Offset should be 9");
            assertEquals((Long) 10L, partition1Offset, "Offset should be 10");
        } else {
            assertNotNull(partition2Offset, "Partition2 offset should be not null");
            assertNotNull(partition3Offset, "Partition3 offset should be not null");
            assertNull(partition0Offset, "Partition0 offset should be null");
            assertNull(partition1Offset, "Partition1 offset should be null");

            assertEquals((Long) 9L, partition2Offset, "Offset should be 9");
            assertEquals((Long) 10L, partition3Offset, "Offset should be 10");
        }

        // Close it.
        consumer.close();
    }

    /**
     * This is an integration test of multiple SidelineConsumers.
     * We stand up a namespace with 5 partitions.
     * We then have a consumer size of 2.
     * We run the test once using consumerIndex 0
     *   - Verify we only consume from partitions 0,1,2
     * We run the test once using consumerIndex 1
     *   - Verify we only consume from partitions 3 and 4
     * @param consumerIndex What consumerIndex to run the test with.
     */
    @ParameterizedTest
    @MethodSource("providerOfConsumerIndexes")
    public void testConsumeWithConsumerGroupOddNumberOfPartitions(final int consumerIndex) {
        final int numberOfMsgsPerPartition = 10;

        // Create a namespace with 4 partitions
        topicName = "testConsumeWithConsumerGroupOddNumberOfPartitions" + Clock.systemUTC().millis();
        getKafkaTestUtils().createTopic(topicName, 5, (short) 1);

        // Define some topicPartitions
        final TopicPartition kafkaTopicPartition0 = new TopicPartition(topicName, 0);
        final TopicPartition kafkaTopicPartition1 = new TopicPartition(topicName, 1);
        final TopicPartition kafkaTopicPartition2 = new TopicPartition(topicName, 2);
        final TopicPartition kafkaTopicPartition3 = new TopicPartition(topicName, 3);
        final TopicPartition kafkaTopicPartition4 = new TopicPartition(topicName, 4);

        final ConsumerPartition partition0 = new ConsumerPartition(topicName, 0);
        final ConsumerPartition partition1 = new ConsumerPartition(topicName, 1);
        final ConsumerPartition partition2 = new ConsumerPartition(topicName, 2);
        final ConsumerPartition partition3 = new ConsumerPartition(topicName, 3);
        final ConsumerPartition partition4 = new ConsumerPartition(topicName, 4);

        // produce 10 msgs into even partitions, 11 into odd partitions
        produceRecords(numberOfMsgsPerPartition, 0);
        produceRecords(numberOfMsgsPerPartition + 1, 1);
        produceRecords(numberOfMsgsPerPartition, 2);
        produceRecords(numberOfMsgsPerPartition + 1, 3);
        produceRecords(numberOfMsgsPerPartition, 4);

        // Some initial setup
        final List<ConsumerPartition> expectedPartitions;
        final int expectedRecordsToConsume;
        if (consumerIndex == 0) {
            // If we're consumerIndex 0, we expect partitionIds 0,1, or 2
            expectedPartitions = Arrays.asList(partition0 , partition1, partition2);

            // We expect to get out 31 records
            expectedRecordsToConsume = 31;
        } else if (consumerIndex == 1) {
            // If we're consumerIndex 0, we expect partitionIds 3 or 4
            expectedPartitions = Arrays.asList(partition3 , partition4);

            // We expect to get out 21 records
            expectedRecordsToConsume = 21;
        } else {
            throw new RuntimeException("Invalid input to test");
        }

        // Setup our config
        final Map<String, Object> config = getDefaultConfig(topicName);

        // Adjust the config so that we have 2 consumers, and we are consumer index that was passed in.
        final ConsumerPeerContext consumerPeerContext = new ConsumerPeerContext(2, consumerIndex);

        // create our vspout id
        final VirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Create our Persistence Manager
        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(new HashMap<>());

        // Create our consumer
        final Consumer consumer = new Consumer();
        consumer.open(config, virtualSpoutIdentifier, consumerPeerContext, persistenceAdapter, new LogRecorder(), null);

        // Ask the underlying consumer for our assigned partitions.
        final Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate we are assigned 2 partitions, and its partitionIds 0 and 1
        assertNotNull(assignedPartitions, "Should be non-null");
        assertFalse(assignedPartitions.isEmpty(), "Should not be empty");
        assertEquals(expectedPartitions.size(), assignedPartitions.size(), "Should be assigned correct number of partitions");
        for (final ConsumerPartition expectedTopicPartition : expectedPartitions) {
            assertTrue(
                assignedPartitions.contains(new ConsumerPartition(expectedTopicPartition.namespace(), expectedTopicPartition.partition())),
                "Should contain expected partition"
            );
        }

        // Attempt to consume records
        // Read from namespace, verify we get what we expect
        for (int x = 0; x < expectedRecordsToConsume; x++) {
            final Record foundRecord = consumer.nextRecord();
            assertNotNull(foundRecord);

            // Validate its from a partition we expect
            final int foundPartitionId = foundRecord.getPartition();
            assertTrue(
                expectedPartitions.contains(new ConsumerPartition(topicName, foundPartitionId)),
                "Should be from one of our expected partitions"
            );

            // Lets ack the tuple as we go
            consumer.commitOffset(foundRecord.getNamespace(), foundRecord.getPartition(), foundRecord.getOffset());
        }

        // Validate next calls all return null, as there is nothing left in those topics on partitions 0 and 1 to consume.
        for (int x = 0; x < 2; x++) {
            final Record foundRecord = consumer.nextRecord();
            assertNull(foundRecord, "Should have nothing new to consume and be null");
        }

        // Now lets flush state
        final ConsumerState consumerState = consumer.flushConsumerState();

        // validate the state only has data for our partitions
        assertNotNull(consumerState, "Should not be null");
        assertEquals(expectedPartitions.size(), consumerState.size(), "Should only have correct number of entries");

        for (final ConsumerPartition expectedPartition : expectedPartitions) {
            assertTrue(consumerState.containsKey(expectedPartition), "Should contain for first expected partition");

            if (expectedPartition.partition() % 2 == 0) {
                assertEquals(
                    (Long) 9L, consumerState.getOffsetForNamespaceAndPartition(expectedPartition),
                    "Offset stored should be 9 on even partitions"
                );
            } else {
                assertEquals(
                    (Long) 10L, consumerState.getOffsetForNamespaceAndPartition(expectedPartition),
                    "Offset stored should be 10 on odd partitions"
                );
            }
        }

        // And double check w/ persistence manager directly
        final Long partition0Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 0);
        final Long partition1Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 1);
        final Long partition2Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 2);
        final Long partition3Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 3);
        final Long partition4Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 4);

        if (consumerIndex == 0) {
            assertNotNull(partition0Offset, "Partition0 offset should be not null");
            assertNotNull(partition1Offset, "Partition1 offset should be not null");
            assertNotNull(partition2Offset, "Partition2 offset should be not null");
            assertNull(partition3Offset, "Partition3 offset should be null");
            assertNull(partition4Offset, "Partition4 offset should be null");

            assertEquals((Long) 9L, partition0Offset, "Offset should be 9");
            assertEquals((Long) 10L, partition1Offset, "Offset should be 10");
            assertEquals((Long) 9L, partition2Offset, "Offset should be 9");
        } else {
            assertNotNull(partition3Offset, "Partition3 offset should be not null");
            assertNotNull(partition4Offset, "Partition4 offset should be not null");
            assertNull(partition0Offset, "Partition0 offset should be null");
            assertNull(partition1Offset, "Partition1 offset should be null");
            assertNull(partition2Offset, "Partition2 offset should be null");

            assertEquals((Long) 10L, partition3Offset, "Offset should be 10");
            assertEquals((Long) 9L, partition4Offset, "Offset should be 9");
        }

        // Close it.
        consumer.close();
    }

    /**
     * Provides consumer indexes 0 and 1.
     */
    public static Object[][] providerOfConsumerIndexes() {
        return new Object[][]{
            {0},
            {1}
        };
    }

    /**
     * 1. Setup a consumer to consume from a topic with 1 partition.
     * 2. Produce several messages into that partition.
     * 3. Consume all of the msgs from the topic.
     * 4. Produce more msgs into the topic.
     * 5. Unsubscribe from that partition.
     * 6. Attempt to consume more msgs, verify none are returned, because we told the consumer to unsubscribe.
     */
    @Test
    public void testConsumeFromTopicAfterUnsubscribingFromSinglePartition() {
        // Define our expected namespace/partition
        final ConsumerPartition expectedTopicPartition = new ConsumerPartition(topicName, 0);

        // Create our consumer
        final Consumer consumer = getDefaultConsumerInstanceAndOpen(topicName);

        // Ask the underlying consumer for our assigned partitions.
        final Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull(assignedPartitions, "Should be non-null");
        assertFalse(assignedPartitions.isEmpty(), "Should not be empty");
        assertEquals(1, assignedPartitions.size(), "Should contain 1 entries");
        assertTrue(assignedPartitions.contains(expectedTopicPartition), "Should contain our expected namespace/partition 0");

        // Now produce 5 msgs
        final int expectedNumberOfMsgs = 5;
        List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(expectedNumberOfMsgs, 0);

        // Attempt to consume them
        // Read from namespace, verify we get what we expect
        for (int x = 0; x < expectedNumberOfMsgs; x++) {
            final Record foundRecord = consumer.nextRecord();
            final ProducedKafkaRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);
            validateRecordMatchesInput(expectedRecord, foundRecord);
        }

        // Next one should return null
        Record foundRecord = consumer.nextRecord();
        assertNull(foundRecord, "Should have nothing new to consume and be null");

        // Now produce 5 more msgs
        produceRecords(expectedNumberOfMsgs, 0);

        // Now unsubscribe from the partition
        final boolean result = consumer.unsubscribeConsumerPartition(expectedTopicPartition);
        assertTrue(result, "Should be true");

        // Attempt to consume, but nothing should be returned, because we unsubscribed.
        for (int x = 0; x < expectedNumberOfMsgs; x++) {
            foundRecord = consumer.nextRecord();
            assertNull(foundRecord);
        }

        // Close out consumer
        consumer.close();
    }

    /**
     * 1. Setup a consumer to consume from a namespace with 2 partitions.
     * 2. Produce several messages into both partitions
     * 3. Consume all of the msgs from the namespace.
     * 4. Produce more msgs into both partitions.
     * 5. Unsubscribe from partition 0.
     * 6. Attempt to consume more msgs, verify only those from partition 1 come through.
     */
    @Test
    public void testConsumeFromTopicAfterUnsubscribingFromMultiplePartitions() {
        this.topicName = "testConsumeFromTopicAfterUnsubscribingFromMultiplePartitions" + System.currentTimeMillis();
        final int expectedNumberOfPartitions = 2;

        // Create our multi-partition namespace.
        getKafkaTestUtils().createTopic(topicName, expectedNumberOfPartitions, (short) 1);

        // Define our expected namespace/partitions
        final ConsumerPartition expectedTopicPartition0 = new ConsumerPartition(topicName, 0);
        final ConsumerPartition expectedTopicPartition1 = new ConsumerPartition(topicName, 1);

        // Create our consumer
        final Consumer consumer = getDefaultConsumerInstanceAndOpen(topicName);

        // Ask the underlying consumer for our assigned partitions.
        final Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull(assignedPartitions, "Should be non-null");
        assertFalse(assignedPartitions.isEmpty(), "Should not be empty");
        assertEquals(expectedNumberOfPartitions, assignedPartitions.size(), "Should contain 2 entries");
        assertTrue(assignedPartitions.contains(expectedTopicPartition0), "Should contain our expected namespace/partition 0");
        assertTrue(assignedPartitions.contains(expectedTopicPartition1), "Should contain our expected namespace/partition 1");

        // Now produce 5 msgs to each namespace (10 msgs total)
        final int expectedNumberOfMsgsPerPartition = 5;
        List<ProducedKafkaRecord<byte[], byte[]>> producedRecordsPartition0 = produceRecords(expectedNumberOfMsgsPerPartition, 0);
        List<ProducedKafkaRecord<byte[], byte[]>> producedRecordsPartition1 = produceRecords(expectedNumberOfMsgsPerPartition, 1);

        // Attempt to consume them
        // Read from namespace, verify we get what we expect
        int partition0Index = 0;
        int partition1Index = 0;
        for (int x = 0; x < (expectedNumberOfMsgsPerPartition * 2); x++) {
            final Record foundRecord = consumer.nextRecord();
            assertNotNull(foundRecord);

            // Determine which partition its from
            final int partitionSource = foundRecord.getPartition();
            assertTrue(partitionSource == 0 || partitionSource == 1, "Should be partition 0 or 1");

            ProducedKafkaRecord<byte[], byte[]> expectedRecord;
            if (partitionSource == 0) {
                expectedRecord = producedRecordsPartition0.get(partition0Index);
                partition0Index++;
            } else {
                expectedRecord = producedRecordsPartition1.get(partition1Index);
                partition1Index++;
            }

            // Compare to what we expected
            validateRecordMatchesInput(expectedRecord, foundRecord);
        }

        // Next one should return null
        Record foundRecord = consumer.nextRecord();
        assertNull(foundRecord, "Should have nothing new to consume and be null");

        // Now produce 5 more msgs into each partition
        producedRecordsPartition0 = produceRecords(expectedNumberOfMsgsPerPartition, 0);
        producedRecordsPartition1 = produceRecords(expectedNumberOfMsgsPerPartition, 1);

        // Now unsub from the partition 0
        final boolean result = consumer.unsubscribeConsumerPartition(expectedTopicPartition0);
        assertTrue(result, "Should be true");

        // Attempt to consume, but nothing should be returned from partition 0, only partition 1
        for (int x = 0; x < expectedNumberOfMsgsPerPartition; x++) {
            foundRecord = consumer.nextRecord();
            assertNotNull(foundRecord);

            // Determine which partition its from, should be only 1
            assertEquals(1, foundRecord.getPartition(), "Should be partition 1");

            // Validate it
            final ProducedKafkaRecord<byte[], byte[]> expectedRecord = producedRecordsPartition1.get(x);

            // Compare to what we expected
            validateRecordMatchesInput(expectedRecord, foundRecord);
        }

        // Next one should return null
        foundRecord = consumer.nextRecord();
        assertNull(foundRecord, "Should have nothing new to consume and be null");

        // Close out consumer
        consumer.close();
    }

    /**
     * This tests what happens if we ask to consume from an offset that is invalid (does not exist).
     * Here's what we setup:
     *
     * 2 partitions, produce 4 messages into each.
     *
     * Start a consumer, asking to start at:
     *   offset 2 for partition 1, (recorded completed offset = 1)
     *   offset 21 for partition 2. (recorded completed offset = 20)
     *
     * Offset 20 does not exist for partition 2, this will raise an exception which
     * by the underlying kafka consumer.  This exception should be handled internally
     * resetting the offset on partition 2 to the earliest available (which happens to be 0).
     *
     * We then consume and expect to receive messages:
     *   partition 0: messages 2,3      (because we started at offset 2)
     *   partition 1: messages 0,1,2,3  (because we got reset to earliest)
     *
     * This test also validates that for non-reset partitions, that it does not lose
     * any messages.
     */
    @Test
    public void testWhatHappensIfOffsetIsInvalidShouldResetSmallest() {
        // Kafka namespace setup
        this.topicName = "testWhatHappensIfOffsetIsInvalidShouldResetSmallest" + System.currentTimeMillis();
        final int numberOfPartitions = 2;
        final int numberOfMsgsPerPartition = 4;

        // How many msgs we should expect, 2 for partition 0, 4 from partition1
        final int numberOfExpectedMessages = 2;

        // Define our namespace/partitions
        final ConsumerPartition topicPartition0 = new ConsumerPartition(topicName, 0);
        final ConsumerPartition topicPartition1 = new ConsumerPartition(topicName, 1);

        // Define starting offsets for partitions
        final long partition0StartingOffset = 1L;
        final long partition1StartingOffset = 20L;

        // Create our multi-partition namespace.
        getKafkaTestUtils().createTopic(topicName, numberOfPartitions, (short) 1);

        // Produce messages into both topics
        produceRecords(numberOfMsgsPerPartition, 0);
        produceRecords(numberOfMsgsPerPartition, 1);

        // Setup our config set to reset to none
        // We should handle this internally now.
        final Map<String, Object> config = getDefaultConfig(topicName);

        // Create our Persistence Manager
        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(new HashMap<>());

        // Create & persist the starting state for our test
        // Partition 0 has starting offset = 1
        persistenceAdapter.persistConsumerState("MyConsumerId", 0, partition0StartingOffset);

        // Partition 1 has starting offset = 21, which is invalid.  If our consumer is configured properly,
        // it should reset to earliest, meaning offset 0 in this case.
        persistenceAdapter.persistConsumerState("MyConsumerId", 1, partition1StartingOffset);

        // Create our consumer
        final Consumer consumer = new Consumer();
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), persistenceAdapter, new LogRecorder(), null);

        // Validate PartitionOffsetManager is correctly setup
        ConsumerState consumerState = consumer.getCurrentState();
        assertEquals(
            (Long) partition0StartingOffset, consumerState.getOffsetForNamespaceAndPartition(topicPartition0),
            "Partition 0's last committed offset should be its starting offset"
        );
        assertEquals(
            (Long) partition1StartingOffset, consumerState.getOffsetForNamespaceAndPartition(topicPartition1),
            "Partition 1's last committed offset should be its starting offset"
        );

        // Define the values we expect to get
        final Set<String> expectedValues = Stream.of(
            // Partition 0 should not skip any messages,
            "partition0-offset2", "partition0-offset3"
            // Nothing from partition 1 since we will reset from tail
        ).collect(Collectors.toSet());

        final List<Record> records = asyncConsumeMessages(consumer, 2);
        for (final Record record : records) {
            expectedValues.remove("partition" + record.getPartition() + "-offset" + record.getOffset());
        }

        // Now do validation
        logger.info("Found {} msgs", records.size());
        assertEquals(numberOfExpectedMessages, records.size(), "Should have " + numberOfExpectedMessages + " messages from kafka");
        assertTrue(expectedValues.isEmpty(), "Expected set should now be empty, we found everything " + expectedValues);

        // Call nextRecord 2 more times, both should be null
        for (int x = 0; x < 2; x++) {
            assertNull(consumer.nextRecord(), "Should be null");
        }

        // Validate PartitionOffsetManager is correctly setup
        // We have not acked anything,
        consumerState = consumer.getCurrentState();
        assertEquals(
            (Long) partition0StartingOffset, consumerState.getOffsetForNamespaceAndPartition(topicPartition0),
            "Partition 0's last committed offset should still be its starting offset"
        );

        // This is -1 because the original offset we asked for was invalid, so it got set to (earliest offset - 1), or for us (0 - 1) => -1
        assertEquals(
            Long.valueOf(4L),
            consumerState.getOffsetForNamespaceAndPartition(topicPartition1),
            "Partition 1's last committed offset should be reset to latest, or 4 in our case"
        );

        // Close consumer
        consumer.close();
    }

    /**
     * This tests what happens if we ask to consume from an offset that is invalid (does not exist).
     * Here's what we setup:
     *
     * 2 partitions, produce 4 messages into each.
     *
     * Start a consumer, asking to start at:
     *   offset 0 for partition 0, (recorded completed offset = -1)
     *   offset 0 for partition 1. (recorded completed offset = -1)
     *
     * We then consume and expect to receive messages:
     *   partition 0: messages 0,1,2,3  (because we started at offset 0)
     *   partition 1: messages 0,1,2,3  (because we started at offset 0)
     *
     * Then we should produce 4 more messages into each topic.
     * Before we consume, we should set partition 1's position to offset 21, which is invalid.
     *
     * Then we attempt to consume, partition1's offset will be deemed invalid, and should roll back
     * to the earliest (expected) and partition0's offset should restart from offset 3.
     */
    @Test
    public void testWhatHappensIfOffsetIsInvalidShouldResetSmallestExtendedTest() {
        // Kafka namespace setup
        this.topicName = "testWhatHappensIfOffsetIsInvalidShouldResetSmallest" + System.currentTimeMillis();
        final int numberOfPartitions = 2;
        final int numberOfMsgsPerPartition = 4;

        // How many msgs we should expect, 4 for partition 0, 4 from partition1
        final int numberOfExpectedMessages = 8;

        // Define our namespace/partitions
        final ConsumerPartition topicPartition0 = new ConsumerPartition(topicName, 0);
        final ConsumerPartition topicPartition1 = new ConsumerPartition(topicName, 1);

        // Define starting offsets for partitions
        final long partition0StartingOffset = -1L;
        final long partition1StartingOffset = -1L;

        // Create our multi-partition namespace.
        getKafkaTestUtils().createTopic(topicName, numberOfPartitions, (short) 1);

        // Produce messages into both topics
        produceRecords(numberOfMsgsPerPartition, 0);
        produceRecords(numberOfMsgsPerPartition, 1);

        // Setup our config set to reset to none
        // We should handle this internally now.
        final Map<String, Object> config = getDefaultConfig(topicName);

        // Create our Persistence Manager
        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(new HashMap<>());

        // Create & persist the starting state for our test
        // Partition 0 has starting offset = -1
        persistenceAdapter.persistConsumerState("MyConsumerId", 0, partition0StartingOffset);

        // Partition 1 has starting offset = -1
        persistenceAdapter.persistConsumerState("MyConsumerId", 1, partition1StartingOffset);

        // Create our consumer
        final Consumer consumer = new Consumer();
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), persistenceAdapter, new LogRecorder(), null);

        // Validate PartitionOffsetManager is correctly setup
        ConsumerState consumerState = consumer.getCurrentState();
        assertEquals(
            (Long) partition0StartingOffset,
            consumerState.getOffsetForNamespaceAndPartition(topicPartition0),
            "Partition 0's last committed offset should be its starting offset"
        );
        assertEquals(
            (Long) partition1StartingOffset,
            consumerState.getOffsetForNamespaceAndPartition(topicPartition1),
            "Partition 1's last committed offset should be its starting offset"
        );

        // Define the values we expect to get
        final Set<String> expectedValues = Stream.of(
            // Partition 0 should not skip any messages!
            "partition0-offset0", "partition0-offset1", "partition0-offset2", "partition0-offset3",

            // Partition 1 should not skip any messages!
            "partition1-offset0", "partition1-offset1", "partition1-offset2", "partition1-offset3"
        ).collect(Collectors.toSet());

        List<Record> records = new ArrayList<>();
        Record consumerRecord;
        int attempts = 0;
        do {
            consumerRecord = consumer.nextRecord();
            if (consumerRecord != null) {
                logger.info("Found offset {} on {}", consumerRecord.getOffset(), consumerRecord.getPartition());
                records.add(consumerRecord);

                // Remove from our expected set
                expectedValues.remove("partition" + consumerRecord.getPartition() + "-offset" + consumerRecord.getOffset());
            } else {
                attempts++;
            }
        } while (attempts <= 2);

        // Now do validation
        logger.info("Found {} msgs", records.size());
        assertEquals(numberOfExpectedMessages, records.size(), "Should have 8 messages from kafka");
        assertTrue(expectedValues.isEmpty(), "Expected set should now be empty, we found everything");

        // Now produce more msgs to partition 0 and 1
        produceRecords(numberOfMsgsPerPartition, 1);
        produceRecords(numberOfMsgsPerPartition, 0);

        // Re-define what we expect to see.
        expectedValues.clear();
        expectedValues.addAll(Collections.unmodifiableSet(Stream.of(
            // The next 4 entries from partition 0
            "partition0-offset4", "partition0-offset5", "partition0-offset6", "partition0-offset7"
            // Nothing from partition 1 because we reset it to the tail
        ).collect(Collectors.toSet())));

        // Now set partition 1's offset to something invalid
        consumer.getKafkaConsumer().seek(new TopicPartition(topicName, 1), 1000L);

        // Reset loop vars
        records = new ArrayList<>();
        attempts = 0;
        do {
            // Get the next record
            consumerRecord = consumer.nextRecord();
            if (consumerRecord != null) {
                logger.info("Found offset {} on {}", consumerRecord.getOffset(), consumerRecord.getPartition());
                records.add(consumerRecord);

                // Remove from our expected set
                final boolean didRemove = expectedValues.remove(
                    "partition" + consumerRecord.getPartition() + "-offset" + consumerRecord.getOffset()
                );

                // If we didn't remove something
                if (!didRemove) {
                    // That means something funky is afoot.
                    logger.info("Found unexpected message from consumer: {}", consumerRecord);
                }
            } else {
                attempts++;
            }
        } while (attempts <= 2);

        // Now do validation
        logger.info("Found {} msgs", records.size());
        assertEquals(
            (1 * numberOfMsgsPerPartition),
            records.size(),
            "Should have 4 (4 from partition0, 0 from partition1) messages from kafka"
        );
        assertTrue(expectedValues.isEmpty(), "Expected set should now be empty, we found everything " + expectedValues);

        // Call nextRecord 2 more times, both should be null
        for (int x = 0; x < 2; x++) {
            assertNull(consumer.nextRecord(), "Should be null");
        }

        // Validate PartitionOffsetManager is correctly setup
        // We have not acked anything,
        consumerState = consumer.getCurrentState();
        assertEquals(
            (Long) partition0StartingOffset,
                consumerState.getOffsetForNamespaceAndPartition(topicPartition0),
                "Partition 0's last committed offset should still be its starting offset"

        );

        // This is 8 because the original offset we asked for was invalid, so it got set to the latest offset, or for us 8
        assertEquals(
            Long.valueOf(8L),
            consumerState.getOffsetForNamespaceAndPartition(topicPartition1),
            "Partition 1's last committed offset should be reset to latest, or 8 in our case"
        );

        // Close consumer
        consumer.close();
    }

    /**
     * TLDR; Tests what happens if offsets are invalid, and topic has partitions which are empty.
     *
     * 1 - Create a topic with 2 partitions
     *     Partition 0: has no data / empty
     *     Partition 1: has data
     *
     * 2 - Set state for all 3 partitions
     *     Partition 0: have no stored offset
     *     Partition 1: set offset = 1000 (higher than how much data we have)
     *
     * 3 - Start consumer, see what happens.
     *     Partition 0: should start from head
     *     Partition 1: Should detect invalid offset, reset back to 0 and re-consume.
     */
    @Test
    public void testResetOffsetsWhenHasEmptyPartition() {
        // Kafka namespace setup
        this.topicName = "testResetOffsetsWhenHasEmptyPartition" + System.currentTimeMillis();
        final int numberOfPartitions = 2;
        final int numberOfMsgsOnPartition0 = 0;
        final int numberOfMsgsOnPartition1 = 4;

        // How many msgs we should expect, 0 for partition 0, 0 from partition1
        // We'll skip over partition 1 because we're going to reset to tail
        final int numberOfExpectedMessages = numberOfMsgsOnPartition0;

        // Define our namespace/partitions
        final ConsumerPartition partition0 = new ConsumerPartition(topicName, 0);
        final ConsumerPartition partition1 = new ConsumerPartition(topicName, 1);

        // Define starting offsets for partitions
        final long partition0StartingOffset = 0L;
        final long partition1StartingOffset = 100L;

        // Create our multi-partition namespace.
        getKafkaTestUtils().createTopic(topicName, numberOfPartitions, (short) 1);

        // Produce messages into partition1
        produceRecords(numberOfMsgsOnPartition1, partition1.partition());

        // Setup our config set to reset to none
        // We should handle this internally now.
        final Map<String, Object> config = getDefaultConfig(topicName);

        // Create our Persistence Manager
        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(new HashMap<>());

        // Create & persist the starting state for our test
        // Partition 0 has NO starting state

        // Partition 1 has starting offset = 100L, which is invalid
        persistenceAdapter.persistConsumerState("MyConsumerId", 1, partition1StartingOffset);

        // Create our consumer
        final Consumer consumer = new Consumer();
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), persistenceAdapter, new LogRecorder(), null);

        // Attempt to retrieve records
        final List<Record> records = asyncConsumeMessages(consumer, numberOfExpectedMessages);

        // Validate we got 0 records, no need to do deeper inspection for this test
        assertEquals(0, records.size(), "We should have 0 records");

        // Now validate the state
        ConsumerState consumerState = consumer.flushConsumerState();

        // High level validation
        assertNotNull(consumerState);
        assertFalse(consumerState.isEmpty());

        // Before acking anything
        assertEquals(Long.valueOf(-1L), consumerState.getOffsetForNamespaceAndPartition(topicName, 0), "Has partition 0 offset at -1");
        assertEquals(Long.valueOf(4L), consumerState.getOffsetForNamespaceAndPartition(topicName, 1), "Has partition 1 offset at 4");

        // Ack all of our messages in consumer
        for (final Record record : records) {
            consumer.commitOffset(record.getNamespace(), record.getPartition(), record.getOffset());
        }

        // Commit state
        consumerState = consumer.flushConsumerState();

        // High level validation
        assertNotNull(consumerState);
        assertFalse(consumerState.isEmpty());

        // After acking messages
        assertEquals(Long.valueOf(-1L), consumerState.getOffsetForNamespaceAndPartition(topicName, 0), "Has partition 0 offset at -1");
        assertEquals(Long.valueOf(4L), consumerState.getOffsetForNamespaceAndPartition(topicName, 1), "Has partition 1 offset at 4");

        // Clean up
        consumer.close();
    }

    /**
     * TLDR; Tests what happens if offsets are invalid, and topic has partitions which are empty.
     * Essentially the same test as above, except we store a starting offset of -1 for partition 0
     *
     * 1 - Create a topic with 2 partitions
     *     Partition 0: has no data / empty
     *     Partition 1: has data
     *
     * 2 - Set state for 2 partitions
     *     Partition 0: have stored offset of -1
     *     Partition 1: set offset = 100 (higher than how much data we have)
     *
     * 3 - Start consumer, see what happens.
     *     Partition 0: should start from head
     *     Partition 1: Should detect invalid offset, reset back to 0 and re-consume.
     */
    @Test
    public void testResetOffsetsWhenHasEmptyPartitionAndStoredOffsetOfNegative1() {
        // Kafka namespace setup
        this.topicName = "testResetOffsetsWhenHasEmptyPartition" + System.currentTimeMillis();
        final int numberOfPartitions = 2;
        final int numberOfMsgsOnPartition0 = 2;
        final int numberOfMsgsOnPartition1 = 4;

        // How many msgs we should expect, 2 for partition 0, 0 from partition1 because our offset is past it
        // and presumable we have played those before.
        final int numberOfExpectedMessages = numberOfMsgsOnPartition0;

        // Define our namespace/partitions
        final ConsumerPartition partition0 = new ConsumerPartition(topicName, 0);
        final ConsumerPartition partition1 = new ConsumerPartition(topicName, 1);

        // Define starting offsets for partitions
        final long partition0StartingOffset = -1L;
        final long partition1StartingOffset = 100L;

        // Create our multi-partition namespace.
        getKafkaTestUtils().createTopic(topicName, numberOfPartitions, (short) 1);

        // Produce messages into partition0 and partition1
        produceRecords(numberOfMsgsOnPartition0, 0);
        produceRecords(numberOfMsgsOnPartition1, 1);

        // Setup our config set to reset to none
        // We should handle this internally now.
        final Map<String, Object> config = getDefaultConfig(topicName);

        // Create our Persistence Manager
        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(new HashMap<>());

        // Create & persist the starting state for our test
        // Partition 0 has starting state of -1
        persistenceAdapter.persistConsumerState("MyConsumerId", 0, partition0StartingOffset);

        // Partition 1 has starting offset = 100L, which is invalid
        persistenceAdapter.persistConsumerState("MyConsumerId", 1, partition1StartingOffset);

        // Create our consumer
        final Consumer consumer = new Consumer();
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), persistenceAdapter, new LogRecorder(), null);

        // Attempt to retrieve records
        final List<Record> records = asyncConsumeMessages(consumer, numberOfExpectedMessages);

        // Validate we got 2 records, both should be from partition 0
        assertEquals(numberOfExpectedMessages, records.size(), "We should have 2 records");

        // Validate only partition 0
        for (final Record record : records) {
            assertEquals(0, record.getPartition(), "Should have come from partition0 only");
        }

        // Now validate the state
        ConsumerState consumerState = consumer.flushConsumerState();

        // High level validation
        assertNotNull(consumerState);
        assertFalse(consumerState.isEmpty());

        // Before acking anything
        assertEquals(Long.valueOf(-1L), consumerState.getOffsetForNamespaceAndPartition(topicName, 0), "Has partition 0 offset at -1");
        assertEquals(
            Long.valueOf(numberOfMsgsOnPartition1),
            consumerState.getOffsetForNamespaceAndPartition(topicName, 1),
            "Has partition 1 offset at " + numberOfMsgsOnPartition1
        );

        // Ack all of our messages in consumer
        for (final Record record : records) {
            consumer.commitOffset(record.getNamespace(), record.getPartition(), record.getOffset());
        }

        // Commit state
        consumerState = consumer.flushConsumerState();

        // High level validation
        assertNotNull(consumerState);
        assertFalse(consumerState.isEmpty());

        // After acking messages
        assertEquals(Long.valueOf(1L), consumerState.getOffsetForNamespaceAndPartition(topicName, 0), "Has partition 0 offset at 1");
        // We haven't moved, we reset to the tail so nothing should have changed since we've not published new messages
        assertEquals(
            Long.valueOf(numberOfMsgsOnPartition1),
            consumerState.getOffsetForNamespaceAndPartition(topicName, 1),
            "Has partition 1 offset at " + numberOfMsgsOnPartition1
        );

        // Clean up
        consumer.close();
    }

    /**
     * This is a test for a weird edge case we hit in production where the consumer seeks past where we are supposed to
     * be, and so we move the pointer back to where we think is valid, rather than resetting to the head of the log.
     */
    @Test
    public void testResetOffsetsWhenOffByOne() {
        // Kafka namespace setup
        this.topicName = "testResetOffsetsWhenOffByOne" + System.currentTimeMillis();

        final int numberOfPartitions = 2;
        final int numberOfMsgsOnPartition0 = 0;
        final int numberOfMsgsOnPartition1 = 4;

        // Define our namespace/partitions
        final ConsumerPartition partition0 = new ConsumerPartition(topicName, 0);
        final TopicPartition topicPartition0 = new TopicPartition(partition0.namespace(), partition0.partition());
        final ConsumerPartition partition1 = new ConsumerPartition(topicName, 1);
        final TopicPartition topicPartition1 = new TopicPartition(partition1.namespace(), partition1.partition());

        // Create our multi-partition namespace.
        getKafkaTestUtils().createTopic(topicName, numberOfPartitions, (short) 1);

        // Produce messages into partition1
        produceRecords(numberOfMsgsOnPartition1, partition1.partition());

        // Setup our config set to reset to none
        // We should handle this internally now.
        final Map<String, Object> config = getDefaultConfig(topicName);

        // Create our Persistence Manager
        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(new HashMap<>());

        // Move our persisted state to the end of the log, this is where the consumer will begin consuming from
        persistenceAdapter.persistConsumerState("MyConsumerId", 1, numberOfMsgsOnPartition1);

        // Create our consumer
        final Consumer consumer = new Consumer();
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), persistenceAdapter, new LogRecorder(), null);

        // We are at the end of the log, so this should yield NULL every time, there's nothing after our offset
        final Record record1 = consumer.nextRecord();

        assertNull(record1, "Consumer should not find a record");

        assertEquals(
            numberOfMsgsOnPartition1,
            consumer.getKafkaConsumer().position(topicPartition1),
            "Kafka's position should not match the total number of messages on the partition since we are at the end of it"
        );

        // Seek the consumer past the end of the log, this should create an OutOfRangeException
        consumer.getKafkaConsumer().seek(
            topicPartition1,
            numberOfMsgsOnPartition1 + 1
        );

        assertEquals(
            numberOfMsgsOnPartition1 + 1,
            consumer.getKafkaConsumer().position(topicPartition1),
            "Seek call on Kafka should be past the end of our messages"
        );

        // Now attempt to consume a message, the pointer for kafka is past the end of the log so this is going to
        // generate an exception which we will catch, and if everything is working correctly we will reset it to the last
        // valid offset that we processed
        consumer.nextRecord();

        assertEquals(
            numberOfMsgsOnPartition1,
            consumer.getKafkaConsumer().position(topicPartition1),
            "Seek call on Kafka should have been reset to our last message"
        );

        // Clean up
        consumer.close();
    }


    /**
     * Calling nextRecord calls fillBuffer() which has special handling for OutOfRangeExceptions.  There is some
     * recursion in this method, but it should not go on forever and yield a StackOverflow exception.  This test verifies
     * that when the KafkaConsumer is relentlessly throwing OutOfRangeExceptions that we stop trying to fill the buffer
     * after five attempts and do not have any other errors.
     */
    @Test
    public void testNextRecordWithRecursiveOutOfRangeException() {
        // Create mock KafkaConsumer instance
        @SuppressWarnings("unchecked")
        final KafkaConsumer<byte[], byte[]> mockKafkaConsumer = (KafkaConsumer<byte[], byte[]>) mock(KafkaConsumer.class);

        Mockito.when(mockKafkaConsumer.assignment()).thenReturn(
            Stream.of(new TopicPartition("Foobar", 0)).collect(Collectors.toSet())
        );
        Mockito.when(mockKafkaConsumer.partitionsFor(topicName)).thenReturn(Arrays.asList(
            new PartitionInfo(topicName, 0, null, null, null)
        ));
        Mockito.when(mockKafkaConsumer.poll(300)).thenThrow(
            new OffsetOutOfRangeException(new HashMap<>())
        );

        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(new HashMap<>());

        // Create our consumer
        final Consumer consumer = new Consumer(mockKafkaConsumer);
        consumer.open(
            getDefaultConfig(topicName),
            getDefaultVSpoutId(),
            getDefaultConsumerCohortDefinition(),
            persistenceAdapter,
            new LogRecorder(),
            null
        );

        final Record record = consumer.nextRecord();

        assertNull(record);

        Mockito.verify(mockKafkaConsumer, Mockito.times(5)).poll(300);

        consumer.close();
    }

    /**
     * Helper method.
     * @param consumerState the consumer state we want to validate
     * @param topicPartition the namespace/partition we want to validate
     * @param expectedOffset the offset we expect
     */
    private void validateConsumerState(
        final ConsumerState consumerState,
        final ConsumerPartition topicPartition,
        final long expectedOffset
    ) {
        final long actualOffset = consumerState.getOffsetForNamespaceAndPartition(topicPartition);
        assertEquals(expectedOffset, actualOffset, "Expected offset");
    }

    /**
     * helper method to produce records into kafka.
     */
    private List<ProducedKafkaRecord<byte[], byte[]>> produceRecords(final int numberOfRecords, final int partitionId) {
        return getKafkaTestUtils().produceRecords(numberOfRecords, topicName, partitionId);
    }

    /**
     * Utility to validate the records consumed from kafka match what was produced into kafka.
     * @param expectedRecord Records produced to kafka
     * @param foundRecord Records read from kafka
     */
    private void validateRecordMatchesInput(final ProducedKafkaRecord<byte[], byte[]> expectedRecord, final Record foundRecord) {
        assertNotNull(foundRecord);

        // Get values from the generated Tuple
        final String key = (String) foundRecord.getValues().get(0);
        final String value = (String) foundRecord.getValues().get(1);

        assertEquals(new String(expectedRecord.getKey(), StandardCharsets.UTF_8), key, "Found expected key");
        assertEquals(new String(expectedRecord.getValue(), StandardCharsets.UTF_8), value, "Found expected value");
    }

    /**
     * Utility method to generate a standard config map.
     */
    private Map<String, Object> getDefaultConfig() {
        return getDefaultConfig(topicName);
    }

    /**
     * Utility method to generate a standard config map.
     */
    private Map<String, Object> getDefaultConfig(final String topicName) {
        final Map<String, Object> defaultConfig = new HashMap<>();
        // Kafka Consumer config items
        defaultConfig.put(
            KafkaConsumerConfig.KAFKA_BROKERS,
            Collections.singletonList(sharedKafkaTestResource.getKafkaConnectString())
        );
        defaultConfig.put(KafkaConsumerConfig.KAFKA_TOPIC, topicName);
        defaultConfig.put(KafkaConsumerConfig.CONSUMER_ID_PREFIX, "TestPrefix");
        defaultConfig.put(KafkaConsumerConfig.DESERIALIZER_CLASS, Utf8StringDeserializer.class.getName());

        // Dynamic Spout config items
        defaultConfig.put(SpoutConfig.PERSISTENCE_ZK_ROOT, "/dynamic-spout-test");
        defaultConfig.put(SpoutConfig.PERSISTENCE_ZK_SERVERS, Collections.singletonList(
            sharedKafkaTestResource.getZookeeperConnectString()
        ));
        defaultConfig.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, ZookeeperPersistenceAdapter.class.getName());

        return SpoutConfig.setDefaults(defaultConfig);
    }

    /**
     * Utility method to generate a default ConsumerPeerContext instance.
     */
    private ConsumerPeerContext getDefaultConsumerCohortDefinition() {
        return new ConsumerPeerContext(1, 0);
    }

    /**
     * Utility method to generate a default VirtualSpoutIdentifier instance.
     */
    private VirtualSpoutIdentifier getDefaultVSpoutId() {
        return new DefaultVirtualSpoutIdentifier("MyConsumerId");
    }

    /**
     * Utility method to generate a defaultly configured and opened Consumer instance.
     */
    private Consumer getDefaultConsumerInstanceAndOpen(final String topicName) {
        // Create our Persistence Manager
        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(new HashMap<>());

        // Create our consumer
        final Consumer consumer = new Consumer();
        consumer.open(
            getDefaultConfig(topicName),
            getDefaultVSpoutId(),
            getDefaultConsumerCohortDefinition(),
            persistenceAdapter,
            new LogRecorder(),
            null
        );

        return consumer;
    }

    /**
     * Utility method to generate a defaultly configured and opened Consumer instance.
     */
    private Consumer getDefaultConsumerInstanceAndOpen() {
        return getDefaultConsumerInstanceAndOpen(topicName);
    }

    private List<Record> asyncConsumeMessages(final Consumer consumer, final int numberOfMessagesToConsume) {
        final List<Record> consumedMessages = new ArrayList<>();

        await()
            .atMost(5, TimeUnit.SECONDS)
            .pollInterval(100, TimeUnit.MILLISECONDS)
            .until(() -> {
                final Record nextRecord = consumer.nextRecord();
                if (nextRecord != null) {
                    consumedMessages.add(nextRecord);
                }
                return consumedMessages.size();
            }, equalTo(numberOfMessagesToConsume));

        // Santity Test - Now call consume again, we shouldn't get any messages
        final Record nextRecord = consumer.nextRecord();
        assertNull(nextRecord, "Should have no more records");

        return consumedMessages;
    }

    /**
     * Simple accessor.
     */
    private KafkaTestUtils getKafkaTestUtils() {
        return sharedKafkaTestResource.getKafkaTestUtils();
    }
}