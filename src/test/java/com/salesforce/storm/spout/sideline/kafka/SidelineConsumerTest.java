package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import com.salesforce.storm.spout.sideline.persistence.InMemoryPersistenceManager;
import com.salesforce.storm.spout.sideline.persistence.PersistenceManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.shade.com.google.common.base.Charsets;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Validates that our SidelineConsumer works as we expect under various scenarios.
 */
public class SidelineConsumerTest {

    private static final Logger logger = LoggerFactory.getLogger(SidelineConsumerTest.class);
    private KafkaTestServer kafkaTestServer;
    private String topicName;

    /**
     * Here we stand up an internal test kafka and zookeeper service.
     */
    @Before
    public void setup() throws Exception {
        // ensure we're in a clean state
        tearDown();

        // Setup kafka test server
        kafkaTestServer = new KafkaTestServer();
        kafkaTestServer.start();

        // Generate topic name
        topicName = SidelineConsumerTest.class.getSimpleName() + DateTime.now().getMillis();

        // Create topic
        kafkaTestServer.createTopic(topicName);
    }

    /**
     * Here we shut down the internal test kafka and zookeeper services.
     */
    @After
    public void tearDown() {
        // Close out kafka test server if needed
        if (kafkaTestServer == null) {
            return;
        }
        try {
            kafkaTestServer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        kafkaTestServer = null;
    }

    /**
     * Tests the constructor saves off instances of things passed into it properly.
     */
    @Test
    public void testConstructor() {
        // Setup our config
        List<String> brokerHosts = Lists.newArrayList(kafkaTestServer.getKafkaServer().serverConfig().advertisedHostName() + ":" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort());
        final SidelineConsumerConfig config = new SidelineConsumerConfig(brokerHosts, "MyConsumerId", topicName);
        config.setKafkaConsumerProperty("auto.offset.reset", "earliest");

        // Create instance of a StateConsumer, we'll just use a dummy instance.
        PersistenceManager persistenceManager = new InMemoryPersistenceManager();
        persistenceManager.open(Maps.newHashMap());

        // Call constructor
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, persistenceManager);

        // Validate our instances got set
        assertNotNull("Config is not null", sidelineConsumer.getConsumerConfig());
        assertEquals(config, sidelineConsumer.getConsumerConfig());
        assertNotNull("PersistenceManager is not null", sidelineConsumer.getPersistenceManager());
        assertEquals(persistenceManager, sidelineConsumer.getPersistenceManager());
    }

    // TODO: these test cases
    // test calling open twice throws exception.
    // test calling open w/ a starting state.
    // test calling getCurrentState().

    /**
     * Tests that our logic for flushing consumer state to the persistence layer works
     * as we expect it to.
     */
    @Test
    public void testTimedFlushConsumerState() throws InterruptedException {
        final String expectedConsumerId = "MyConsumerId";

        // Setup our config
        List<String> brokerHosts = Lists.newArrayList(kafkaTestServer.getKafkaServer().serverConfig().advertisedHostName() + ":" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort());
        final SidelineConsumerConfig config = new SidelineConsumerConfig(brokerHosts, expectedConsumerId, topicName);

        // Set timeout to 1 seconds.
        config.setFlushStateTimeMS(1000);

        // Create mock persistence manager so we can determine if it was called
        PersistenceManager mockPersistenceManager = mock(PersistenceManager.class);

        // Create a mock clock so we can control time (bwahaha)
        Instant instant = Clock.systemUTC().instant();
        Clock mockClock = Clock.fixed(instant, ZoneId.systemDefault());

        // Call constructor
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, mockPersistenceManager);
        sidelineConsumer.setClock(mockClock);

        // Call our method once
        sidelineConsumer.timedFlushConsumerState();

        // Make sure persistence layer was not hit
        verify(mockPersistenceManager, never()).persistConsumerState(anyString(), anyObject());

        // Sleep for 1.5 seconds
        Thread.sleep(1500);

        // Call our method again
        sidelineConsumer.timedFlushConsumerState();

        // Make sure persistence layer was not hit because we're using a mocked clock that has not changed :p
        verify(mockPersistenceManager, never()).persistConsumerState(anyString(), anyObject());

        // Now lets adjust our mock clock up by 2 seconds.
        instant = instant.plus(2000, ChronoUnit.MILLIS);
        mockClock = Clock.fixed(instant, ZoneId.systemDefault());
        sidelineConsumer.setClock(mockClock);

        // Call our method again, it should have triggered this time.
        sidelineConsumer.timedFlushConsumerState();

        // Make sure persistence layer WAS hit because we adjust our mock clock ahead 2 secs
        verify(mockPersistenceManager, times(1)).persistConsumerState(eq(expectedConsumerId), anyObject());

        // Call our method again, it shouldn't fire.
        sidelineConsumer.timedFlushConsumerState();

        // Make sure persistence layer was not hit again because we're using a mocked clock that has not changed since the last call :p
        verify(mockPersistenceManager, times(1)).persistConsumerState(anyString(), anyObject());

        // Now lets adjust our mock clock up by 1.5 seconds.
        instant = instant.plus(1500, ChronoUnit.MILLIS);
        mockClock = Clock.fixed(instant, ZoneId.systemDefault());
        sidelineConsumer.setClock(mockClock);

        // Call our method again, it should have triggered this time.
        sidelineConsumer.timedFlushConsumerState();

        // Make sure persistence layer WAS hit a 2nd time because we adjust our mock clock ahead
        verify(mockPersistenceManager, times(2)).persistConsumerState(eq(expectedConsumerId), anyObject());
    }

    /**
     * Verifies that when we call open that it makes the appropriate calls
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

        // Setup our config
        List<String> brokerHosts = Lists.newArrayList(kafkaTestServer.getKafkaServer().serverConfig().advertisedHostName() + ":" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort());
        final SidelineConsumerConfig config = new SidelineConsumerConfig(brokerHosts, consumerId, topicName);
        config.setKafkaConsumerProperty("auto.offset.reset", "earliest");

        // Create mock KafkaConsumer instance
        KafkaConsumer<byte[], byte[]> mockKafkaConsumer = mock(KafkaConsumer.class);

        // When we call partitionsFor(), we should return a single partition number 0 for our topic.
        List<PartitionInfo> mockPartitionInfos = Lists.newArrayList(new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0]));
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // Create instance of a StateConsumer, we'll just use a dummy instance.
        PersistenceManager mockPersistenceManager = mock(PersistenceManager.class);

        // When getState is called, return the following state
        ConsumerState emptyConsumerState = new ConsumerState();
        when(mockPersistenceManager.retrieveConsumerState(eq(consumerId))).thenReturn(emptyConsumerState);

        // Call constructor injecting our mocks
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, mockPersistenceManager, mockKafkaConsumer);

        // Now call open
        sidelineConsumer.open(null);

        // For every partition returned by mockKafkaConsumer.partitionsFor(), we should subscribe to them via the mockKafkaConsumer.assign() call
        verify(mockKafkaConsumer, times(1)).assign(eq(Lists.newArrayList(new TopicPartition(topicName, 0))));

        // Since ConsumerStateManager has no state for partition 0, we should call seekToBeginning on that partition
        verify(mockKafkaConsumer, times(1)).seekToBeginning(eq(Lists.newArrayList(new TopicPartition(topicName, 0))));
    }

    /**
     * Verifies that when we call open that it makes the appropriate calls
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

        // Setup our config
        List<String> brokerHosts = Lists.newArrayList(kafkaTestServer.getKafkaServer().serverConfig().advertisedHostName() + ":" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort());
        final SidelineConsumerConfig config = new SidelineConsumerConfig(brokerHosts, consumerId, topicName);
        config.setKafkaConsumerProperty("auto.offset.reset", "earliest");

        // Create mock KafkaConsumer instance
        KafkaConsumer<byte[], byte[]> mockKafkaConsumer = mock(KafkaConsumer.class);

        // When we call partitionsFor(), We return partitions 0,1, and 2.
        List<PartitionInfo> mockPartitionInfos = Lists.newArrayList(
                new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
                new PartitionInfo(topicName, 1, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
                new PartitionInfo(topicName, 2, new Node(0, "localhost", 9092), new Node[0], new Node[0])
        );
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // Create instance of a PersistenceManager, we'll just use a dummy instance.
        PersistenceManager mockPersistenceManager = mock(PersistenceManager.class);

        // When getState is called, return the following state
        ConsumerState emptyConsumerState = new ConsumerState();
        when(mockPersistenceManager.retrieveConsumerState(eq(consumerId))).thenReturn(emptyConsumerState);

        // Call constructor injecting our mocks
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, mockPersistenceManager, mockKafkaConsumer);

        // Now call open
        sidelineConsumer.open(null);

        // For every partition returned by mockKafkaConsumer.partitionsFor(), we should subscribe to them via the mockKafkaConsumer.assign() call
        verify(mockKafkaConsumer, times(1)).assign(eq(Lists.newArrayList(
                new TopicPartition(topicName, 0),
                new TopicPartition(topicName, 1),
                new TopicPartition(topicName, 2))));

        // Since ConsumerStateManager has no state for partition 0, we should call seekToBeginning on that partition
        verify(mockKafkaConsumer, times(1)).seekToBeginning(eq(Lists.newArrayList(
                new TopicPartition(topicName, 0),
                new TopicPartition(topicName, 1),
                new TopicPartition(topicName, 2))));
    }

    /**
     * Verifies that when we call open that it makes the appropriate calls
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
        final long expectedOffset = 12345L;

        // Setup our config
        List<String> brokerHosts = Lists.newArrayList(kafkaTestServer.getKafkaServer().serverConfig().advertisedHostName() + ":" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort());
        final SidelineConsumerConfig config = new SidelineConsumerConfig(brokerHosts, consumerId, topicName);
        config.setKafkaConsumerProperty("auto.offset.reset", "earliest");

        // Create mock KafkaConsumer instance
        KafkaConsumer<byte[], byte[]> mockKafkaConsumer = mock(KafkaConsumer.class);

        // When we call partitionsFor(), we should return a single partition number 0 for our topic.
        List<PartitionInfo> mockPartitionInfos = Lists.newArrayList(new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0]));
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // Create instance of a StateConsumer, we'll just use a dummy instance.
        PersistenceManager mockPersistenceManager = mock(PersistenceManager.class);

        // When getState is called, return the following state
        ConsumerState consumerState = new ConsumerState();
        consumerState.setOffset(new TopicPartition(topicName, 0), expectedOffset);
        when(mockPersistenceManager.retrieveConsumerState(eq(consumerId))).thenReturn(consumerState);

        // Call constructor injecting our mocks
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, mockPersistenceManager, mockKafkaConsumer);

        // Now call open
        sidelineConsumer.open(null);

        // For every partition returned by mockKafkaConsumer.partitionsFor(), we should subscribe to them via the mockKafkaConsumer.assign() call
        verify(mockKafkaConsumer, times(1)).assign(eq(Lists.newArrayList(new TopicPartition(topicName, 0))));

        // Since ConsumerStateManager has state for partition 0, we should NEVER call seekToBeginning on that partition
        verify(mockKafkaConsumer, never()).seekToBeginning(eq(Lists.newArrayList(new TopicPartition(topicName, 0))));

        // Instead since there is state, we should call seek on that partition
        verify(mockKafkaConsumer, times(1)).seek(eq(new TopicPartition(topicName, 0)), eq(expectedOffset));
    }

    /**
     * Verifies that when we call open that it makes the appropriate calls
     * to ConsumerStateManager to initialize.
     *
     * This test has the ConsumerStateManager (a mock) return ConsumerState for every partition on the topic.
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

        // Define expected offsets per partition
        final long expectedPartition0Offset = 1234L;
        final long expectedPartition1Offset = 4321L;
        final long expectedPartition2Offset = 1337L;

        // Setup our config
        List<String> brokerHosts = Lists.newArrayList(kafkaTestServer.getKafkaServer().serverConfig().advertisedHostName() + ":" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort());
        final SidelineConsumerConfig config = new SidelineConsumerConfig(brokerHosts, consumerId, topicName);
        config.setKafkaConsumerProperty("auto.offset.reset", "earliest");

        // Create mock KafkaConsumer instance
        KafkaConsumer<byte[], byte[]> mockKafkaConsumer = mock(KafkaConsumer.class);

        // When we call partitionsFor(), We return partitions 0,1, and 2.
        List<PartitionInfo> mockPartitionInfos = Lists.newArrayList(
                new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
                new PartitionInfo(topicName, 1, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
                new PartitionInfo(topicName, 2, new Node(0, "localhost", 9092), new Node[0], new Node[0])
        );
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // Create instance of a StateConsumer, we'll just use a dummy instance.
        PersistenceManager mockPersistenceManager = mock(PersistenceManager.class);

        // When getState is called, return the following state
        ConsumerState consumerState = new ConsumerState();
        consumerState.setOffset(partition0, expectedPartition0Offset);
        consumerState.setOffset(partition1, expectedPartition1Offset);
        consumerState.setOffset(partition2, expectedPartition2Offset);
        when(mockPersistenceManager.retrieveConsumerState(eq(consumerId))).thenReturn(consumerState);

        // Call constructor injecting our mocks
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, mockPersistenceManager, mockKafkaConsumer);

        // Now call open
        sidelineConsumer.open(null);

        // For every partition returned by mockKafkaConsumer.partitionsFor(), we should subscribe to them via the mockKafkaConsumer.assign() call
        verify(mockKafkaConsumer, times(1)).assign(eq(Lists.newArrayList(
                partition0,
                partition1,
                partition2)));

        // Since ConsumerStateManager has state for all partitions, we should never call seekToBeginning on any partitions
        verify(mockKafkaConsumer, never()).seekToBeginning(anyList());

        // Instead since there is state, we should call seek on each partition
        InOrder inOrderVerification = inOrder(mockKafkaConsumer);
        inOrderVerification.verify(mockKafkaConsumer, times(1)).seek(eq(partition0), eq(expectedPartition0Offset));
        inOrderVerification.verify(mockKafkaConsumer, times(1)).seek(eq(partition1), eq(expectedPartition1Offset));
        inOrderVerification.verify(mockKafkaConsumer, times(1)).seek(eq(partition2), eq(expectedPartition2Offset));
    }

    /**
     * Verifies that when we call open that it makes the appropriate calls
     * to ConsumerStateManager to initialize.
     *
     * This test has the ConsumerStateManager (a mock) return ConsumerState for every partition on the topic.
     * We verify that our internal kafka client then knows to start reading from the previously saved consumer state
     * offsets
     */
    @Test
    public void testConnectWithMultiplePartitionsOnTopicWithSomePreviouslySavedState() {
        // Define our ConsumerId
        final String consumerId = "MyConsumerId";

        // Define partitionTopics so we can re-use em
        final TopicPartition partition0 = new TopicPartition(topicName, 0);
        final TopicPartition partition1 = new TopicPartition(topicName, 1);
        final TopicPartition partition2 = new TopicPartition(topicName, 2);
        final TopicPartition partition3 = new TopicPartition(topicName, 3);

        // Define expected offsets per partition, partition 1 and 3 has no previously saved state.
        final long expectedPartition0Offset = 1234L;
        final long expectedPartition2Offset = 1337L;

        // Setup our config
        List<String> brokerHosts = Lists.newArrayList(kafkaTestServer.getKafkaServer().serverConfig().advertisedHostName() + ":" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort());
        final SidelineConsumerConfig config = new SidelineConsumerConfig(brokerHosts, consumerId, topicName);
        config.setKafkaConsumerProperty("auto.offset.reset", "earliest");

        // Create mock KafkaConsumer instance
        KafkaConsumer<byte[], byte[]> mockKafkaConsumer = mock(KafkaConsumer.class);

        // When we call partitionsFor(), We return partitions 0,1,2,3
        List<PartitionInfo> mockPartitionInfos = Lists.newArrayList(
                new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
                new PartitionInfo(topicName, 1, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
                new PartitionInfo(topicName, 2, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
                new PartitionInfo(topicName, 3, new Node(0, "localhost", 9092), new Node[0], new Node[0])
        );
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // Create instance of a StateConsumer, we'll just use a dummy instance.
        PersistenceManager mockPersistenceManager = mock(PersistenceManager.class);

        // When getState is called, return the following state for partitions 0 and 2.
        ConsumerState consumerState = new ConsumerState();
        consumerState.setOffset(partition0, expectedPartition0Offset);
        consumerState.setOffset(partition2, expectedPartition2Offset);
        when(mockPersistenceManager.retrieveConsumerState(eq(consumerId))).thenReturn(consumerState);

        // Call constructor injecting our mocks
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, mockPersistenceManager, mockKafkaConsumer);

        // Now call open
        sidelineConsumer.open(null);

        // For every partition returned by mockKafkaConsumer.partitionsFor(), we should subscribe to them via the mockKafkaConsumer.assign() call
        verify(mockKafkaConsumer, times(1)).assign(eq(Lists.newArrayList(
                partition0,
                partition1,
                partition2,
                partition3)));

        // Since ConsumerStateManager has state for only 2 partitions, we should call seekToBeginning on partitions 1 and 3.
        verify(mockKafkaConsumer, times(1)).seekToBeginning(eq(Lists.newArrayList(
                partition1, partition3
        )));

        // For the partitions with state, we should call seek on each partition
        InOrder inOrderVerification = inOrder(mockKafkaConsumer);
        inOrderVerification.verify(mockKafkaConsumer, times(1)).seek(eq(partition0), eq(expectedPartition0Offset));
        inOrderVerification.verify(mockKafkaConsumer, times(1)).seek(eq(partition2), eq(expectedPartition2Offset));
    }

    /**
     * Tests that the getAssignedPartitions() works as we expect.
     * This test uses a topic with a single partition that we are subscribed to.
     */
    @Test
    public void testGetAssignedPartitionsWithSinglePartition() {
        final int expectedPartitionId = 0;

        // Setup our config
        SidelineConsumerConfig config = getDefaultSidelineConsumerConfig();

        // Create our Persistence Manager
        PersistenceManager persistenceManager = new InMemoryPersistenceManager();
        persistenceManager.open(Maps.newHashMap());

        // Create our consumer
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, persistenceManager);
        sidelineConsumer.open(null);

        // Ask the underlying consumer for our assigned partitions.
        Set<TopicPartition> assignedPartitions = sidelineConsumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate it
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain 1 entry", 1, assignedPartitions.size());
        assertTrue("Should contain our expected topic/partition", assignedPartitions.contains(new TopicPartition(topicName, expectedPartitionId)));
    }

    /**
     * Tests that the getAssignedPartitions() works as we expect.
     * This test uses a topic with a single partition that we are subscribed to.
     */
    @Test
    public void testGetAssignedPartitionsWithMultiplePartitions() {
        // Define and create our topic
        final String expectedTopicName = "MyMultiPartitionTopic";
        final int expectedNumberOfPartitions = 5;
        kafkaTestServer.createTopic(expectedTopicName, expectedNumberOfPartitions);

        // Setup our config
        SidelineConsumerConfig config = getDefaultSidelineConsumerConfig(expectedTopicName);

        // Create our Persistence Manager
        PersistenceManager persistenceManager = new InMemoryPersistenceManager();
        persistenceManager.open(Maps.newHashMap());

        // Create our consumer
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, persistenceManager);
        sidelineConsumer.open(null);

        // Ask the underlying consumer for our assigned partitions.
        Set<TopicPartition> assignedPartitions = sidelineConsumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate it
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain 5 entries", expectedNumberOfPartitions, assignedPartitions.size());
        for (int x=0; x<expectedNumberOfPartitions; x++) {
            assertTrue("Should contain our expected topic/partition " + x, assignedPartitions.contains(new TopicPartition(expectedTopicName, x)));
        }
    }

    /**
     * Tests that the unsubscribeTopicPartition() works as we expect.
     * This test uses a topic with a single partition that we are subscribed to.
     */
    @Test
    public void testUnsubscribeTopicPartitionSinglePartition() {
        // Define our expected topic/partition
        final TopicPartition expectedTopicPartition = new TopicPartition(topicName, 0);

        // Setup our config
        SidelineConsumerConfig config = getDefaultSidelineConsumerConfig();

        // Create our Persistence Manager
        PersistenceManager persistenceManager = new InMemoryPersistenceManager();
        persistenceManager.open(Maps.newHashMap());

        // Create our consumer
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, persistenceManager);
        sidelineConsumer.open(null);

        // Ask the underlying consumer for our assigned partitions.
        Set<TopicPartition> assignedPartitions = sidelineConsumer.getAssignedPartitions();
        logger.info("ASsigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain 1 entries", 1, assignedPartitions.size());
        assertTrue("Should contain our expected topic/partition 0", assignedPartitions.contains(expectedTopicPartition));

        // Now unsub from our topic partition
        final boolean result = sidelineConsumer.unsubscribeTopicPartition(expectedTopicPartition);
        assertTrue("Should have returned true", result);

        // Now ask for assigned partitions, should have none
        // Ask the underlying consumer for our assigned partitions.
        assignedPartitions = sidelineConsumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertTrue("Should be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain 0 entries", 0, assignedPartitions.size());
        assertFalse("Should NOT contain our expected topic/partition 0", assignedPartitions.contains(expectedTopicPartition));
    }

    /**
     * Tests that the unsubscribeTopicPartition() works as we expect.
     * This test uses a topic with multiple partitions that we are subscribed to.
     */
    @Test
    public void testUnsubscribeTopicPartitionMultiplePartitions() {
        // Define and create our topic
        final String expectedTopicName = "MyMultiPartitionTopic";
        final int expectedNumberOfPartitions = 5;
        kafkaTestServer.createTopic(expectedTopicName, expectedNumberOfPartitions);

        // Setup our config
        SidelineConsumerConfig config = getDefaultSidelineConsumerConfig(expectedTopicName);

        // Create our Persistence Manager
        PersistenceManager persistenceManager = new InMemoryPersistenceManager();
        persistenceManager.open(Maps.newHashMap());

        // Create our consumer
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, persistenceManager);
        sidelineConsumer.open(null);

        // Ask the underlying consumer for our assigned partitions.
        Set<TopicPartition> assignedPartitions = sidelineConsumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain entries", expectedNumberOfPartitions, assignedPartitions.size());
        for (int x=0; x<expectedNumberOfPartitions; x++) {
            assertTrue("Should contain our expected topic/partition " + x, assignedPartitions.contains(new TopicPartition(expectedTopicName, x)));
        }

        // Now unsub from our topic partition
        final int expectedRemovePartition = 2;
        final TopicPartition toRemoveTopicPartition = new TopicPartition(expectedTopicName, expectedRemovePartition);

        final boolean result = sidelineConsumer.unsubscribeTopicPartition(toRemoveTopicPartition);
        assertTrue("Should have returned true", result);

        // Now ask for assigned partitions, should have none
        // Ask the underlying consumer for our assigned partitions.
        assignedPartitions = sidelineConsumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should be not empty", assignedPartitions.isEmpty());
        assertEquals("Should contain entries", (expectedNumberOfPartitions - 1), assignedPartitions.size());
        assertFalse("Should NOT contain our removed topic/partition 0", assignedPartitions.contains(toRemoveTopicPartition));

        // Attempt to remove the same topicPartitionAgain, it should return false
        final boolean result2 = sidelineConsumer.unsubscribeTopicPartition(toRemoveTopicPartition);
        assertFalse("Should return false the second time", result2);

        // Now remove another topic/partition
        final int expectedRemovePartition2 = 4;
        final TopicPartition toRemoveTopicPartition2 = new TopicPartition(expectedTopicName, expectedRemovePartition2);

        final boolean result3 = sidelineConsumer.unsubscribeTopicPartition(toRemoveTopicPartition2);
        assertTrue("Should have returned true", result3);

        // Now ask for assigned partitions, should have none
        // Ask the underlying consumer for our assigned partitions.
        assignedPartitions = sidelineConsumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should be not empty", assignedPartitions.isEmpty());
        assertEquals("Should contain entries", (expectedNumberOfPartitions - 2), assignedPartitions.size());
        assertFalse("Should NOT contain our removed topic/partition 1", assignedPartitions.contains(toRemoveTopicPartition));
        assertFalse("Should NOT contain our removed topic/partition 2", assignedPartitions.contains(toRemoveTopicPartition2));
    }

    /**
     * We attempt to consume from the topic and get our expected messages.
     * We will NOT acknowledge any of the messages as being processed, so it should have no state saved.
     */
    @Test
    public void testSimpleConsumeFromTopicWithNoStateSaved() {
        // Define how many records to produce
        final int numberOfRecordsToProduce = 5;

        // Produce 5 entries to the topic.
        final List<ProducerRecord<byte[], byte[]>> producedRecords = produceRecords(numberOfRecordsToProduce);

        // Setup our config
        SidelineConsumerConfig config = getDefaultSidelineConsumerConfig();

        // Create our Persistence Manager
        PersistenceManager persistenceManager = new InMemoryPersistenceManager();
        persistenceManager.open(Maps.newHashMap());

        // Create our consumer
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, persistenceManager);
        sidelineConsumer.open(null);

        // Read from topic, verify we get what we expect
        for (int x=0; x<numberOfRecordsToProduce; x++) {
            ConsumerRecord<byte[], byte[]> foundRecord = sidelineConsumer.nextRecord();
            assertNotNull(foundRecord);

            // Compare to what we expected
            ProducerRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);
            logger.info("Expected {} Actual {}", expectedRecord.key(), foundRecord.key());
            assertEquals("Found expected key",  new String(expectedRecord.key(), Charsets.UTF_8), new String(foundRecord.key(), Charsets.UTF_8));
            assertEquals("Found expected value", new String(expectedRecord.value(), Charsets.UTF_8), new String(foundRecord.value(), Charsets.UTF_8));
        }

        // Next one should return null
        ConsumerRecord<byte[], byte[]> foundRecord = sidelineConsumer.nextRecord();
        assertNull("Should have nothing new to consume and be null", foundRecord);

        // Close out consumer
        sidelineConsumer.close();
    }

    /**
     * We attempt to consume from the topic and get our expected messages.
     * We ack the messages each as we get it, in order, one by one.
     */
    @Test
    public void testSimpleConsumeFromTopicWithAckingInOrderOneByOne() {
        // Define how many records to produce
        final int numberOfRecordsToProduce = 5;

        // Define our topicPartition
        final TopicPartition partition0 = new TopicPartition(topicName, 0);

        // Produce 5 entries to the topic.
        final List<ProducerRecord<byte[], byte[]>> producedRecords = produceRecords(numberOfRecordsToProduce);

        // Setup our config
        List<String> brokerHosts = Lists.newArrayList(kafkaTestServer.getKafkaServer().serverConfig().advertisedHostName() + ":" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort());
        SidelineConsumerConfig config = new SidelineConsumerConfig(brokerHosts, "MyConsumerId", topicName);
        config.setKafkaConsumerProperty("auto.offset.reset", "earliest");

        // Create our Persistence Manager
        PersistenceManager persistenceManager = new InMemoryPersistenceManager();
        persistenceManager.open(Maps.newHashMap());

        // Create our consumer
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, persistenceManager);
        sidelineConsumer.open(null);

        // Read from topic, verify we get what we expect
        for (int x=0; x<numberOfRecordsToProduce; x++) {
            ConsumerRecord<byte[], byte[]> foundRecord = sidelineConsumer.nextRecord();
            assertNotNull(foundRecord);

            // Compare to what we expected
            ProducerRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);
            logger.info("Expected {} Actual {}", expectedRecord.key(), foundRecord.key());
            assertEquals("Found expected key",  new String(expectedRecord.key(), Charsets.UTF_8), new String(foundRecord.key(), Charsets.UTF_8));
            assertEquals("Found expected value", new String(expectedRecord.value(), Charsets.UTF_8), new String(foundRecord.value(), Charsets.UTF_8));

            // Ack this message
            sidelineConsumer.commitOffset(foundRecord);

            // Verify it got updated to our current offset
            validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, foundRecord.offset());
        }
        logger.info("Consumer State {}", sidelineConsumer.flushConsumerState());

        // Close out consumer
        sidelineConsumer.close();
    }

    /**
     * We attempt to consume from the topic and get our expected messages.
     * We ack the messages each as we get it, in order, one by one.
     */
    @Test
    public void testSimpleConsumeFromTopicWithAckingInOrderAllAtTheEnd() {
        // Define how many records to produce
        final int numberOfRecordsToProduce = 5;

        // Define our topicPartition
        final TopicPartition partition0 = new TopicPartition(topicName, 0);

        // Produce 5 entries to the topic.
        final List<ProducerRecord<byte[], byte[]>> producedRecords = produceRecords(numberOfRecordsToProduce);

        // Setup our config
        List<String> brokerHosts = Lists.newArrayList(kafkaTestServer.getKafkaServer().serverConfig().advertisedHostName() + ":" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort());
        SidelineConsumerConfig config = new SidelineConsumerConfig(brokerHosts, "MyConsumerId", topicName);
        config.setKafkaConsumerProperty("auto.offset.reset", "earliest");

        // Create our Persistence Manager
        PersistenceManager persistenceManager = new InMemoryPersistenceManager();
        persistenceManager.open(Maps.newHashMap());

        // Create our consumer
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, persistenceManager);
        sidelineConsumer.open(null);

        // Read from topic, verify we get what we expect
        List<ConsumerRecord> foundRecords = Lists.newArrayList();
        for (int x=0; x<numberOfRecordsToProduce; x++) {
            ConsumerRecord<byte[], byte[]> foundRecord = sidelineConsumer.nextRecord();
            assertNotNull(foundRecord);
            foundRecords.add(foundRecord);

            // Compare to what we expected
            ProducerRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);
            logger.info("Expected {} Actual {}", expectedRecord.key(), foundRecord.key());
            assertEquals("Found expected key",  new String(expectedRecord.key(), Charsets.UTF_8), new String(foundRecord.key(), Charsets.UTF_8));
            assertEquals("Found expected value", new String(expectedRecord.value(), Charsets.UTF_8), new String(foundRecord.value(), Charsets.UTF_8));
        }
        logger.info("Consumer State {}", sidelineConsumer.flushConsumerState());

        // Verify state is still 0
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 0L);

        // Now ack them one by one
        for (ConsumerRecord foundRecord : foundRecords) {
            sidelineConsumer.commitOffset(foundRecord);
        }

        // Now validate state.
        ConsumerState consumerState = sidelineConsumer.flushConsumerState();

        // Verify it got updated to our current offset
        validateConsumerState(consumerState, partition0, (numberOfRecordsToProduce - 1));
        logger.info("Consumer State {}", sidelineConsumer.flushConsumerState());

        // Close out consumer
        sidelineConsumer.close();
    }

    /**
     * We attempt to consume from the topic and get our expected messages.
     * We ack the messages each as we get it, in order, one by one.
     */
    @Test
    public void testSimpleConsumeFromTopicWithAckingOutOfOrderAllAtTheEnd() {
        // Define how many records to produce
        final int numberOfRecordsToProduce = 9;

        // Define our topic/partition.
        final TopicPartition partition0 = new TopicPartition(topicName, 0);

        // Produce 5 entries to the topic.
        final List<ProducerRecord<byte[], byte[]>> producedRecords = produceRecords(numberOfRecordsToProduce);

        // Setup our config
        List<String> brokerHosts = Lists.newArrayList(kafkaTestServer.getKafkaServer().serverConfig().advertisedHostName() + ":" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort());
        SidelineConsumerConfig config = new SidelineConsumerConfig(brokerHosts, "MyConsumerId", topicName);
        config.setKafkaConsumerProperty("auto.offset.reset", "earliest");

        // Create our Persistence Manager
        PersistenceManager persistenceManager = new InMemoryPersistenceManager();
        persistenceManager.open(Maps.newHashMap());

        // Create our consumer
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, persistenceManager);
        sidelineConsumer.open(null);

        // Read from topic, verify we get what we expect
        List<ConsumerRecord> foundRecords = Lists.newArrayList();
        for (int x=0; x<numberOfRecordsToProduce; x++) {
            ConsumerRecord<byte[], byte[]> foundRecord = sidelineConsumer.nextRecord();
            assertNotNull(foundRecord);
            foundRecords.add(foundRecord);

            // Compare to what we expected
            ProducerRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);
            logger.info("Expected {} Actual {}", expectedRecord.key(), foundRecord.key());
            assertEquals("Found expected key",  new String(expectedRecord.key(), Charsets.UTF_8), new String(foundRecord.key(), Charsets.UTF_8));
            assertEquals("Found expected value", new String(expectedRecord.value(), Charsets.UTF_8), new String(foundRecord.value(), Charsets.UTF_8));
        }
        logger.info("Consumer State {}", sidelineConsumer.flushConsumerState());

        // Verify state is still 0
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 0L);

        // Now ack in the following order:
        // commit offset 2 => offset should be 0 still
        sidelineConsumer.commitOffset(partition0, 2L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 0L);

        // commit offset 1 => offset should be 0 still
        sidelineConsumer.commitOffset(partition0, 1L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 0L);

        // commit offset 0 => offset should be 2 now
        sidelineConsumer.commitOffset(partition0, 0L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 2L);

        // commit offset 3 => offset should be 3 now
        sidelineConsumer.commitOffset(partition0, 3L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 3L);

        // commit offset 4 => offset should be 4 now
        sidelineConsumer.commitOffset(partition0, 4L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 4L);

        // commit offset 5 => offset should be 5 now
        sidelineConsumer.commitOffset(partition0, 5L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 5L);

        // commit offset 7 => offset should be 5 still
        sidelineConsumer.commitOffset(partition0, 7L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 5L);

        // commit offset 8 => offset should be 5 still
        sidelineConsumer.commitOffset(partition0, 8L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 5L);

        // commit offset 6 => offset should be 8 now
        sidelineConsumer.commitOffset(partition0, 6L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 8L);

        // Now validate state.
        logger.info("Consumer State {}", sidelineConsumer.flushConsumerState());

        // Close out consumer
        sidelineConsumer.close();
    }

    /**
     * We attempt to consume from the topic and get our expected messages.
     * We use a previously saved state to skip the first several messages.
     */
    @Test
    public void testConsumerWithInitialStateToSkipMessages() {
        // Define how many records to produce
        final int numberOfRecordsToProduce = 10;
        final int numberOfExpectedRecordsToConsume = 5;

        // Produce entries to the topic.
        final List<ProducerRecord<byte[], byte[]>> producedRecords = produceRecords(numberOfRecordsToProduce);

        // Setup our config
        SidelineConsumerConfig config = getDefaultSidelineConsumerConfig();

        // Create our Persistence Manager
        PersistenceManager persistenceManager = new InMemoryPersistenceManager();
        persistenceManager.open(Maps.newHashMap());

        // Create a state in which we have already acked the first 5 messages
        ConsumerState consumerState = new ConsumerState();
        consumerState.setOffset(new TopicPartition(topicName, 0), 5L);
        persistenceManager.persistConsumerState(config.getConsumerId(), consumerState);

        // Create our consumer
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, persistenceManager);
        sidelineConsumer.open(null);

        // Read from topic, verify we get what we expect
        for (int x=numberOfExpectedRecordsToConsume; x<numberOfRecordsToProduce; x++) {
            ConsumerRecord<byte[], byte[]> foundRecord = sidelineConsumer.nextRecord();
            assertNotNull(foundRecord);

            // Compare to what we expected
            ProducerRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);
            logger.info("Expected {} Actual {}", expectedRecord.key(), foundRecord.key());
            assertEquals("Found expected key",  new String(expectedRecord.key(), Charsets.UTF_8), new String(foundRecord.key(), Charsets.UTF_8));
            assertEquals("Found expected value", new String(expectedRecord.value(), Charsets.UTF_8), new String(foundRecord.value(), Charsets.UTF_8));
        }

        // Next one should return null
        ConsumerRecord<byte[], byte[]> foundRecord = sidelineConsumer.nextRecord();
        assertNull("Should have nothing new to consume and be null", foundRecord);

        // Close out consumer
        sidelineConsumer.close();
    }

    /**
     * 1. Setup a consumer to consume from a topic with 2 partitions.
     * 2. Produce several messages into both partitions
     * 3. Consume all of the msgs from the topic.
     * 4. Ack in various orders for the msgs
     * 5. Validate that the state is correct.
     */
    @Test
    public void testConsumeFromTopicWithMultipePartitionsWithAcking() {
        this.topicName = "MyMultiPartitionTopic";
        final int expectedNumberOfPartitions = 2;

        // Create our multi-partition topic.
        kafkaTestServer.createTopic(topicName, expectedNumberOfPartitions);

        // Define our expected topic/partitions
        final TopicPartition partition0 = new TopicPartition(topicName, 0);
        final TopicPartition partition1 = new TopicPartition(topicName, 1);

        // Setup our config
        SidelineConsumerConfig config = getDefaultSidelineConsumerConfig(topicName);

        // Create our Persistence Manager
        PersistenceManager persistenceManager = new InMemoryPersistenceManager();
        persistenceManager.open(Maps.newHashMap());

        // Create our consumer
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, persistenceManager);
        sidelineConsumer.open(null);

        // Ask the underlying consumer for our assigned partitions.
        Set<TopicPartition> assignedPartitions = sidelineConsumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain 2 entries", expectedNumberOfPartitions, assignedPartitions.size());
        assertTrue("Should contain our expected topic/partition 0", assignedPartitions.contains(partition0));
        assertTrue("Should contain our expected topic/partition 1", assignedPartitions.contains(partition1));

        // Now produce 5 msgs to each topic (10 msgs total)
        final int expectedNumberOfMsgsPerPartition = 5;
        List<ProducerRecord<byte[], byte[]>> producedRecordsPartition0 = produceRecords(expectedNumberOfMsgsPerPartition, 0);
        List<ProducerRecord<byte[], byte[]>> producedRecordsPartition1 = produceRecords(expectedNumberOfMsgsPerPartition, 1);

        // Attempt to consume them
        // Read from topic, verify we get what we expect
        int partition0Index = 0;
        int partition1Index = 0;
        for (int x=0; x<(expectedNumberOfMsgsPerPartition * 2); x++) {
            ConsumerRecord<byte[], byte[]> foundRecord = sidelineConsumer.nextRecord();
            assertNotNull(foundRecord);

            // Determine which partition its from
            final int partitionSource = foundRecord.partition();
            assertTrue("Should be partition 0 or 1", partitionSource == 0 || partitionSource == 1);

            ProducerRecord<byte[], byte[]> expectedRecord;
            if (partitionSource == 0) {
                expectedRecord = producedRecordsPartition0.get(partition0Index);
                partition0Index++;
            } else {
                expectedRecord = producedRecordsPartition1.get(partition1Index);
                partition1Index++;
            }

            // Compare to what we expected
            logger.info("Expected {} Actual {}", expectedRecord.key(), foundRecord.key());
            assertEquals("Found expected key",  new String(expectedRecord.key(), Charsets.UTF_8), new String(foundRecord.key(), Charsets.UTF_8));
            assertEquals("Found expected value", new String(expectedRecord.value(), Charsets.UTF_8), new String(foundRecord.value(), Charsets.UTF_8));
        }
        // Next one should return null
        ConsumerRecord<byte[], byte[]> foundRecord = sidelineConsumer.nextRecord();
        assertNull("Should have nothing new to consume and be null", foundRecord);

        logger.info("Consumer State {}", sidelineConsumer.flushConsumerState());

        // Verify state is still 0 for partition 0
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 0L);

        // Verify state is still 0 for partition 1
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition1, 0L);

        // Ack offset 1 on partition 0, state should be: [partition0: 0, partition1: 0]
        sidelineConsumer.commitOffset(partition0, 1L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 0L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition1, 0L);

        // Ack offset 0 on partition 0, state should be: [partition0: 1, partition1: 0]
        sidelineConsumer.commitOffset(partition0, 0L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 1L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition1, 0L);

        // Ack offset 2 on partition 0, state should be: [partition0: 2, partition1: 0]
        sidelineConsumer.commitOffset(partition0, 2L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 2L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition1, 0L);

        // Ack offset 0 on partition 1, state should be: [partition0: 2, partition1: 0]
        sidelineConsumer.commitOffset(partition1, 0L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 2L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition1, 0L);

        // Ack offset 2 on partition 1, state should be: [partition0: 2, partition1: 0]
        sidelineConsumer.commitOffset(partition1, 2L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 2L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition1, 0L);

        // Ack offset 0 on partition 1, state should be: [partition0: 2, partition1: 0]
        sidelineConsumer.commitOffset(partition1, 0L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 2L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition1, 0L);

        // Ack offset 1 on partition 1, state should be: [partition0: 2, partition1: 2]
        sidelineConsumer.commitOffset(partition1, 1L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 2L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition1, 2L);

        // Ack offset 3 on partition 1, state should be: [partition0: 2, partition1: 3]
        sidelineConsumer.commitOffset(partition1, 3L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition0, 2L);
        validateConsumerState(sidelineConsumer.flushConsumerState(), partition1, 3L);

        // Close out consumer
        sidelineConsumer.close();
    }

    /**
     * 1. Setup a consumer to consume from a topic with 1 partition.
     * 2. Produce several messages into that partition.
     * 3. Consume all of the msgs from the topic.
     * 4. Produce more msgs into the topic.
     * 5. Unsubscribe from that partition.
     * 6. Attempt to consume more msgs, verify none are found.
     */
    @Test
    public void testConsumeFromTopicAfterUnsubscribingFromSinglePartition() {
        // Define our expected topic/partition
        final TopicPartition expectedTopicPartition = new TopicPartition(topicName, 0);

        // Setup our config
        SidelineConsumerConfig config = getDefaultSidelineConsumerConfig();

        // Create our Persistence Manager
        PersistenceManager persistenceManager = new InMemoryPersistenceManager();
        persistenceManager.open(Maps.newHashMap());

        // Create our consumer
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, persistenceManager);
        sidelineConsumer.open(null);

        // Ask the underlying consumer for our assigned partitions.
        Set<TopicPartition> assignedPartitions = sidelineConsumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain 1 entries", 1, assignedPartitions.size());
        assertTrue("Should contain our expected topic/partition 0", assignedPartitions.contains(expectedTopicPartition));

        // Now produce 5 msgs
        final int expectedNumberOfMsgs = 5;
        List<ProducerRecord<byte[], byte[]>> producedRecords = produceRecords(expectedNumberOfMsgs);

        // Attempt to consume them
        // Read from topic, verify we get what we expect
        for (int x=0; x<expectedNumberOfMsgs; x++) {
            ConsumerRecord<byte[], byte[]> foundRecord = sidelineConsumer.nextRecord();
            assertNotNull(foundRecord);

            // Compare to what we expected
            ProducerRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);
            logger.info("Expected {} Actual {}", expectedRecord.key(), foundRecord.key());
            assertEquals("Found expected key",  new String(expectedRecord.key(), Charsets.UTF_8), new String(foundRecord.key(), Charsets.UTF_8));
            assertEquals("Found expected value", new String(expectedRecord.value(), Charsets.UTF_8), new String(foundRecord.value(), Charsets.UTF_8));
        }

        // Next one should return null
        ConsumerRecord<byte[], byte[]> foundRecord = sidelineConsumer.nextRecord();
        assertNull("Should have nothing new to consume and be null", foundRecord);

        // Now produce 5 more msgs
        producedRecords = produceRecords(expectedNumberOfMsgs);

        // Now unsub from the partition
        final boolean result = sidelineConsumer.unsubscribeTopicPartition(expectedTopicPartition);
        assertTrue("Should be true", result);

        // Attempt to consume, but nothing should be returned, because we unsubscribed.
        for (int x=0; x<expectedNumberOfMsgs; x++) {
            foundRecord = sidelineConsumer.nextRecord();
            assertNull(foundRecord);
        }

        // Close out consumer
        sidelineConsumer.close();
    }

    /**
     * 1. Setup a consumer to consume from a topic with 2 partitions.
     * 2. Produce several messages into both partitions
     * 3. Consume all of the msgs from the topic.
     * 4. Produce more msgs into both partitions.
     * 5. Unsubscribe from partition 0.
     * 6. Attempt to consume more msgs, verify only those from partition 1 come thru.
     */
    @Test
    public void testConsumeFromTopicAfterUnsubscribingFromMultipePartitions() {
        this.topicName = "MyMultiPartitionTopic";
        final int expectedNumberOfPartitions = 2;

        // Create our multi-partition topic.
        kafkaTestServer.createTopic(topicName, expectedNumberOfPartitions);

        // Define our expected topic/partitions
        final TopicPartition expectedTopicPartition0 = new TopicPartition(topicName, 0);
        final TopicPartition expectedTopicPartition1 = new TopicPartition(topicName, 1);

        // Setup our config
        SidelineConsumerConfig config = getDefaultSidelineConsumerConfig(topicName);

        // Create our Persistence Manager
        PersistenceManager persistenceManager = new InMemoryPersistenceManager();
        persistenceManager.open(Maps.newHashMap());

        // Create our consumer
        SidelineConsumer sidelineConsumer = new SidelineConsumer(config, persistenceManager);
        sidelineConsumer.open(null);

        // Ask the underlying consumer for our assigned partitions.
        Set<TopicPartition> assignedPartitions = sidelineConsumer.getAssignedPartitions();
        logger.info("ASsigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain 2 entries", expectedNumberOfPartitions, assignedPartitions.size());
        assertTrue("Should contain our expected topic/partition 0", assignedPartitions.contains(expectedTopicPartition0));
        assertTrue("Should contain our expected topic/partition 1", assignedPartitions.contains(expectedTopicPartition1));

        // Now produce 5 msgs to each topic (10 msgs total)
        final int expectedNumberOfMsgsPerPartition = 5;
        List<ProducerRecord<byte[], byte[]>> producedRecordsPartition0 = produceRecords(expectedNumberOfMsgsPerPartition, 0);
        List<ProducerRecord<byte[], byte[]>> producedRecordsPartition1 = produceRecords(expectedNumberOfMsgsPerPartition, 1);

        // Attempt to consume them
        // Read from topic, verify we get what we expect
        int partition0Index = 0;
        int partition1Index = 0;
        for (int x=0; x<(expectedNumberOfMsgsPerPartition * 2); x++) {
            ConsumerRecord<byte[], byte[]> foundRecord = sidelineConsumer.nextRecord();
            assertNotNull(foundRecord);

            // Determine which partition its from
            final int partitionSource = foundRecord.partition();
            assertTrue("Should be partition 0 or 1", partitionSource == 0 || partitionSource == 1);

            ProducerRecord<byte[], byte[]> expectedRecord;
            if (partitionSource == 0) {
                expectedRecord = producedRecordsPartition0.get(partition0Index);
                partition0Index++;
            } else {
                expectedRecord = producedRecordsPartition1.get(partition1Index);
                partition1Index++;
            }

            // Compare to what we expected
            logger.info("Expected {} Actual {}", expectedRecord.key(), foundRecord.key());
            assertEquals("Found expected key",  new String(expectedRecord.key(), Charsets.UTF_8), new String(foundRecord.key(), Charsets.UTF_8));
            assertEquals("Found expected value", new String(expectedRecord.value(), Charsets.UTF_8), new String(foundRecord.value(), Charsets.UTF_8));
        }
        // Next one should return null
        ConsumerRecord<byte[], byte[]> foundRecord = sidelineConsumer.nextRecord();
        assertNull("Should have nothing new to consume and be null", foundRecord);

        // Now produce 5 more msgs into each partition
        producedRecordsPartition0 = produceRecords(expectedNumberOfMsgsPerPartition, 0);
        producedRecordsPartition1 = produceRecords(expectedNumberOfMsgsPerPartition, 1);

        // Now unsub from the partition 0
        final boolean result = sidelineConsumer.unsubscribeTopicPartition(expectedTopicPartition0);
        assertTrue("Should be true", result);

        // Attempt to consume, but nothing should be returned from partition 0, only partition 1
        for (int x=0; x<expectedNumberOfMsgsPerPartition; x++) {
            foundRecord = sidelineConsumer.nextRecord();
            assertNotNull(foundRecord);

            // Determine which partition its from, should be only 1
            assertEquals("Should be partition 1", 1, foundRecord.partition());

            // Validate it
            ProducerRecord<byte[], byte[]> expectedRecord = producedRecordsPartition1.get(x);

            // Compare to what we expected
            logger.info("Expected {} Actual {}", expectedRecord.key(), foundRecord.key());
            assertEquals("Found expected key",  new String(expectedRecord.key(), Charsets.UTF_8), new String(foundRecord.key(), Charsets.UTF_8));
            assertEquals("Found expected value", new String(expectedRecord.value(), Charsets.UTF_8), new String(foundRecord.value(), Charsets.UTF_8));
        }

        // Next one should return null
        foundRecord = sidelineConsumer.nextRecord();
        assertNull("Should have nothing new to consume and be null", foundRecord);

        // Close out consumer
        sidelineConsumer.close();
    }

    /**
     * Helper method
     * @param consumerState - the consumer state we want to validate
     * @param topicPartition - the topic/partition we want to validate
     * @param expectedOffset - the offset we expect
     */
    private void validateConsumerState(ConsumerState consumerState, TopicPartition topicPartition, long expectedOffset) {
        final long actualOffset = consumerState.getOffsetForTopicAndPartition(topicPartition);
        assertEquals("Expected offset", expectedOffset, actualOffset);
    }

    private List<ProducerRecord<byte[], byte[]>> produceRecords(final int numberOfRecords) {
        return produceRecords(numberOfRecords, 0);
    }

    private List<ProducerRecord<byte[], byte[]>> produceRecords(final int numberOfRecords, final int partitionId) {
        List<ProducerRecord<byte[], byte[]>> producedRecords = Lists.newArrayList();

        KafkaProducer producer = kafkaTestServer.getKafkaProducer("org.apache.kafka.common.serialization.ByteArraySerializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        for (int x=0; x<numberOfRecords; x++) {
            // Construct key and value
            long timeStamp = DateTime.now().getMillis();
            String key = "key" + timeStamp;
            String value = "value" + timeStamp;

            // Construct filter
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, partitionId, key.getBytes(Charsets.UTF_8), value.getBytes(Charsets.UTF_8));
            producedRecords.add(record);

            // Send it.
            producer.send(record);
        }
        // Publish to the topic and close.
        producer.flush();
        logger.info("Produce completed");
        producer.close();

        return producedRecords;
    }

    private SidelineConsumerConfig getDefaultSidelineConsumerConfig() {
        return getDefaultSidelineConsumerConfig(topicName);
    }

    private SidelineConsumerConfig getDefaultSidelineConsumerConfig(final String topicName) {
        List<String> brokerHosts = Lists.newArrayList(kafkaTestServer.getKafkaServer().serverConfig().advertisedHostName() + ":" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort());
        SidelineConsumerConfig config = new SidelineConsumerConfig(brokerHosts, "MyConsumerId", topicName);
        config.setKafkaConsumerProperty("auto.offset.reset", "earliest");
        return config;
    }
}