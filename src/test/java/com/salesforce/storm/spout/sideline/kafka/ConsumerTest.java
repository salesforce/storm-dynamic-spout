package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.sideline.ConsumerPartition;
import com.salesforce.storm.spout.sideline.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.consumer.ConsumerCohortDefinition;
import com.salesforce.storm.spout.sideline.consumer.ConsumerState;
import com.salesforce.storm.spout.sideline.consumer.Record;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Deserializer;
import com.salesforce.storm.spout.sideline.kafka.deserializer.NullDeserializer;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Utf8StringDeserializer;
import com.salesforce.storm.spout.sideline.persistence.InMemoryPersistenceAdapter;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.utils.KafkaTestUtils;
import com.salesforce.storm.spout.sideline.utils.ProducedKafkaRecord;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import com.google.common.base.Charsets;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
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
@RunWith(DataProviderRunner.class)
public class ConsumerTest {
    // TODO: these test cases
    // test calling open() w/ a starting state.

    private static final Logger logger = LoggerFactory.getLogger(ConsumerTest.class);
    private static KafkaTestServer kafkaTestServer;
    private String topicName;

    /**
     * By default, no exceptions should be thrown.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Here we stand up an internal test kafka and zookeeper service.
     * Once for all methods in this class.
     */
    @BeforeClass
    public static void setupKafkaServer() throws Exception {
        // Setup kafka test server
        kafkaTestServer = new KafkaTestServer();
        kafkaTestServer.start();
    }

    /**
     * This happens once before every test method.
     * Create a new empty namespace with randomly generated name.
     */
    @Before
    public void beforeTest() {
        // Generate namespace name
        topicName = ConsumerTest.class.getSimpleName() + Clock.systemUTC().millis();

        // Create namespace
        kafkaTestServer.createTopic(topicName);
    }

    /**
     * Here we shut down the internal test kafka and zookeeper services.
     */
    @AfterClass
    public static void destroyKafkaServer() {
        // Close out kafka test server if needed
        if (kafkaTestServer == null) {
            return;
        }
        try {
            kafkaTestServer.shutdown();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        kafkaTestServer = null;
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
        persistenceAdapter.open(Maps.newHashMap());

        // Generate a VirtualSpoutIdentifier
        final VirtualSpoutIdentifier virtualSpoutIdentifier = getDefaultVSpoutId();

        // Generate a consumer cohort def
        final ConsumerCohortDefinition consumerCohortDefinition = getDefaultConsumerCohortDefinition();

        // Define expected kafka brokers
        final String expectedKafkaBrokers =
            kafkaTestServer.getKafkaServer().serverConfig().advertisedHostName() + ":"
            + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort();

        // Call constructor
        Consumer consumer = new Consumer();
        consumer.open(config, virtualSpoutIdentifier, consumerCohortDefinition, persistenceAdapter, null);

        // Validate our instances got set
        assertNotNull("Config is not null", consumer.getConsumerConfig());

        // Deeper validation of generated ConsumerConfig
        final ConsumerConfig foundConsumerConfig = consumer.getConsumerConfig();
        assertEquals("ConsumerIdSet as expected", virtualSpoutIdentifier.toString(), foundConsumerConfig.getConsumerId());
        assertEquals("Topic set correctly", topicName, foundConsumerConfig.getTopic());
        assertEquals("KafkaBrokers set correctly", expectedKafkaBrokers, foundConsumerConfig.getKafkaConsumerProperties().getProperty(BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("Set Number of Consumers as expected", consumerCohortDefinition.getTotalInstances(), consumer.getConsumerConfig().getNumberOfConsumers());
        assertEquals("Set Index of OUR Consumer is set as expected", consumerCohortDefinition.getInstanceNumber(), consumer.getConsumerConfig().getIndexOfConsumer());

        // Additional properties set correctly
        assertNotNull("PersistenceAdapter is not null", consumer.getPersistenceAdapter());
        assertEquals(persistenceAdapter, consumer.getPersistenceAdapter());
        assertNotNull("Deserializer is not null", consumer.getDeserializer());
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
        KafkaConsumer<byte[], byte[]> mockKafkaConsumer = mock(KafkaConsumer.class);

        // When we call partitionsFor(), we should return a single partition number 0 for our namespace.
        List<PartitionInfo> mockPartitionInfos = Lists.newArrayList(
            new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0])
        );
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // When we ask for the position of partition 0, we should return 1000L
        when(mockKafkaConsumer.position(partition0)).thenReturn(earliestPosition);

        // Create instance of a StateConsumer, we'll just use a dummy instance.
        PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);

        // When getState is called, return the following state
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), anyInt())).thenReturn(null);

        // Call constructor injecting our mocks
        Consumer consumer = new Consumer(mockKafkaConsumer);

        // Now call open
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), mockPersistenceAdapter,null);

        // Now call open again, we expect this to throw an exception
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("open more than once");

        // Call it
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(),mockPersistenceAdapter, null);
    }

    /**
     * Tests that our logic for flushing consumer state works if auto commit is enabled.
     * This is kind of a weak test for this.
     *
     * This test is disabled because we have no way exposed to set this property anymore.
     */
    @Test
    @Ignore
    public void testTimedFlushConsumerState() throws InterruptedException {
        final String expectedConsumerId = "MyConsumerId";

        // Setup our config
        final Map<String, Object> config = getDefaultConfig();

        // TODO - We used to be able to set this, but its not accessible anymore.
        // Enable auto commit and Set timeout to 1 second.
        // ConsumerConfig consumerConfig = new ConsumerConfig();
        //consumerConfig.setConsumerStateAutoCommit(true);
        //consumerConfig.setConsumerStateAutoCommitIntervalMs(1000);

        // Create mock persistence manager so we can determine if it was called
        PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);

        // Create a mock clock so we can control time (bwahaha)
        Instant instant = Clock.systemUTC().instant();
        Clock mockClock = Clock.fixed(instant, ZoneId.systemDefault());

        KafkaConsumer<byte[], byte[]> mockKafkaConsumer = mock(KafkaConsumer.class);

        List<PartitionInfo> mockPartitionInfos = Lists.newArrayList(new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0]));
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // Call constructor
        Consumer consumer = new Consumer(mockKafkaConsumer);
        consumer.setClock(mockClock);
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), mockPersistenceAdapter, null);

        // Call our method once
        consumer.timedFlushConsumerState();

        // Make sure persistence layer was not hit
        verify(mockPersistenceAdapter, never()).persistConsumerState(anyString(), anyInt(), anyLong());

        // Sleep for 1.5 seconds
        Thread.sleep(1500);

        // Call our method again
        consumer.timedFlushConsumerState();

        // Make sure persistence layer was not hit because we're using a mocked clock that has not changed :p
        verify(mockPersistenceAdapter, never()).persistConsumerState(anyString(), anyInt(), anyLong());

        // Now lets adjust our mock clock up by 2 seconds.
        instant = instant.plus(2000, ChronoUnit.MILLIS);
        mockClock = Clock.fixed(instant, ZoneId.systemDefault());
        consumer.setClock(mockClock);

        // Call our method again, it should have triggered this time.
        consumer.timedFlushConsumerState();

        // Make sure persistence layer WAS hit because we adjust our mock clock ahead 2 secs
        verify(mockPersistenceAdapter, times(1)).persistConsumerState(eq(expectedConsumerId), anyInt(), anyLong());

        // Call our method again, it shouldn't fire.
        consumer.timedFlushConsumerState();

        // Make sure persistence layer was not hit again because we're using a mocked clock that has not changed since the last call :p
        verify(mockPersistenceAdapter, times(1)).persistConsumerState(anyString(), anyInt(), anyLong());

        // Now lets adjust our mock clock up by 1.5 seconds.
        instant = instant.plus(1500, ChronoUnit.MILLIS);
        mockClock = Clock.fixed(instant, ZoneId.systemDefault());
        consumer.setClock(mockClock);

        // Call our method again, it should have triggered this time.
        consumer.timedFlushConsumerState();

        // Make sure persistence layer WAS hit a 2nd time because we adjust our mock clock ahead
        verify(mockPersistenceAdapter, times(2)).persistConsumerState(eq(expectedConsumerId), anyInt(), anyLong());
    }

    /**
     * Tests that our logic for flushing consumer state is disabled if auto commit is disabled.
     * This is kind of a weak test for this.
     */
    @Test
    public void testTimedFlushConsumerStateWhenAutoCommitIsDisabled() throws InterruptedException {
        // Create config
        final Map<String, Object> config = getDefaultConfig(topicName);

        // This is disabled by default... so this test should still run ok
        // Disable and set interval to 1 second.
        //config.setConsumerStateAutoCommit(false);
        //config.setConsumerStateAutoCommitIntervalMs(1000);

        // Create mock persistence manager so we can determine if it was called
        PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);

        // Create a mock clock so we can control time (bwahaha)
        Instant instant = Clock.systemUTC().instant();
        Clock mockClock = Clock.fixed(instant, ZoneId.systemDefault());

        KafkaConsumer<byte[], byte[]> mockKafkaConsumer = mock(KafkaConsumer.class);

        List<PartitionInfo> mockPartitionInfos = Lists.newArrayList(new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0]));
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // Call constructor
        Consumer consumer = new Consumer();
        consumer.setClock(mockClock);

        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), mockPersistenceAdapter, null);

        // Call our method once
        consumer.timedFlushConsumerState();

        // Make sure persistence layer was not hit
        verify(mockPersistenceAdapter, never()).persistConsumerState(anyString(), anyInt(), anyLong());

        // Sleep for 1.5 seconds
        Thread.sleep(1500);

        // Call our method again
        consumer.timedFlushConsumerState();

        // Make sure persistence layer was not hit because we're using a mocked clock that has not changed :p
        verify(mockPersistenceAdapter, never()).persistConsumerState(anyString(), anyInt(), anyLong());

        // Now lets adjust our mock clock up by 2 seconds.
        instant = instant.plus(2000, ChronoUnit.MILLIS);
        mockClock = Clock.fixed(instant, ZoneId.systemDefault());
        consumer.setClock(mockClock);

        // Call our method again, it should have triggered this time.
        consumer.timedFlushConsumerState();

        // Make sure persistence layer was not hit
        verify(mockPersistenceAdapter, never()).persistConsumerState(anyString(), anyInt(), anyLong());

        // Call our method again, it shouldn't fire.
        consumer.timedFlushConsumerState();

        // Make sure persistence layer was not hit
        verify(mockPersistenceAdapter, never()).persistConsumerState(anyString(), anyInt(), anyLong());

        // Now lets adjust our mock clock up by 1.5 seconds.
        instant = instant.plus(1500, ChronoUnit.MILLIS);
        mockClock = Clock.fixed(instant, ZoneId.systemDefault());
        consumer.setClock(mockClock);

        // Call our method again, it should have triggered this time.
        consumer.timedFlushConsumerState();

        // Make sure persistence layer was not hit
        verify(mockPersistenceAdapter, never()).persistConsumerState(anyString(), anyInt(), anyLong());
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
        KafkaConsumer<byte[], byte[]> mockKafkaConsumer = mock(KafkaConsumer.class);

        // When we call partitionsFor(), we should return a single partition number 0 for our namespace.
        List<PartitionInfo> mockPartitionInfos = Lists.newArrayList(new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0]));
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // When we ask for the position of partition 0, we should return 1000L
        when(mockKafkaConsumer.position(kafkaTopicPartition0)).thenReturn(earliestPosition);

        // Create instance of a StateConsumer, we'll just use a dummy instance.
        PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);

        // Create mock Deserializer
        Deserializer mockDeserializer = mock(Deserializer.class);

        // When getState is called, return the following state
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), anyInt())).thenReturn(null);

        // Call constructor injecting our mocks
        Consumer consumer = new Consumer(mockKafkaConsumer);
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), mockPersistenceAdapter, null);

        // For every partition returned by mockKafkaConsumer.partitionsFor(), we should subscribe to them via the mockKafkaConsumer.assign() call
        verify(mockKafkaConsumer, times(1)).assign(eq(Lists.newArrayList(kafkaTopicPartition0)));

        // Since ConsumerStateManager has no state for partition 0, we should call seekToBeginning on that partition
        verify(mockKafkaConsumer, times(1)).seekToBeginning(eq(Lists.newArrayList(kafkaTopicPartition0)));

        // Verify position was asked for
        verify(mockKafkaConsumer, times(1)).position(kafkaTopicPartition0);

        // Verify ConsumerStateManager returns the correct values
        final ConsumerState currentState = consumer.getCurrentState();
        assertNotNull("Should be non-null", currentState);

        // State should have one entry
        assertEquals("Should have 1 entry", 1, currentState.size());

        // Offset should have offset 1000L - 1 for completed offset.
        assertEquals("Expected value should be 999", (earliestPosition - 1), (long) currentState.getOffsetForNamespaceAndPartition(partition0));
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
        KafkaConsumer<byte[], byte[]> mockKafkaConsumer = mock(KafkaConsumer.class);

        // When we call partitionsFor(), We return partitions 0,1, and 2.
        List<PartitionInfo> mockPartitionInfos = Lists.newArrayList(
                new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
                new PartitionInfo(topicName, 1, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
                new PartitionInfo(topicName, 2, new Node(0, "localhost", 9092), new Node[0], new Node[0])
        );
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // Create instance of a PersistenceAdapter, we'll just use a dummy instance.
        PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);

        // When getState is called, return the following state
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), anyInt())).thenReturn(null);

        // When we ask for the positions for each partition return mocked values
        when(mockKafkaConsumer.position(kafkaTopicPartition0)).thenReturn(earliestPositionPartition0);
        when(mockKafkaConsumer.position(kafkaTopicPartition1)).thenReturn(earliestPositionPartition1);
        when(mockKafkaConsumer.position(kafkaTopicPartition2)).thenReturn(earliestPositionPartition2);

        // Call constructor injecting our mocks
        Consumer consumer = new Consumer(mockKafkaConsumer);
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), mockPersistenceAdapter, null);

        // For every partition returned by mockKafkaConsumer.partitionsFor(), we should subscribe to them via the mockKafkaConsumer.assign() call
        verify(mockKafkaConsumer, times(1)).assign(eq(Lists.newArrayList(
                new TopicPartition(topicName, 0),
                new TopicPartition(topicName, 1),
                new TopicPartition(topicName, 2))));

        // Since ConsumerStateManager has no state for partition 0, we should call seekToBeginning on that partition
        verify(mockKafkaConsumer, times(1)).seekToBeginning(eq(Lists.newArrayList(
            new TopicPartition(topicName, 0)
        )));
        verify(mockKafkaConsumer, times(1)).seekToBeginning(eq(Lists.newArrayList(
            new TopicPartition(topicName, 1)
        )));
        verify(mockKafkaConsumer, times(1)).seekToBeginning(eq(Lists.newArrayList(
            new TopicPartition(topicName, 2)
        )));

        // Validate we got our calls for the current position
        verify(mockKafkaConsumer, times(1)).position(kafkaTopicPartition0);
        verify(mockKafkaConsumer, times(1)).position(kafkaTopicPartition1);
        verify(mockKafkaConsumer, times(1)).position(kafkaTopicPartition2);

        // Verify ConsumerStateManager returns the correct values
        final ConsumerState currentState = consumer.getCurrentState();
        assertNotNull("Should be non-null", currentState);

        // State should have one entry
        assertEquals("Should have 3 entries", 3, currentState.size());

        // Offsets should be the earliest position - 1
        assertEquals("Expected value for partition0", (earliestPositionPartition0 - 1), (long) currentState.getOffsetForNamespaceAndPartition(partition0));
        assertEquals("Expected value for partition1", (earliestPositionPartition1 - 1), (long) currentState.getOffsetForNamespaceAndPartition(partition1));
        assertEquals("Expected value for partition2", (earliestPositionPartition2 - 1), (long) currentState.getOffsetForNamespaceAndPartition(partition2));
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
        KafkaConsumer<byte[], byte[]> mockKafkaConsumer = mock(KafkaConsumer.class);

        // When we call partitionsFor(), we should return a single partition number 0 for our namespace.
        List<PartitionInfo> mockPartitionInfos = Lists.newArrayList(new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0]));
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // Create instance of a StateConsumer, we'll just use a dummy instance.
        PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);

        // When getState is called, return the following state
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(0))).thenReturn(lastCommittedOffset);

        // Call constructor injecting our mocks
        Consumer consumer = new Consumer(mockKafkaConsumer);
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), mockPersistenceAdapter, null);

        // For every partition returned by mockKafkaConsumer.partitionsFor(), we should subscribe to them via the mockKafkaConsumer.assign() call
        verify(mockKafkaConsumer, times(1)).assign(eq(Lists.newArrayList(partition0)));

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
        KafkaConsumer<byte[], byte[]> mockKafkaConsumer = mock(KafkaConsumer.class);

        // When we call partitionsFor(), We return partitions 0,1, and 2.
        List<PartitionInfo> mockPartitionInfos = Lists.newArrayList(
                new PartitionInfo(topicName, 0, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
                new PartitionInfo(topicName, 1, new Node(0, "localhost", 9092), new Node[0], new Node[0]),
                new PartitionInfo(topicName, 2, new Node(0, "localhost", 9092), new Node[0], new Node[0])
        );
        when(mockKafkaConsumer.partitionsFor(eq(topicName))).thenReturn(mockPartitionInfos);

        // Create instance of a StateConsumer, we'll just use a dummy instance.
        PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);

        // When getState is called, return the following state
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(partition0.partition()))).thenReturn(lastCommittedOffsetPartition0);
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(partition1.partition()))).thenReturn(lastCommittedOffsetPartition1);
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(partition2.partition()))).thenReturn(lastCommittedOffsetPartition2);

        // Call constructor injecting our mocks
        Consumer consumer = new Consumer(mockKafkaConsumer);

        // Now call open
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), mockPersistenceAdapter, null);

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
     * Verifies that when we call connect that it makes the appropriate calls
     * to ConsumerStateManager to initialize.
     *
     * This test has the ConsumerStateManager (a mock) return ConsumerState for every partition on the namespace.
     * We verify that our internal kafka client then knows to start reading from the previously saved consumer state
     * offsets
     *
     * We setup this test with 4 partitions in our namespace, 0 -> 3
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
        PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);

        // When getState is called, return the following state for partitions 0 and 2.
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(kafkaTopicPartition0.partition()))).thenReturn(lastCommittedOffsetPartition0);
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(kafkaTopicPartition1.partition()))).thenReturn(null);
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(kafkaTopicPartition2.partition()))).thenReturn(lastCommittedOffsetPartition2);
        when(mockPersistenceAdapter.retrieveConsumerState(eq(consumerId), eq(kafkaTopicPartition3.partition()))).thenReturn(null);

        // Define values returned for partitions without state
        when(mockKafkaConsumer.position(kafkaTopicPartition1)).thenReturn(earliestOffsetPartition1);
        when(mockKafkaConsumer.position(kafkaTopicPartition3)).thenReturn(earliestOffsetPartition3);

        // Call constructor injecting our mocks
        Consumer consumer = new Consumer(mockKafkaConsumer);

        // Now call open
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), mockPersistenceAdapter, null);

        // For every partition returned by mockKafkaConsumer.partitionsFor(), we should subscribe to them via the mockKafkaConsumer.assign() call
        verify(mockKafkaConsumer, times(1)).assign(eq(Lists.newArrayList(
            kafkaTopicPartition0,
            kafkaTopicPartition1,
            kafkaTopicPartition2,
            kafkaTopicPartition3
        )));

        // Since ConsumerStateManager has state for only 2 partitions, we should call seekToBeginning on partitions 1 and 3.
        verify(mockKafkaConsumer, times(1)).seekToBeginning(eq(Lists.newArrayList(
            kafkaTopicPartition1
        )));
        verify(mockKafkaConsumer, times(1)).seekToBeginning(eq(Lists.newArrayList(
            kafkaTopicPartition3
        )));

        // Verify we asked for the positions for 2 unknown state partitions
        verify(mockKafkaConsumer, times(1)).position(kafkaTopicPartition1);
        verify(mockKafkaConsumer, times(1)).position(kafkaTopicPartition3);

        // For the partitions with state, we should call seek on each partition
        InOrder inOrderVerification = inOrder(mockKafkaConsumer);
        inOrderVerification.verify(mockKafkaConsumer, times(1)).seek(eq(kafkaTopicPartition0), eq(expectedPartition0Offset));
        inOrderVerification.verify(mockKafkaConsumer, times(1)).seek(eq(kafkaTopicPartition2), eq(expectedPartition2Offset));

        // Never seeked for partitions without any state
        verify(mockKafkaConsumer, never()).seek(eq(kafkaTopicPartition1), anyLong());
        verify(mockKafkaConsumer, never()).seek(eq(kafkaTopicPartition3), anyLong());

        // Now validate the consumer state
        final ConsumerState resultingConsumerState = consumer.getCurrentState();

        assertNotNull("Should be non-null", resultingConsumerState);

        // State should have one entry
        assertEquals("Should have 4 entries", 4, resultingConsumerState.size());

        // Offsets should be set to what we expected.
        assertEquals("Expected value for partition0", expectedStateOffsetPartition0, (long) resultingConsumerState.getOffsetForNamespaceAndPartition(partition0));
        assertEquals("Expected value for partition1", expectedStateOffsetPartition1, (long) resultingConsumerState.getOffsetForNamespaceAndPartition(partition1));
        assertEquals("Expected value for partition2", expectedStateOffsetPartition2, (long) resultingConsumerState.getOffsetForNamespaceAndPartition(partition2));
        assertEquals("Expected value for partition2", expectedStateOffsetPartition3, (long) resultingConsumerState.getOffsetForNamespaceAndPartition(partition3));
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
        Consumer consumer = getDefaultConsumerInstanceAndOpen();

        // Ask the underlying consumer for our assigned partitions.
        Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate it
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain 1 entry", 1, assignedPartitions.size());
        assertTrue("Should contain our expected namespace/partition", assignedPartitions.contains(new ConsumerPartition(topicName, expectedPartitionId)));
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
        kafkaTestServer.createTopic(expectedTopicName, expectedNumberOfPartitions);

        // Create our consumer
        Consumer consumer = getDefaultConsumerInstanceAndOpen(expectedTopicName);

        // Ask the underlying consumer for our assigned partitions.
        Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate it
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain 5 entries", expectedNumberOfPartitions, assignedPartitions.size());
        for (int x=0; x<expectedNumberOfPartitions; x++) {
            assertTrue("Should contain our expected namespace/partition " + x, assignedPartitions.contains(new ConsumerPartition(expectedTopicName, x)));
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
        Consumer consumer = getDefaultConsumerInstanceAndOpen(topicName);

        // Grab persistence adapter instance.
        final PersistenceAdapter persistenceAdapter = consumer.getPersistenceAdapter();

        // Ask the underlying consumer for our assigned partitions.
        Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain 1 entries", 1, assignedPartitions.size());
        assertTrue("Should contain our expected namespace/partition 0", assignedPartitions.contains(expectedTopicPartition));

        // Now unsub from our namespace partition
        final boolean result = consumer.unsubscribeConsumerPartition(expectedTopicPartition);
        assertTrue("Should have returned true", result);

        // Now ask for assigned partitions, should have none
        // Ask the underlying consumer for our assigned partitions.
        assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertTrue("Should be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain 0 entries", 0, assignedPartitions.size());
        assertFalse("Should NOT contain our expected namespace/partition 0", assignedPartitions.contains(expectedTopicPartition));

        // Now we want to validate that removeConsumerState() removes all state, even for unassigned partitions.

        // Call flush and ensure we have persisted state on partition 0
        consumer.flushConsumerState();
        assertNotNull("Should have state persisted for this partition", persistenceAdapter.retrieveConsumerState(consumer.getConsumerId(), 0));

        // If we call removeConsumerState, it should remove all state from the persistence layer
        consumer.removeConsumerState();

        // Validate no more state persisted for partition 0
        assertNull("Should have null state", persistenceAdapter.retrieveConsumerState(consumer.getConsumerId(), 0));
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
        kafkaTestServer.createTopic(expectedTopicName, expectedNumberOfPartitions);

        // Create our consumer
        Consumer consumer = getDefaultConsumerInstanceAndOpen(expectedTopicName);

        // Grab persistence adapter instance.
        final PersistenceAdapter persistenceAdapter = consumer.getPersistenceAdapter();

        // Ask the underlying consumer for our assigned partitions.
        Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain entries", expectedNumberOfPartitions, assignedPartitions.size());
        for (int x=0; x<expectedNumberOfPartitions; x++) {
            assertTrue("Should contain our expected namespace/partition " + x, assignedPartitions.contains(new ConsumerPartition(expectedTopicName, x)));
        }

        // Now unsub from our namespace partition
        final int expectedRemovePartition = 2;
        final ConsumerPartition toRemoveTopicPartition = new ConsumerPartition(expectedTopicName, expectedRemovePartition);

        final boolean result = consumer.unsubscribeConsumerPartition(toRemoveTopicPartition);
        assertTrue("Should have returned true", result);

        // Now ask for assigned partitions, should have none
        // Ask the underlying consumer for our assigned partitions.
        assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should be not empty", assignedPartitions.isEmpty());
        assertEquals("Should contain entries", (expectedNumberOfPartitions - 1), assignedPartitions.size());
        assertFalse("Should NOT contain our removed namespace/partition 0", assignedPartitions.contains(toRemoveTopicPartition));

        // Attempt to remove the same topicPartitionAgain, it should return false
        final boolean result2 = consumer.unsubscribeConsumerPartition(toRemoveTopicPartition);
        assertFalse("Should return false the second time", result2);

        // Now remove another namespace/partition
        final int expectedRemovePartition2 = 4;
        final ConsumerPartition toRemoveTopicPartition2 = new ConsumerPartition(expectedTopicName, expectedRemovePartition2);

        final boolean result3 = consumer.unsubscribeConsumerPartition(toRemoveTopicPartition2);
        assertTrue("Should have returned true", result3);

        // Now ask for assigned partitions, should have none
        // Ask the underlying consumer for our assigned partitions.
        assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should be not empty", assignedPartitions.isEmpty());
        assertEquals("Should contain entries", (expectedNumberOfPartitions - 2), assignedPartitions.size());
        assertFalse("Should NOT contain our removed namespace/partition 1", assignedPartitions.contains(toRemoveTopicPartition));
        assertFalse("Should NOT contain our removed namespace/partition 2", assignedPartitions.contains(toRemoveTopicPartition2));

        // Now we want to validate that removeConsumerState() removes all state, even for unassigned partitions.

        // Call flush and ensure we have persisted state on all partitions
        consumer.flushConsumerState();
        for (int partitionId=0; partitionId<expectedNumberOfPartitions; partitionId++) {
            assertNotNull("Should have state persisted for this partition", persistenceAdapter.retrieveConsumerState(consumer.getConsumerId(), partitionId));
        }

        // If we call removeConsumerState, it should remove all state from the persistence layer
        consumer.removeConsumerState();

        // Validate we dont have state on any partitions now
        for (int partitionId=0; partitionId<expectedNumberOfPartitions; partitionId++) {
            assertNull("Should not have state persisted for this partition", persistenceAdapter.retrieveConsumerState(consumer.getConsumerId(), partitionId));
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
        Consumer consumer = getDefaultConsumerInstanceAndOpen();

        // Read from namespace, verify we get what we expect
        for (int x=0; x<numberOfRecordsToProduce; x++) {
            // Get the next record
            final Record foundRecord = consumer.nextRecord();

            // Compare to what we expected
            final ProducedKafkaRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);

            // Validate em
            validateRecordMatchesInput(expectedRecord, foundRecord);
        }

        // Next one should return null
        Record foundRecord = consumer.nextRecord();
        assertNull("Should have nothing new to consume and be null", foundRecord);

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
        Consumer consumer = getDefaultConsumerInstanceAndOpen();

        // Read from namespace, verify we get what we expect
        for (int x=0; x<numberOfRecordsToProduce; x++) {
            // Get next record from consumer
            final Record foundRecord = consumer.nextRecord();

            // Compare to what we expected
            ProducedKafkaRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);

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
        Consumer consumer = getDefaultConsumerInstanceAndOpen();

        // Read from namespace, verify we get what we expect
        List<Record> foundRecords = Lists.newArrayList();
        for (int x=0; x<numberOfRecordsToProduce; x++) {
            // Get next record from consumer
            final Record foundRecord = consumer.nextRecord();

            // Get the next produced record that we expect
            ProducedKafkaRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);

            // Validate we got what we expected
            validateRecordMatchesInput(expectedRecord, foundRecord);

            // Add to our found records
            foundRecords.add(foundRecord);
        }
        logger.info("Consumer State {}", consumer.flushConsumerState());

        // Verify state is still -1 (meaning it hasn't acked/completed ANY offsets yet)
        validateConsumerState(consumer.flushConsumerState(), partition0, -1L);

        // Now ack them one by one
        for (Record foundRecord : foundRecords) {
            consumer.commitOffset(foundRecord.getNamespace(), foundRecord.getPartition(), foundRecord.getOffset());
        }

        // Now validate state.
        ConsumerState consumerState = consumer.flushConsumerState();

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
        Consumer consumer = getDefaultConsumerInstanceAndOpen();

        // Read from namespace, verify we get what we expect
        List<Record> foundRecords = Lists.newArrayList();
        for (int x=0; x<numberOfRecordsToProduce; x++) {
            // Get next record from consumer
            final Record foundRecord = consumer.nextRecord();

            // Get the next produced record that we expect
            ProducedKafkaRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);

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
        Map<String, Object> config = getDefaultConfig(topicName);

        // Set deserializer instance to our null deserializer
        config.put(SidelineSpoutConfig.DESERIALIZER_CLASS, NullDeserializer.class.getName());

        // Create our Persistence Manager
        PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(Maps.newHashMap());

        // Create our consumer
        Consumer consumer = new Consumer();
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), persistenceAdapter, null);

        // Read from namespace, verify we get what we expect
        for (int x=0; x<numberOfRecordsToProduce; x++) {
            // Get the next record
            final Record foundRecord = consumer.nextRecord();

            // It should be null
            assertNull("Null Deserializer produced null return value", foundRecord);

            // Validate our consumer position should increase.
            validateConsumerState(consumer.flushConsumerState(), partition0, x);
        }

        // Next one should return null
        Record foundRecord = consumer.nextRecord();
        assertNull("Should have nothing new to consume and be null", foundRecord);

        // Close out consumer
        consumer.close();
    }

    /**
     * Produce 10 messages into a kafka topic: offsets [0-9]
     * Setup our SidelineConsumer such that its pre-existing state says to start at offset 4
     * Consume using the SidelineConsumer, verify we only get the last 5 messages back.
     */
    @Test
    public void testConsumerWithInitialStateToSkipMessages() {
        // Define how many records to produce
        final int numberOfRecordsToProduce = 10;

        // Produce entries to the namespace.
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(numberOfRecordsToProduce, 0);

        // Create a list of the records we expect to get back from the consumer, this should be the last 5 entries.
        List<ProducedKafkaRecord<byte[], byte[]>> expectedProducedRecords = producedRecords.subList(5,10);

        // Setup our config
        Map<String, Object> config = getDefaultConfig();

        // Create virtualSpoutId
        final VirtualSpoutIdentifier virtualSpoutIdentifier = getDefaultVSpoutId();

        // Create our Persistence Manager
        PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(Maps.newHashMap());

        // Create a state in which we have already acked the first 5 messages
        // 5 first msgs marked completed (0,1,2,3,4) = Committed Offset = 4.
        persistenceAdapter.persistConsumerState(virtualSpoutIdentifier.toString(), 0, 4L);

        // Create our consumer
        Consumer consumer = new Consumer();
        consumer.open(config, virtualSpoutIdentifier, getDefaultConsumerCohortDefinition(), persistenceAdapter, null);

        // Read from namespace, verify we get what we expect, we should only get the last 5 records.
        List<Record> consumedRecords = asyncConsumeMessages(consumer, 5);
        Iterator<ProducedKafkaRecord<byte[], byte[]>> expectedProducedRecordsIterator = expectedProducedRecords.iterator();
        for (Record foundRecord: consumedRecords) {
            // Get the produced record we expected to get back.
            ProducedKafkaRecord<byte[], byte[]> expectedRecord = expectedProducedRecordsIterator.next();

            // Validate we got what we expected
            validateRecordMatchesInput(expectedRecord, foundRecord);
        }

        // Additional calls to nextRecord() should return null
        for (int x=0; x<2; x++) {
            Record foundRecord = consumer.nextRecord();
            assertNull("Should have nothing new to consume and be null", foundRecord);
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
        kafkaTestServer.createTopic(topicName, expectedNumberOfPartitions);

        // Define our expected namespace/partitions
        final ConsumerPartition partition0 = new ConsumerPartition(topicName, 0);
        final ConsumerPartition partition1 = new ConsumerPartition(topicName, 1);

        // Create our consumer
        Consumer consumer = getDefaultConsumerInstanceAndOpen(topicName);

        // Ask the underlying consumer for our assigned partitions.
        Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain 2 entries", expectedNumberOfPartitions, assignedPartitions.size());
        assertTrue("Should contain our expected namespace/partition 0", assignedPartitions.contains(partition0));
        assertTrue("Should contain our expected namespace/partition 1", assignedPartitions.contains(partition1));

        // Now produce 5 msgs to each namespace (10 msgs total)
        final int expectedNumberOfMsgsPerPartition = 5;
        List<ProducedKafkaRecord<byte[], byte[]>> producedRecordsPartition0 = produceRecords(expectedNumberOfMsgsPerPartition, 0);
        List<ProducedKafkaRecord<byte[], byte[]>> producedRecordsPartition1 = produceRecords(expectedNumberOfMsgsPerPartition, 1);

        // Attempt to consume them
        // Read from namespace, verify we get what we expect
        int partition0Index = 0;
        int partition1Index = 0;
        for (int x=0; x<(expectedNumberOfMsgsPerPartition * 2); x++) {
            Record foundRecord = consumer.nextRecord();
            assertNotNull(foundRecord);

            // Determine which partition its from
            final int partitionSource = foundRecord.getPartition();
            assertTrue("Should be partition 0 or 1", partitionSource == 0 || partitionSource == 1);

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
        assertNull("Should have nothing new to consume and be null", foundRecord);

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
     * This is an integration test of multiple SidelineConsumers.
     * We stand up a topic with 4 partitions.
     * We then have a consumer size of 2.
     * We run the test once using consumerIndex 0
     *   - Verify we only consume from partitions 0 and 1
     * We run the test once using consumerIndex 1
     *   - Verify we only consume from partitions 2 and 3
     * @param consumerIndex What consumerIndex to run the test with.
     */
    @Test
    @UseDataProvider("providerOfConsumerIndexes")
    public void testConsumeWithConsumerGroupEvenNumberOfPartitions(final int consumerIndex) {
        final int numberOfMsgsPerPartition = 10;

        // Create a namespace with 4 partitions
        topicName = "testConsumeWithConsumerGroupEvenNumberOfPartitions" + Clock.systemUTC().millis();
        kafkaTestServer.createTopic(topicName, 4);

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
            expectedPartitions = Lists.newArrayList(partition0 , partition1);
        } else if (consumerIndex == 1) {
            // If we're consumerIndex 0, we expect partitionIds 2 or 3
            expectedPartitions = Lists.newArrayList(partition2 , partition3);
        } else {
            throw new RuntimeException("Invalid input to test");
        }

        // Setup our config
        final Map<String, Object> config = getDefaultConfig(topicName);

        // Adjust the config so that we have 2 consumers, and we are consumer index that was passed in.
        final ConsumerCohortDefinition consumerCohortDefinition = new ConsumerCohortDefinition(2, consumerIndex);

        // create our vspout id
        final VirtualSpoutIdentifier virtualSpoutIdentifier = new VirtualSpoutIdentifier("MyConsumerId");

        // Create our Persistence Manager
        PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(Maps.newHashMap());

        // Create our consumer
        Consumer consumer = new Consumer();
        consumer.open(config, virtualSpoutIdentifier, consumerCohortDefinition, persistenceAdapter, null);

        // Ask the underlying consumer for our assigned partitions.
        Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate we are assigned 2 partitions, and its partitionIds 0 and 1
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should be assigned 2 partitions", 2, assignedPartitions.size());
        assertTrue("Should contain first expected partition", assignedPartitions.contains(expectedPartitions.get(0)));
        assertTrue("Should contain 2nd partition", assignedPartitions.contains(expectedPartitions.get(1)));

        // Attempt to consume 21 records
        // Read from topic, verify we get what we expect
        for (int x=0; x<(numberOfMsgsPerPartition * 2) + 1; x++) {
            Record foundRecord = consumer.nextRecord();

            // Validate its from partition 0 or 1
            final int foundPartitionId = foundRecord.getPartition();
            assertTrue("Should be from one of our expected partitions", foundPartitionId == expectedPartitions.get(0).partition() || foundPartitionId == expectedPartitions.get(1).partition());

            // Lets ack the tuple as we go
            consumer.commitOffset(foundRecord.getNamespace(), foundRecord.getPartition(), foundRecord.getOffset());
        }

        // Validate next calls all return null, as there is nothing left in those topics on partitions 0 and 1 to consume.
        for (int x=0; x<2; x++) {
            Record foundRecord = consumer.nextRecord();
            assertNull("Should have nothing new to consume and be null", foundRecord);
        }

        // Now lets flush state
        final ConsumerState consumerState = consumer.flushConsumerState();

        // validate the state only has data for our partitions
        assertNotNull("Should not be null", consumerState);
        assertEquals("Should only have 2 entries", 2, consumerState.size());
        assertTrue("Should contain for first expected partition", consumerState.containsKey(expectedPartitions.get(0)));
        assertTrue("Should contain for 2nd expected partition", consumerState.containsKey(expectedPartitions.get(1)));
        assertEquals("Offset stored should be 9 on first expected partition", (Long) 9L, consumerState.getOffsetForNamespaceAndPartition(expectedPartitions.get(0)));
        assertEquals("Offset stored should be 10 on 2nd expected partition", (Long) 10L, consumerState.getOffsetForNamespaceAndPartition(expectedPartitions.get(1)));

        // And double check w/ persistence manager directly
        final Long partition0Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 0);
        final Long partition1Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 1);
        final Long partition2Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 2);
        final Long partition3Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 3);

        if (consumerIndex == 0) {
            assertNotNull("Partition0 offset should be not null", partition0Offset);
            assertNotNull("Partition1 offset should be not null", partition1Offset);
            assertNull("Partition2 offset should be null", partition2Offset);
            assertNull("Partition3 offset should be null", partition3Offset);

            assertEquals("Offset should be 9", (Long) 9L, partition0Offset);
            assertEquals("Offset should be 10", (Long) 10L, partition1Offset);
        } else {
            assertNotNull("Partition2 offset should be not null", partition2Offset);
            assertNotNull("Partition3 offset should be not null", partition3Offset);
            assertNull("Partition0 offset should be null", partition0Offset);
            assertNull("Partition1 offset should be null", partition1Offset);

            assertEquals("Offset should be 9", (Long) 9L, partition2Offset);
            assertEquals("Offset should be 10", (Long) 10L, partition3Offset);
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
    @Test
    @UseDataProvider("providerOfConsumerIndexes")
    public void testConsumeWithConsumerGroupOddNumberOfPartitions(final int consumerIndex) {
        final int numberOfMsgsPerPartition = 10;

        // Create a namespace with 4 partitions
        topicName = "testConsumeWithConsumerGroupOddNumberOfPartitions" + Clock.systemUTC().millis();
        kafkaTestServer.createTopic(topicName, 5);

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
            expectedPartitions = Lists.newArrayList(partition0 , partition1, partition2);

            // We expect to get out 31 records
            expectedRecordsToConsume = 31;
        } else if (consumerIndex == 1) {
            // If we're consumerIndex 0, we expect partitionIds 3 or 4
            expectedPartitions = Lists.newArrayList(partition3 , partition4);

            // We expect to get out 21 records
            expectedRecordsToConsume = 21;
        } else {
            throw new RuntimeException("Invalid input to test");
        }

        // Setup our config
        final Map<String, Object> config = getDefaultConfig(topicName);

        // Adjust the config so that we have 2 consumers, and we are consumer index that was passed in.
        final ConsumerCohortDefinition consumerCohortDefinition = new ConsumerCohortDefinition(2, consumerIndex);

        // create our vspout id
        final VirtualSpoutIdentifier virtualSpoutIdentifier = new VirtualSpoutIdentifier("MyConsumerId");

        // Create our Persistence Manager
        PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(Maps.newHashMap());

        // Create our consumer
        Consumer consumer = new Consumer();
        consumer.open(config, virtualSpoutIdentifier, consumerCohortDefinition, persistenceAdapter, null);

        // Ask the underlying consumer for our assigned partitions.
        Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate we are assigned 2 partitions, and its partitionIds 0 and 1
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should be assigned correct number of partitions", expectedPartitions.size(), assignedPartitions.size());
        for (ConsumerPartition expectedTopicPartition : expectedPartitions) {
            assertTrue("Should contain expected partition", assignedPartitions.contains(new ConsumerPartition(expectedTopicPartition.namespace(), expectedTopicPartition.partition())));
        }

        // Attempt to consume records
        // Read from namespace, verify we get what we expect
        for (int x=0; x<expectedRecordsToConsume; x++) {
            Record foundRecord = consumer.nextRecord();
            assertNotNull(foundRecord);

            // Validate its from a partition we expect
            final int foundPartitionId = foundRecord.getPartition();
            assertTrue("Should be from one of our expected partitions", expectedPartitions.contains(new ConsumerPartition(topicName, foundPartitionId)));

            // Lets ack the tuple as we go
            consumer.commitOffset(foundRecord.getNamespace(), foundRecord.getPartition(), foundRecord.getOffset());
        }

        // Validate next calls all return null, as there is nothing left in those topics on partitions 0 and 1 to consume.
        for (int x=0; x<2; x++) {
            Record foundRecord = consumer.nextRecord();
            assertNull("Should have nothing new to consume and be null", foundRecord);
        }

        // Now lets flush state
        final ConsumerState consumerState = consumer.flushConsumerState();

        // validate the state only has data for our partitions
        assertNotNull("Should not be null", consumerState);
        assertEquals("Should only have correct number of entries", expectedPartitions.size(), consumerState.size());

        for (ConsumerPartition expectedPartition: expectedPartitions) {
            assertTrue("Should contain for first expected partition", consumerState.containsKey(expectedPartition));

            if (expectedPartition.partition() % 2 == 0) {
                assertEquals("Offset stored should be 9 on even partitions", (Long) 9L, consumerState.getOffsetForNamespaceAndPartition(expectedPartition));
            } else {
                assertEquals("Offset stored should be 10 on odd partitions", (Long) 10L, consumerState.getOffsetForNamespaceAndPartition(expectedPartition));
            }
        }

        // And double check w/ persistence manager directly
        final Long partition0Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 0);
        final Long partition1Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 1);
        final Long partition2Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 2);
        final Long partition3Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 3);
        final Long partition4Offset = persistenceAdapter.retrieveConsumerState(virtualSpoutIdentifier.toString(), 4);

        if (consumerIndex == 0) {
            assertNotNull("Partition0 offset should be not null", partition0Offset);
            assertNotNull("Partition1 offset should be not null", partition1Offset);
            assertNotNull("Partition2 offset should be not null", partition2Offset);
            assertNull("Partition3 offset should be null", partition3Offset);
            assertNull("Partition4 offset should be null", partition4Offset);

            assertEquals("Offset should be 9", (Long) 9L, partition0Offset);
            assertEquals("Offset should be 10", (Long) 10L, partition1Offset);
            assertEquals("Offset should be 9", (Long) 9L, partition2Offset);
        } else {
            assertNotNull("Partition3 offset should be not null", partition3Offset);
            assertNotNull("Partition4 offset should be not null", partition4Offset);
            assertNull("Partition0 offset should be null", partition0Offset);
            assertNull("Partition1 offset should be null", partition1Offset);
            assertNull("Partition2 offset should be null", partition2Offset);

            assertEquals("Offset should be 10", (Long) 10L, partition3Offset);
            assertEquals("Offset should be 9", (Long) 9L, partition4Offset);
        }

        // Close it.
        consumer.close();
    }

    /**
     * Provides consumer indexes 0 and 1.
     */
    @DataProvider
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
     * 6. Attempt to consume more msgs, verify none are found.
     */
    @Test
    public void testConsumeFromTopicAfterUnsubscribingFromSinglePartition() {
        // Define our expected namespace/partition
        final ConsumerPartition expectedTopicPartition = new ConsumerPartition(topicName, 0);

        // Create our consumer
        Consumer consumer = getDefaultConsumerInstanceAndOpen(topicName);

        // Ask the underlying consumer for our assigned partitions.
        Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain 1 entries", 1, assignedPartitions.size());
        assertTrue("Should contain our expected namespace/partition 0", assignedPartitions.contains(expectedTopicPartition));

        // Now produce 5 msgs
        final int expectedNumberOfMsgs = 5;
        List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(expectedNumberOfMsgs, 0);

        // Attempt to consume them
        // Read from namespace, verify we get what we expect
        for (int x=0; x<expectedNumberOfMsgs; x++) {
            Record foundRecord = consumer.nextRecord();
            ProducedKafkaRecord<byte[], byte[]> expectedRecord = producedRecords.get(x);
            validateRecordMatchesInput(expectedRecord, foundRecord);
        }

        // Next one should return null
        Record foundRecord = consumer.nextRecord();
        assertNull("Should have nothing new to consume and be null", foundRecord);

        // Now produce 5 more msgs
        producedRecords = produceRecords(expectedNumberOfMsgs, 0);

        // Now unsub from the partition
        final boolean result = consumer.unsubscribeConsumerPartition(expectedTopicPartition);
        assertTrue("Should be true", result);

        // Attempt to consume, but nothing should be returned, because we unsubscribed.
        for (int x=0; x<expectedNumberOfMsgs; x++) {
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
        kafkaTestServer.createTopic(topicName, expectedNumberOfPartitions);

        // Define our expected namespace/partitions
        final ConsumerPartition expectedTopicPartition0 = new ConsumerPartition(topicName, 0);
        final ConsumerPartition expectedTopicPartition1 = new ConsumerPartition(topicName, 1);

        // Create our consumer
        Consumer consumer = getDefaultConsumerInstanceAndOpen(topicName);

        // Ask the underlying consumer for our assigned partitions.
        Set<ConsumerPartition> assignedPartitions = consumer.getAssignedPartitions();
        logger.info("Assigned partitions: {}", assignedPartitions);

        // Validate setup
        assertNotNull("Should be non-null", assignedPartitions);
        assertFalse("Should not be empty", assignedPartitions.isEmpty());
        assertEquals("Should contain 2 entries", expectedNumberOfPartitions, assignedPartitions.size());
        assertTrue("Should contain our expected namespace/partition 0", assignedPartitions.contains(expectedTopicPartition0));
        assertTrue("Should contain our expected namespace/partition 1", assignedPartitions.contains(expectedTopicPartition1));

        // Now produce 5 msgs to each namespace (10 msgs total)
        final int expectedNumberOfMsgsPerPartition = 5;
        List<ProducedKafkaRecord<byte[], byte[]>> producedRecordsPartition0 = produceRecords(expectedNumberOfMsgsPerPartition, 0);
        List<ProducedKafkaRecord<byte[], byte[]>> producedRecordsPartition1 = produceRecords(expectedNumberOfMsgsPerPartition, 1);

        // Attempt to consume them
        // Read from namespace, verify we get what we expect
        int partition0Index = 0;
        int partition1Index = 0;
        for (int x=0; x<(expectedNumberOfMsgsPerPartition * 2); x++) {
            Record foundRecord = consumer.nextRecord();
            assertNotNull(foundRecord);

            // Determine which partition its from
            final int partitionSource = foundRecord.getPartition();
            assertTrue("Should be partition 0 or 1", partitionSource == 0 || partitionSource == 1);

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
        assertNull("Should have nothing new to consume and be null", foundRecord);

        // Now produce 5 more msgs into each partition
        producedRecordsPartition0 = produceRecords(expectedNumberOfMsgsPerPartition, 0);
        producedRecordsPartition1 = produceRecords(expectedNumberOfMsgsPerPartition, 1);

        // Now unsub from the partition 0
        final boolean result = consumer.unsubscribeConsumerPartition(expectedTopicPartition0);
        assertTrue("Should be true", result);

        // Attempt to consume, but nothing should be returned from partition 0, only partition 1
        for (int x=0; x<expectedNumberOfMsgsPerPartition; x++) {
            foundRecord = consumer.nextRecord();
            assertNotNull(foundRecord);

            // Determine which partition its from, should be only 1
            assertEquals("Should be partition 1", 1, foundRecord.getPartition());

            // Validate it
            ProducedKafkaRecord<byte[], byte[]> expectedRecord = producedRecordsPartition1.get(x);

            // Compare to what we expected
            validateRecordMatchesInput(expectedRecord, foundRecord);
        }

        // Next one should return null
        foundRecord = consumer.nextRecord();
        assertNull("Should have nothing new to consume and be null", foundRecord);

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
     *   partition 0 -> messages 2,3      (because we started at offset 2)
     *   partition 1 -> messages 0,1,2,3  (because we got reset to earliest)
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
        final int numberOfExpectedMessages = 6;

        // Define our namespace/partitions
        final ConsumerPartition topicPartition0 = new ConsumerPartition(topicName, 0);
        final ConsumerPartition topicPartition1 = new ConsumerPartition(topicName, 1);

        // Define starting offsets for partitions
        final long partition0StartingOffset = 1L;
        final long partition1StartingOffset = 20L;

        // Create our multi-partition namespace.
        kafkaTestServer.createTopic(topicName, numberOfPartitions);

        // Produce messages into both topics
        produceRecords(numberOfMsgsPerPartition, 0);
        produceRecords(numberOfMsgsPerPartition, 1);

        // Setup our config set to reset to none
        // We should handle this internally now.
        Map<String, Object> config = getDefaultConfig(topicName);

        // Create our Persistence Manager
        PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(Maps.newHashMap());

        // Create & persist the starting state for our test
        // Partition 0 has starting offset = 1
        persistenceAdapter.persistConsumerState("MyConsumerId", 0, partition0StartingOffset);

        // Partition 1 has starting offset = 21, which is invalid.  If our sideline consumer is configured properly,
        // it should reset to earliest, meaning offset 0 in this case.
        persistenceAdapter.persistConsumerState("MyConsumerId", 1, partition1StartingOffset);

        // Create our consumer
        Consumer consumer = new Consumer();
        consumer.open(config, getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), persistenceAdapter, null);

        // Validate PartitionOffsetManager is correctly setup
        ConsumerState consumerState = consumer.getCurrentState();
        assertEquals("Partition 0's last committed offset should be its starting offset", (Long) partition0StartingOffset, consumerState.getOffsetForNamespaceAndPartition(topicPartition0));
        assertEquals("Partition 1's last committed offset should be its starting offset", (Long) partition1StartingOffset, consumerState.getOffsetForNamespaceAndPartition(topicPartition1));

        // Define the values we expect to get
        final Set<String> expectedValues = Sets.newHashSet(
            // Partition 0 should not skip any messages!
            "partition0-offset2", "partition0-offset3",

            // Partition 1 should get reset to offset 0
            "partition1-offset0", "partition1-offset1", "partition1-offset2", "partition1-offset3"
        );

        List<Record> records = Lists.newArrayList();
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
        assertEquals("Should have 6 messages from kafka", numberOfExpectedMessages, records.size());
        assertTrue("Expected set should now be empty, we found everything", expectedValues.isEmpty());

        // Call nextRecord 2 more times, both should be null
        for (int x=0; x<2; x++) {
            assertNull("Should be null", consumer.nextRecord());
        }

        // Validate PartitionOffsetManager is correctly setup
        // We have not acked anything,
        consumerState = consumer.getCurrentState();
        assertEquals("Partition 0's last committed offset should still be its starting offset", (Long) partition0StartingOffset, consumerState.getOffsetForNamespaceAndPartition(topicPartition0));

        // This is -1 because the original offset we asked for was invalid, so it got set to (earliest offset - 1), or for us (0 - 1) => -1
        assertEquals("Partition 1's last committed offset should be reset to earliest, or -1 in our case", (Long)(-1L), consumerState.getOffsetForNamespaceAndPartition(topicPartition1));
    }

    /**
     * Helper method
     * @param consumerState - the consumer state we want to validate
     * @param topicPartition - the namespace/partition we want to validate
     * @param expectedOffset - the offset we expect
     */
    private void validateConsumerState(ConsumerState consumerState, ConsumerPartition topicPartition, long expectedOffset) {
        final long actualOffset = consumerState.getOffsetForNamespaceAndPartition(topicPartition);
        assertEquals("Expected offset", expectedOffset, actualOffset);
    }

    /**
     * helper method to produce records into kafka.
     */
    private List<ProducedKafkaRecord<byte[], byte[]>> produceRecords(final int numberOfRecords, final int partitionId) {
        KafkaTestUtils kafkaTestUtils = new KafkaTestUtils(kafkaTestServer);
        return kafkaTestUtils.produceRecords(numberOfRecords, topicName, partitionId);
    }

    /**
     * Utility to validate the records consumed from kafka match what was produced into kafka.
     * @param expectedRecord Records produced to kafka
     * @param foundRecord Records read from kafka
     */
    private void validateRecordMatchesInput(ProducedKafkaRecord<byte[], byte[]> expectedRecord, Record foundRecord) {
        assertNotNull(foundRecord);

        // Get values from the generated Tuple
        final String key = (String) foundRecord.getValues().get(0);
        final String value = (String) foundRecord.getValues().get(1);

        assertEquals("Found expected key",  new String(expectedRecord.getKey(), Charsets.UTF_8), key);
        assertEquals("Found expected value", new String(expectedRecord.getValue(), Charsets.UTF_8), value);
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
        defaultConfig.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList(kafkaTestServer.getKafkaServer().serverConfig().advertisedHostName() + ":" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort()));
        defaultConfig.put(SidelineSpoutConfig.KAFKA_TOPIC, topicName);
        defaultConfig.put(SidelineSpoutConfig.CONSUMER_ID_PREFIX, "TestPrefix");
        defaultConfig.put(SidelineSpoutConfig.PERSISTENCE_ZK_ROOT, "/sideline-spout-test");
        defaultConfig.put(SidelineSpoutConfig.PERSISTENCE_ZK_SERVERS, Lists.newArrayList("localhost:" + kafkaTestServer.getZkServer().getPort()));
        defaultConfig.put(SidelineSpoutConfig.PERSISTENCE_ADAPTER_CLASS, "com.salesforce.storm.spout.sideline.persistence.ZookeeperPersistenceAdapter");
        defaultConfig.put(SidelineSpoutConfig.DESERIALIZER_CLASS, Utf8StringDeserializer.class.getName());

        return SidelineSpoutConfig.setDefaults(defaultConfig);
    }

    /**
     * Utility method to generate a default ConsumerCohortDefinition instance.
     */
    private ConsumerCohortDefinition getDefaultConsumerCohortDefinition() {
        return new ConsumerCohortDefinition(1, 0);
    }

    /**
     * Utility method to generate a default VirtualSpoutIdentifier instance.
     */
    private VirtualSpoutIdentifier getDefaultVSpoutId() {
        return new VirtualSpoutIdentifier("MyConsumerId");
    }

    /**
     * Utility method to generate a defaultly configured and opened Consumer instance.
     */
    private Consumer getDefaultConsumerInstanceAndOpen(final String topicName) {
        // Create our Persistence Manager
        PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(Maps.newHashMap());

        // Create our consumer
        Consumer consumer = new Consumer();
        consumer.open(getDefaultConfig(topicName), getDefaultVSpoutId(), getDefaultConsumerCohortDefinition(), persistenceAdapter, null);

        return consumer;
    }

    /**
     * Utility method to generate a defaultly configured and opened Consumer instance.
     */
    private Consumer getDefaultConsumerInstanceAndOpen() {
        return getDefaultConsumerInstanceAndOpen(topicName);
    }

    private List<Record> asyncConsumeMessages(final Consumer consumer, final int numberOfMessagesToConsume) {
        List<Record> consumedMessages = Lists.newArrayList();

        await()
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> {
                Record nextRecord = consumer.nextRecord();
                if (nextRecord != null) {
                    consumedMessages.add(nextRecord);
                }
                return consumedMessages.size();
            }, equalTo(numberOfMessagesToConsume));
        return consumedMessages;
    }
}