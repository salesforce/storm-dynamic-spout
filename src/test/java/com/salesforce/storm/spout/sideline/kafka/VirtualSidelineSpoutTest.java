package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.FactoryManager;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.filter.StaticMessageFilter;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Deserializer;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Utf8StringDeserializer;
import com.salesforce.storm.spout.sideline.kafka.retryManagers.NeverRetryManager;
import com.salesforce.storm.spout.sideline.kafka.retryManagers.RetryManager;
import com.salesforce.storm.spout.sideline.metrics.LogRecorder;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.mocks.MockTopologyContext;
import com.salesforce.storm.spout.sideline.persistence.PersistenceManager;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.shade.com.google.common.base.Charsets;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(DataProviderRunner.class)
public class VirtualSidelineSpoutTest {

    /**
     * By default, no exceptions should be thrown.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Verify that constructor args get set appropriately.
     */
    @Test
    public void testConstructor() {
        // Create inputs
        final Map expectedTopologyConfig = Maps.newHashMap();
        expectedTopologyConfig.put("Key1", "Value1");
        expectedTopologyConfig.put("Key2", "Value2");
        expectedTopologyConfig.put("Key3", "Value3");

        // Create a mock topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a factory manager
        final FactoryManager factoryManager = new FactoryManager(expectedTopologyConfig);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(expectedTopologyConfig, mockTopologyContext, factoryManager, new LogRecorder());

        // Verify things got set
        assertNotNull("TopologyConfig should be non-null", virtualSidelineSpout.getTopologyConfig());
        assertNotNull("TopologyContext should be non-null", virtualSidelineSpout.getTopologyContext());

        // Verify the config is correct (and not some empty map)
        assertEquals("Should have correct number of entries", expectedTopologyConfig.size(), virtualSidelineSpout.getTopologyConfig().size());
        assertEquals("Should have correct entries", expectedTopologyConfig, virtualSidelineSpout.getTopologyConfig());

        // Verify factory manager set
        assertNotNull("Should have non-null factory manager", virtualSidelineSpout.getFactoryManager());
        assertEquals("Should be our instance passed in", factoryManager, virtualSidelineSpout.getFactoryManager());

        // Verify the config is immutable and throws exception when you try to modify it
        expectedException.expect(UnsupportedOperationException.class);
        virtualSidelineSpout.getTopologyConfig().put("MyKey", "MyValue");
    }

    /**
     * Verify that getTopologyConfigItem() works as expected
     */
    @Test
    public void testGetTopologyConfigItem() {
        // Create inputs
        final Map expectedTopologyConfig = Maps.newHashMap();
        expectedTopologyConfig.put("Key1", "Value1");
        expectedTopologyConfig.put("Key2", "Value2");
        expectedTopologyConfig.put("Key3", "Value3");

        // Create a mock topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a factory manager
        final FactoryManager factoryManager = new FactoryManager(expectedTopologyConfig);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(expectedTopologyConfig, mockTopologyContext, factoryManager, new LogRecorder());

        // Verify things got set
        assertNotNull("TopologyConfig should be non-null", virtualSidelineSpout.getTopologyConfig());

        // Verify the config is correct (and not some empty map)
        assertEquals("Should have correct number of entries", expectedTopologyConfig.size(), virtualSidelineSpout.getTopologyConfig().size());
        assertEquals("Should have correct entries", expectedTopologyConfig, virtualSidelineSpout.getTopologyConfig());

        // Check each item
        assertEquals("Value1", virtualSidelineSpout.getTopologyConfigItem("Key1"));
        assertEquals("Value2", virtualSidelineSpout.getTopologyConfigItem("Key2"));
        assertEquals("Value3", virtualSidelineSpout.getTopologyConfigItem("Key3"));

        // Check a random key that doesn't exist
        assertNull(virtualSidelineSpout.getTopologyConfigItem("Random Key"));
    }

    /**
     * Test setter and getter
     * Note - Setter may go away in liu of being set by the topologyConfig.  getVirtualSpoutId() should remain tho.
     */
    @Test
    public void testSetAndGetConsumerId() {
        // Define input
        final String expectedConsumerId = "myConsumerId";

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(Maps.newHashMap(), new MockTopologyContext(), new FactoryManager(Maps.newHashMap()), new LogRecorder());

        // Set it
        virtualSidelineSpout.setVirtualSpoutId(expectedConsumerId);

        // Verify it
        assertEquals("Got expected consumer id", expectedConsumerId, virtualSidelineSpout.getVirtualSpoutId());
    }

    /**
     * Test setter and getter
     */
    @Test
    public void testSetAndGetSidelineRequestId() {
        // Define input
        final SidelineRequestIdentifier expectedId = new SidelineRequestIdentifier();

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(Maps.newHashMap(), new MockTopologyContext(), new FactoryManager(Maps.newHashMap()), new LogRecorder());

        // Defaults null
        assertNull("should be null", virtualSidelineSpout.getSidelineRequestIdentifier());

        // Set it
        virtualSidelineSpout.setSidelineRequestIdentifier(expectedId);

        // Verify it
        assertEquals("Got expected requext id", expectedId, virtualSidelineSpout.getSidelineRequestIdentifier());
    }

    /**
     * Test setter and getter.
     */
    @Test
    public void testSetAndGetStopRequested() {
        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(Maps.newHashMap(), new MockTopologyContext(), new FactoryManager(Maps.newHashMap()), new LogRecorder());

        // Should default to false
        assertFalse("Should default to false", virtualSidelineSpout.isStopRequested());

        // Set to true
        virtualSidelineSpout.requestStop();
        assertTrue("Should be true", virtualSidelineSpout.isStopRequested());
    }

    /**
     * Calling open() more than once should throw an exception.
     */
    @Test
    public void testCallingOpenTwiceThrowsException() {
        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create mock topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        final List<PartitionInfo> partitions = Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}));

        // Create a mock SidelineConsumer
        final SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(partitions);

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create MetricsRecorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create spout
        final VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, mockTopologyContext, factoryManager, metricsRecorder, mockSidelineConsumer, null, null);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");

        // Call it once.
        virtualSidelineSpout.open();

        // Validate that open() on SidelineConsumer is called once.
        verify(mockSidelineConsumer, times(1)).open(null, partitions);

        // Set expected exception
        expectedException.expect(IllegalStateException.class);
        virtualSidelineSpout.open();
    }

    /**
     * Validate that Open behaves like we expect.
     */
    @Test
    public void testOpen() {
        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create mock topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        final List<PartitionInfo> partitions = Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}));

        // Create a mock SidelineConsumer
        final SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(partitions);

        // Create a mock Deserializer
        Deserializer mockDeserializer = mock(Deserializer.class);
        RetryManager mockRetryManager = mock(RetryManager.class);

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(mockDeserializer, mockRetryManager);

        // Create MetricsRecorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create spout
        final VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, mockTopologyContext, mockFactoryManager, metricsRecorder, mockSidelineConsumer, null, null);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");

        // Call open
        virtualSidelineSpout.open();

        // Validate that we asked factory manager for a deserializer
        verify(mockFactoryManager, times(1)).createNewDeserializerInstance();

        // Validate that we asked factory manager for a failed msg retry manager
        verify(mockFactoryManager, times(1)).createNewFailedMsgRetryManagerInstance();

        // Validate we called open on the RetryManager
        verify(mockRetryManager, times(1)).open(topologyConfig);

        // Validate that open() on SidelineConsumer is called once.
        verify(mockSidelineConsumer, times(1)).open(null, partitions);
    }

    /**
     * Tests when you call nextTuple() and the underlying consumer.nextRecord() returns null,
     * then nextTuple() should also return null.
     */
    @Test
    public void testNextTupleWhenConsumerReturnsNull() {
        // Define some inputs
        final ConsumerRecord<byte[], byte[]> expectedConsumerRecord = null;

        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        final SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockSidelineConsumer.nextRecord()).thenReturn(expectedConsumerRecord);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, mockTopologyContext, factoryManager, metricsRecorder, mockSidelineConsumer, null, null);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.open();

        // Call nextTuple()
        KafkaMessage result = virtualSidelineSpout.nextTuple();

        // Verify its null
        assertNull("Should be null",  result);

        // Verify ack is never called on underlying mock sideline consumer
        verify(mockSidelineConsumer, never()).commitOffset(any(), anyLong());
        verify(mockSidelineConsumer, never()).commitOffset(any());
    }

    /**
     * Tests what happens when you call nextTuple(), and the underlying deserializer fails to
     * deserialize (returns null), then nextTuple() should return null.
     */
    @Test
    public void testNextTupleWhenSerializerFailsToDeserialize() {
        // Define a deserializer that always returns null
        final Deserializer nullDeserializer = new Deserializer() {
            @Override
            public Values deserialize(String topic, int partition, long offset, byte[] key, byte[] value) {
                return null;
            }

            @Override
            public Fields getOutputFields() {
                return new Fields();
            }
        };

        // Define some inputs
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 3;
        final long expectedOffset = 434323L;
        final String expectedKey = "MyKey";
        final String expectedValue = "MyValue";
        final byte[] expectedKeyBytes = expectedKey.getBytes(Charsets.UTF_8);
        final byte[] expectedValueBytes = expectedValue.getBytes(Charsets.UTF_8);
        final ConsumerRecord<byte[], byte[]> expectedConsumerRecord = new ConsumerRecord<>(expectedTopic, expectedPartition, expectedOffset, expectedKeyBytes, expectedValueBytes);

        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(nullDeserializer, null);

        // Create metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create a mock SidelineConsumer
        final SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // TODO: Validate metric collection here
        when(mockSidelineConsumer.getCurrentState()).thenReturn(ConsumerState.builder().build());

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockSidelineConsumer.nextRecord()).thenReturn(expectedConsumerRecord);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, mockTopologyContext, mockFactoryManager, metricsRecorder, mockSidelineConsumer, null, null);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.open();

        // Call nextTuple()
        KafkaMessage result = virtualSidelineSpout.nextTuple();

        // Verify its null
        assertNull("Should be null",  result);

        // Verify ack was called on the tuple
        verify(mockSidelineConsumer, times(1)).commitOffset(new TopicPartition(expectedTopic, expectedPartition), expectedOffset);
    }

    /**
     * Validates what happens when a message is pulled from the underlying kafka consumer, but it is filtered
     * out by the filter chain.  nextTuple() should return null.
     */
    @Test
    public void testNextTupleReturnsNullWhenFiltered() {
        // Define some inputs
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 3;
        final long expectedOffset = 434323L;
        final String expectedConsumerId = "MyConsumerId";
        final String expectedKey = "MyKey";
        final String expectedValue = "MyValue";
        final byte[] expectedKeyBytes = expectedKey.getBytes(Charsets.UTF_8);
        final byte[] expectedValueBytes = expectedValue.getBytes(Charsets.UTF_8);
        final ConsumerRecord<byte[], byte[]> expectedConsumerRecord = new ConsumerRecord<>(expectedTopic, expectedPartition, expectedOffset, expectedKeyBytes, expectedValueBytes);

        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create a mock SidelineConsumer
        final SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // TODO: Validate metric collection here
        when(mockSidelineConsumer.getCurrentState()).thenReturn(ConsumerState.builder().build());

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockSidelineConsumer.nextRecord()).thenReturn(expectedConsumerRecord);

        final StaticMessageFilter filterStep = new StaticMessageFilter();

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                factoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, null
        );
        virtualSidelineSpout.getFilterChain().addStep(new SidelineRequestIdentifier(), filterStep);
        virtualSidelineSpout.setVirtualSpoutId(expectedConsumerId);
        virtualSidelineSpout.open();

        // Call nextTuple()
        KafkaMessage result = virtualSidelineSpout.nextTuple();

        // Check result
        assertNull("Should be null", result);

        // Verify ack was called on the tuple
        verify(mockSidelineConsumer, times(1)).commitOffset(new TopicPartition(expectedTopic, expectedPartition), expectedOffset);
    }

    /**
     * Validate what happens if everything works as expected, its deserialized properly, its not filtered.
     */
    @Test
    public void testNextTuple() {
        // Define some inputs
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 3;
        final long expectedOffset = 434323L;
        final String expectedConsumerId = "MyConsumerId";
        final String expectedKey = "MyKey";
        final String expectedValue = "MyValue";
        final byte[] expectedKeyBytes = expectedKey.getBytes(Charsets.UTF_8);
        final byte[] expectedValueBytes = expectedValue.getBytes(Charsets.UTF_8);
        final ConsumerRecord<byte[], byte[]> expectedConsumerRecord = new ConsumerRecord<>(expectedTopic, expectedPartition, expectedOffset, expectedKeyBytes, expectedValueBytes);

        // Define expected result
        final KafkaMessage expectedKafkaMessage = new KafkaMessage(new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId), new Values(expectedKey, expectedValue));

        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockSidelineConsumer.nextRecord()).thenReturn(expectedConsumerRecord);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                factoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, null);
        virtualSidelineSpout.setVirtualSpoutId(expectedConsumerId);
        virtualSidelineSpout.open();

        // Call nextTuple()
        KafkaMessage result = virtualSidelineSpout.nextTuple();

        // Check result
        assertNotNull("Should not be null", result);

        // Validate it
        assertEquals("Found expected topic", expectedTopic, result.getTopic());
        assertEquals("Found expected partition", expectedPartition, result.getPartition());
        assertEquals("Found expected offset", expectedOffset, result.getOffset());
        assertEquals("Found expected values", new Values(expectedKey, expectedValue), result.getValues());
        assertEquals("Got expected KafkaMessage", expectedKafkaMessage, result);
    }

    /**
     * 1. publish a bunch of messages to a topic with a single partition.
     * 2. create a VirtualSideLineSpout where we explicitly define an ending offset less than the total msgs published
     * 3. Consume from the Spout (call nextTuple())
     * 4. Ensure that we stop getting tuples back after we exceed the ending state offset.
     */
    @Test
    public void testNextTupleIgnoresMessagesThatHaveExceededEndingStatePositionSinglePartition() {
        // Define some variables
        final long endingOffset = 4444L;
        final int partition = 4;
        final String topic = "MyTopic";

        // Define before offset
        final long beforeOffset = (endingOffset - 100);
        final long afterOffset = (endingOffset + 100);

        // Create a ConsumerRecord who's offset is BEFORE the ending offset, this should pass
        final ConsumerRecord<byte[], byte[]> consumerRecordBeforeEnd = new ConsumerRecord<>(topic, partition, beforeOffset, "before-key".getBytes(Charsets.UTF_8), "before-value".getBytes(Charsets.UTF_8));

        // This ConsumerRecord is EQUAL to the limit, and thus should pass.
        final ConsumerRecord<byte[], byte[]> consumerRecordEqualEnd = new ConsumerRecord<>(topic, partition, endingOffset, "equal-key".getBytes(Charsets.UTF_8), "equal-value".getBytes(Charsets.UTF_8));

        // These two should exceed the limit (since its >) and nothing should be returned.
        final ConsumerRecord<byte[], byte[]> consumerRecordAfterEnd = new ConsumerRecord<>(topic, partition, afterOffset, "after-key".getBytes(Charsets.UTF_8), "after-value".getBytes(Charsets.UTF_8));
        final ConsumerRecord<byte[], byte[]> consumerRecordAfterEnd2 = new ConsumerRecord<>(topic, partition, afterOffset + 1, "after-key2".getBytes(Charsets.UTF_8), "after-value2".getBytes(Charsets.UTF_8));

        // Define expected results returned
        final KafkaMessage expectedKafkaMessageBeforeEndingOffset = new KafkaMessage(new TupleMessageId(topic, partition, beforeOffset, "ConsumerId"), new Values("before-key", "before-value"));
        final KafkaMessage expectedKafkaMessageEqualEndingOffset = new KafkaMessage(new TupleMessageId(topic, partition, endingOffset, "ConsumerId"), new Values("equal-key", "equal-value"));

        // Defining our Ending State
        final ConsumerState endingState = ConsumerState.builder()
            .withPartition(new TopicPartition(topic, partition), endingOffset)
            .build();

        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // TODO: Validate metric collection here
        when(mockSidelineConsumer.getCurrentState()).thenReturn(ConsumerState.builder().build());

        // When nextRecord() is called on the mockSidelineConsumer, we need to return our values in order.
        when(mockSidelineConsumer.nextRecord()).thenReturn(consumerRecordBeforeEnd, consumerRecordEqualEnd, consumerRecordAfterEnd, consumerRecordAfterEnd2);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                factoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, endingState);
        virtualSidelineSpout.setVirtualSpoutId("ConsumerId");
        virtualSidelineSpout.open();

        // Call nextTuple(), this should return our entry BEFORE the ending offset
        KafkaMessage result = virtualSidelineSpout.nextTuple();

        // Check result
        assertNotNull("Should not be null because the offset is under the limit.", result);

        // Validate it
        assertEquals("Found expected topic", topic, result.getTopic());
        assertEquals("Found expected partition", partition, result.getPartition());
        assertEquals("Found expected offset", beforeOffset, result.getOffset());
        assertEquals("Found expected values", new Values("before-key", "before-value"), result.getValues());
        assertEquals("Got expected KafkaMessage", expectedKafkaMessageBeforeEndingOffset, result);

        // Call nextTuple(), this offset should be equal to our ending offset
        // Equal to the end offset should still get emitted.
        result = virtualSidelineSpout.nextTuple();

        // Check result
        assertNotNull("Should not be null because the offset is under the limit.", result);

        // Validate it
        assertEquals("Found expected topic", topic, result.getTopic());
        assertEquals("Found expected partition", partition, result.getPartition());
        assertEquals("Found expected offset", endingOffset, result.getOffset());
        assertEquals("Found expected values", new Values("equal-key", "equal-value"), result.getValues());
        assertEquals("Got expected KafkaMessage", expectedKafkaMessageEqualEndingOffset, result);

        // Call nextTuple(), this offset should be greater than our ending offset
        // and thus should return null.
        result = virtualSidelineSpout.nextTuple();

        // Check result
        assertNull("Should be null because the offset is greater than the limit.", result);

        // Call nextTuple(), again the offset should be greater than our ending offset
        // and thus should return null.
        result = virtualSidelineSpout.nextTuple();

        // Check result
        assertNull("Should be null because the offset is greater than the limit.", result);

        // Validate unsubscribed was called on our mock sidelineConsumer
        // Right now this is called twice... unsure if that is an issue. I don't think it is.
        verify(mockSidelineConsumer, times(2)).unsubscribeTopicPartition(eq(new TopicPartition(topic, partition)));

        // Validate that we never called ack on the tuples that were filtered because they exceeded the max offset
        verify(mockSidelineConsumer, times(0)).commitOffset(new TopicPartition(topic, partition), afterOffset);
        verify(mockSidelineConsumer, times(0)).commitOffset(new TopicPartition(topic, partition), afterOffset + 1);
    }

    /**
     * This test does the following:
     *
     * 1. Call nextTuple() -
     *  a. the first time RetryManager should return null, saying it has no failed tuples to replay
     *  b. sideline consumer should return a consumer record, and it should be returned by nextTuple()
     * 2. Call fail() with the message previously returned from nextTuple().
     * 2. Call nextTuple()
     *  a. This time RetryManager should return the failed tuple
     * 3. Call nextTuple()
     *  a. This time RetryManager should return null, saying it has no failed tuples to replay.
     *  b. sideline consumer should return a new consumer record.
     */
    @Test
    public void testCallingFailOnTupleWhenItShouldBeRetriedItGetsRetried() {
        // This is the record coming from the consumer.
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 3;
        final long expectedOffset = 434323L;
        final String expectedConsumerId = "MyConsumerId";
        final String expectedKey = "MyKey";
        final String expectedValue = "MyValue";
        final byte[] expectedKeyBytes = expectedKey.getBytes(Charsets.UTF_8);
        final byte[] expectedValueBytes = expectedValue.getBytes(Charsets.UTF_8);
        final ConsumerRecord<byte[], byte[]> expectedConsumerRecord = new ConsumerRecord<>(expectedTopic, expectedPartition, expectedOffset, expectedKeyBytes, expectedValueBytes);

        // Define expected result
        final KafkaMessage expectedKafkaMessage = new KafkaMessage(new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId), new Values(expectedKey, expectedValue));

        // This is a second record coming from the consumer
        final long unexpectedOffset = expectedOffset + 2L;
        final String unexpectedKey = "NotMyKey";
        final String unexpectedValue = "NotMyValue";
        final byte[] unexpectedKeyBytes = unexpectedKey.getBytes(Charsets.UTF_8);
        final byte[] unexpectedValueBytes = unexpectedValue.getBytes(Charsets.UTF_8);
        final ConsumerRecord<byte[], byte[]> unexpectedConsumerRecord = new ConsumerRecord<>(expectedTopic, expectedPartition, unexpectedOffset, unexpectedKeyBytes, unexpectedValueBytes);

        // Define unexpected result
        final KafkaMessage unexpectedKafkaMessage = new KafkaMessage(new TupleMessageId(expectedTopic, expectedPartition, unexpectedOffset, expectedConsumerId), new Values(unexpectedKey, unexpectedValue));

        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockSidelineConsumer.nextRecord()).thenReturn(expectedConsumerRecord, unexpectedConsumerRecord);

        // Create a mock RetryManager
        RetryManager mockRetryManager = mock(RetryManager.class);

        // First time its called, it should return null.
        // The 2nd time it should return our tuple Id.
        // The 3rd time it should return null again.
        when(mockRetryManager.nextFailedMessageToRetry()).thenReturn(null, expectedKafkaMessage.getTupleMessageId(), null);

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(null, mockRetryManager);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                mockFactoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, null);
        virtualSidelineSpout.setVirtualSpoutId(expectedConsumerId);
        virtualSidelineSpout.open();

        // Call nextTuple()
        KafkaMessage result = virtualSidelineSpout.nextTuple();

        // Verify we asked the failed message manager, but got nothing back
        verify(mockRetryManager, times(1)).nextFailedMessageToRetry();

        // Check result
        assertNotNull("Should not be null", result);

        // Validate it
        assertEquals("Found expected topic", expectedTopic, result.getTopic());
        assertEquals("Found expected partition", expectedPartition, result.getPartition());
        assertEquals("Found expected offset", expectedOffset, result.getOffset());
        assertEquals("Found expected values", new Values(expectedKey, expectedValue), result.getValues());
        assertEquals("Got expected KafkaMessage", expectedKafkaMessage, result);

        // Now call fail on this
        final TupleMessageId failedMessageId = result.getTupleMessageId();

        // Retry manager should retry this tuple.
        when(mockRetryManager.retryFurther(failedMessageId)).thenReturn(true);

        // failed on retry manager shouldn't have been called yet
        verify(mockRetryManager, never()).failed(anyObject());

        // call fail on our message id
        virtualSidelineSpout.fail(failedMessageId);

        // Verify failed calls
        verify(mockRetryManager, times(1)).failed(failedMessageId);

        // Call nextTuple, we should get our failed tuple back.
        result = virtualSidelineSpout.nextTuple();

        // verify we got the tuple from failed manager
        verify(mockRetryManager, times(2)).nextFailedMessageToRetry();

        // Check result
        assertNotNull("Should not be null", result);

        // Validate it
        assertEquals("Found expected topic", expectedTopic, result.getTopic());
        assertEquals("Found expected partition", expectedPartition, result.getPartition());
        assertEquals("Found expected offset", expectedOffset, result.getOffset());
        assertEquals("Found expected values", new Values(expectedKey, expectedValue), result.getValues());
        assertEquals("Got expected KafkaMessage", expectedKafkaMessage, result);

        // And call nextTuple() one more time, this time failed manager should return null
        // and sideline consumer returns our unexpected result
        // Call nextTuple, we should get our failed tuple back.
        result = virtualSidelineSpout.nextTuple();

        // verify we got the tuple from failed manager
        verify(mockRetryManager, times(3)).nextFailedMessageToRetry();

        // Check result
        assertNotNull("Should not be null", result);

        // Validate it
        assertEquals("Found expected topic", expectedTopic, result.getTopic());
        assertEquals("Found expected partition", expectedPartition, result.getPartition());
        assertEquals("Found expected offset", unexpectedOffset, result.getOffset());
        assertEquals("Found expected values", new Values(unexpectedKey, unexpectedValue), result.getValues());
        assertEquals("Got expected KafkaMessage", unexpectedKafkaMessage, result);
    }

    /**
     * Test calling fail with null, it should just silently drop it.
     */
    @Test
    public void testFailWithNull() {
        // Create mock failed msg retry manager
        final RetryManager mockRetryManager = mock(RetryManager.class);

        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(null, mockRetryManager);

        // Create metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                mockFactoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, null);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.open();

        // Call ack with null, nothing should explode.
        virtualSidelineSpout.fail(null);

        // No interactions w/ our mocks
        verify(mockRetryManager, never()).retryFurther(anyObject());
        verify(mockRetryManager, never()).acked(anyObject());
        verify(mockRetryManager, never()).failed(anyObject());
        verify(mockSidelineConsumer, never()).commitOffset(anyObject(), anyLong());
    }

    /**
     * Call fail() with invalid msg type should throw an exception.
     */
    @Test
    public void testFailWithInvalidMsgIdObject() {
        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                factoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, null);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.open();

        // Call ack with a string object, it should throw an exception.
        expectedException.expect(IllegalArgumentException.class);
        virtualSidelineSpout.fail("This is a String!");
    }

    /**
     * Test calling ack with null, it should just silently drop it.
     */
    @Test
    public void testAckWithNull() {
        // Create mock Failed msg retry manager
        final RetryManager mockRetryManager = mock(RetryManager.class);

        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(null, mockRetryManager);

        // Create metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                mockFactoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, null);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.open();

        // Call ack with null, nothing should explode.
        virtualSidelineSpout.ack(null);

        // No interactions w/ our mock sideline consumer for committing offsets
        verify(mockSidelineConsumer, never()).commitOffset(any(TopicPartition.class), anyLong());
        verify(mockSidelineConsumer, never()).commitOffset(any(ConsumerRecord.class));
        verify(mockRetryManager, never()).acked(anyObject());
    }

    /**
     * Call ack() with invalid msg type should throw an exception.
     */
    @Test
    public void testAckWithInvalidMsgIdObject() {
        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                factoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, null);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.open();

        // Call ack with a string object, it should throw an exception.
        expectedException.expect(IllegalArgumentException.class);
        virtualSidelineSpout.ack("This is my String!");
    }

    /**
     * Test calling ack, ensure it passes the commit command to its internal consumer
     */
    @Test
    public void testAck() {
        // Define our msgId
        final String expectedTopicName = "MyTopic";
        final int expectedPartitionId = 33;
        final long expectedOffset = 313376L;
        final TupleMessageId tupleMessageId = new TupleMessageId(expectedTopicName, expectedPartitionId, expectedOffset, "RandomConsumer");

        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final RetryManager mockRetryManager = mock(RetryManager.class);

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(null, mockRetryManager);

        // Create metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // TODO: Validate metric collection here
        when(mockSidelineConsumer.getCurrentState()).thenReturn(ConsumerState.builder().build());

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                mockFactoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, null);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.open();

        // Never called yet
        verify(mockSidelineConsumer, never()).commitOffset(anyObject(), anyLong());
        verify(mockRetryManager, never()).acked(anyObject());

        // Call ack with a string object, it should throw an exception.
        virtualSidelineSpout.ack(tupleMessageId);

        // Verify mock gets called with appropriate arguments
        verify(mockSidelineConsumer, times(1)).commitOffset(eq(new TopicPartition(expectedTopicName, expectedPartitionId)), eq(expectedOffset));

        // Gets acked on the failed retry manager
        verify(mockRetryManager, times(1)).acked(tupleMessageId);
    }

    /**
     * Test calling this method when no defined endingState.  It should default to always
     * return false in that case.
     */
    @Test
    public void testDoesMessageExceedEndingOffsetWithNoEndingStateDefined() {
        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create metrics recorder
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                factoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, null);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.open();

        // Create our test TupleMessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final long expectedOffset = 31332L;
        final String consumerId = "MyConsumerId";
        final TupleMessageId tupleMessageId = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, consumerId);

        // Call our method & validate.
        final boolean result = virtualSidelineSpout.doesMessageExceedEndingOffset(tupleMessageId);
        assertFalse("Should always be false", result);
    }

    /**
     * Test calling this method with a defined endingState, and the TupleMessageId's offset is equal to it,
     * it should return false.
     */
    @Test
    public void testDoesMessageExceedEndingOffsetWhenItEqualsEndingOffset() {
        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Create our test TupleMessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final long expectedOffset = 31332L;
        final String consumerId = "MyConsumerId";
        final TupleMessageId tupleMessageId = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, consumerId);

        // Define our endingState with a position equal to our TupleMessageId
        final ConsumerState endingState = ConsumerState.builder()
            .withPartition(new TopicPartition(expectedTopic, expectedPartition), expectedOffset)
            .build();

        // Define metric record
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create spout passing in ending state.
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                factoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, endingState);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.open();

        // Call our method & validate.
        final boolean result = virtualSidelineSpout.doesMessageExceedEndingOffset(tupleMessageId);
        assertFalse("Should be false", result);
    }

    /**
     * Test calling this method with a defined endingState, and the TupleMessageId's offset is beyond it.
     */
    @Test
    public void testDoesMessageExceedEndingOffsetWhenItDoesExceedEndingOffset() {
        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Create our test TupleMessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final long expectedOffset = 31332L;
        final String consumerId = "MyConsumerId";
        final TupleMessageId tupleMessageId = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, consumerId);

        // Define our endingState with a position less than our TupleMessageId
        final ConsumerState endingState = ConsumerState.builder()
            .withPartition(new TopicPartition(expectedTopic, expectedPartition), (expectedOffset - 100))
            .build();

        // Define metric record
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create spout passing in ending state.
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                factoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, endingState);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.open();

        // Call our method & validate.
        final boolean result = virtualSidelineSpout.doesMessageExceedEndingOffset(tupleMessageId);
        assertTrue("Should be true", result);
    }

    /**
     * Test calling this method with a defined endingState, and the TupleMessageId's offset is before it.
     */
    @Test
    public void testDoesMessageExceedEndingOffsetWhenItDoesNotExceedEndingOffset() {
        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Create our test TupleMessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final long expectedOffset = 31332L;
        final String consumerId = "MyConsumerId";
        final TupleMessageId tupleMessageId = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, consumerId);

        // Define our endingState with a position greater than than our TupleMessageId
        final ConsumerState endingState = ConsumerState.builder()
            .withPartition(new TopicPartition(expectedTopic, expectedPartition), (expectedOffset + 100))
            .build();

        // Define metric record
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create spout passing in ending state.
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                factoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, endingState);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.open();

        // Call our method & validate.
        final boolean result = virtualSidelineSpout.doesMessageExceedEndingOffset(tupleMessageId);
        assertFalse("Should be false", result);
    }

    /**
     * Test calling this method with a defined endingState, and then send in a TupleMessageId
     * associated with a partition that doesn't exist in the ending state.  It should throw
     * an illegal state exception.
     */
    @Test
    public void testDoesMessageExceedEndingOffsetForAnInvalidPartition() {
        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Create our test TupleMessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final long expectedOffset = 31332L;
        final String consumerId = "MyConsumerId";
        final TupleMessageId tupleMessageId = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, consumerId);

        // Define our endingState with a position greater than than our TupleMessageId, but on a different partition
        final ConsumerState endingState = ConsumerState.builder()
            .withPartition(new TopicPartition(expectedTopic, expectedPartition + 1), (expectedOffset + 100))
            .build();

        // Define metric record
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create spout passing in ending state.
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                factoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, endingState);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.open();

        // Call our method & validate exception is thrown
        expectedException.expect(IllegalStateException.class);
        virtualSidelineSpout.doesMessageExceedEndingOffset(tupleMessageId);
    }

    /**
     * This test uses a mock to validate when you call unsubsubscribeTopicPartition() that it passes the argument
     * to its underlying consumer, and passes back the right result value from that call.
     */
    @Test
    public void testUnsubscribeTopicPartition() {
        final boolean expectedResult = true;

        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.unsubscribeTopicPartition(any(TopicPartition.class))).thenReturn(expectedResult);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Define metric record
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                factoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, null);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.open();

        // Create our test TupleMessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final TopicPartition topicPartition = new TopicPartition(expectedTopic, expectedPartition);

        // Call our method & validate.
        final boolean result = virtualSidelineSpout.unsubscribeTopicPartition(topicPartition);
        assertEquals("Got expected result from our method", expectedResult, result);

        // Validate mock call
        verify(mockSidelineConsumer, times(1)).unsubscribeTopicPartition(eq(topicPartition));
    }

    /**
     * Test calling close, verifies what happens if the completed flag is false.
     */
    @Test
    public void testCloseWithCompletedFlagSetToFalse() throws NoSuchFieldException, IllegalAccessException {
        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final SidelineRequestIdentifier sidelineRequestId = new SidelineRequestIdentifier();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Create a mock PersistanceManager & associate with SidelineConsumer.
        PersistenceManager mockPersistenceManager = mock(PersistenceManager.class);
        when(mockSidelineConsumer.getPersistenceManager()).thenReturn(mockPersistenceManager);

        // Define metric record
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                factoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, null);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.setSidelineRequestIdentifier(sidelineRequestId);
        virtualSidelineSpout.open();

        // Mark sure is completed field is set to false before calling close
        Field isCompletedField = virtualSidelineSpout.getClass().getDeclaredField("isCompleted");
        isCompletedField.setAccessible(true);
        isCompletedField.set(virtualSidelineSpout, false);

        // Verify close hasn't been called yet.
        verify(mockSidelineConsumer, never()).close();

        // Call close
        virtualSidelineSpout.close();

        // Verify close was called, and state was flushed
        verify(mockSidelineConsumer, times(1)).flushConsumerState();
        verify(mockSidelineConsumer, times(1)).close();

        // But we never called remove consumer state.
        verify(mockSidelineConsumer, never()).removeConsumerState();

        // Never remove sideline request state
        verify(mockPersistenceManager, never()).clearSidelineRequest(anyObject());
    }

    /**
     * Test calling close, verifies what happens if the completed flag is true.
     * Verifies what happens if SidelineRequestIdentifier is set.
     */
    @Test
    public void testCloseWithCompletedFlagSetToTrue() throws NoSuchFieldException, IllegalAccessException {
        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final SidelineRequestIdentifier sidelineRequestId = new SidelineRequestIdentifier();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Create a mock PersistanceManager & associate with SidelineConsumer.
        PersistenceManager mockPersistenceManager = mock(PersistenceManager.class);
        when(mockSidelineConsumer.getPersistenceManager()).thenReturn(mockPersistenceManager);

        // Define metric record
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                factoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, null);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.setSidelineRequestIdentifier(sidelineRequestId);
        virtualSidelineSpout.open();

        // Mark sure is completed field is set to true before calling close
        Field isCompletedField = virtualSidelineSpout.getClass().getDeclaredField("isCompleted");
        isCompletedField.setAccessible(true);
        isCompletedField.set(virtualSidelineSpout, true);

        // Verify close hasn't been called yet.
        verify(mockSidelineConsumer, never()).close();

        // Call close
        virtualSidelineSpout.close();

        // Verify close was called, and state was cleared
        verify(mockSidelineConsumer, times(1)).removeConsumerState();
        verify(mockPersistenceManager, times(1)).clearSidelineRequest(sidelineRequestId);
        verify(mockSidelineConsumer, times(1)).close();

        // But we never called flush consumer state.
        verify(mockSidelineConsumer, never()).flushConsumerState();
    }

    /**
     * Test calling close, verifies what happens if the completed flag is true.
     * Verifies what happens if SidelineRequestIdentifier is null.
     */
    @Test
    public void testCloseWithCompletedFlagSetToTrueNoSidelineREquestIdentifier() throws NoSuchFieldException, IllegalAccessException {
        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Create a mock PersistanceManager & associate with SidelineConsumer.
        PersistenceManager mockPersistenceManager = mock(PersistenceManager.class);
        when(mockSidelineConsumer.getPersistenceManager()).thenReturn(mockPersistenceManager);

        // Define metric record
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create factory manager
        final FactoryManager factoryManager = new FactoryManager(topologyConfig);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                factoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, null);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.open();

        // Mark sure is completed field is set to true before calling close
        Field isCompletedField = virtualSidelineSpout.getClass().getDeclaredField("isCompleted");
        isCompletedField.setAccessible(true);
        isCompletedField.set(virtualSidelineSpout, true);

        // Verify close hasn't been called yet.
        verify(mockSidelineConsumer, never()).close();

        // Call close
        virtualSidelineSpout.close();

        // Verify close was called, and state was cleared
        verify(mockSidelineConsumer, times(1)).removeConsumerState();
        verify(mockPersistenceManager, never()).clearSidelineRequest(anyObject());
        verify(mockSidelineConsumer, times(1)).close();

        // But we never called flush consumer state.
        verify(mockSidelineConsumer, never()).flushConsumerState();
    }

    /**
     * This test does the following:
     *
     * 1. Call nextTuple() -
     *  a. the first time RetryManager should return null, saying it has no failed tuples to replay
     *  b. sideline consumer should return a consumer record, and it should be returned by nextTuple()
     * 2. Call fail() with the message previously returned from nextTuple().
     * 2. Call nextTuple()
     *  a. This time RetryManager should return the failed tuple
     * 3. Call nextTuple()
     *  a. This time RetryManager should return null, saying it has no failed tuples to replay.
     *  b. sideline consumer should return a new consumer record.
     */
    @Test
    public void testCallingFailCallsAckOnWhenItShouldNotBeRetried() {
        // This is the record coming from the consumer.
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 3;
        final long expectedOffset = 434323L;
        final String expectedConsumerId = "MyConsumerId";
        final String expectedKey = "MyKey";
        final String expectedValue = "MyValue";
        final byte[] expectedKeyBytes = expectedKey.getBytes(Charsets.UTF_8);
        final byte[] expectedValueBytes = expectedValue.getBytes(Charsets.UTF_8);
        final ConsumerRecord<byte[], byte[]> expectedConsumerRecord = new ConsumerRecord<>(expectedTopic, expectedPartition, expectedOffset, expectedKeyBytes, expectedValueBytes);

        // Define expected result
        final KafkaMessage expectedKafkaMessage = new KafkaMessage(new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId), new Values(expectedKey, expectedValue));

        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create mock topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Create a mock RetryManager
        RetryManager mockRetryManager = mock(RetryManager.class);

        // Define metric record
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(null, mockRetryManager);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                mockFactoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, null);
        virtualSidelineSpout.setVirtualSpoutId(expectedConsumerId);
        virtualSidelineSpout.open();

        // Now call fail on this
        final TupleMessageId failedMessageId = expectedKafkaMessage.getTupleMessageId();

        // Retry manager should retry this tuple.
        when(mockRetryManager.retryFurther(failedMessageId)).thenReturn(false);

        // call fail on our message id
        virtualSidelineSpout.fail(failedMessageId);

        // Verify since this wasn't retried, it gets acked both by the consumer and the retry manager.
        verify(mockRetryManager, times(1)).acked(failedMessageId);
        verify(mockSidelineConsumer, times(1)).commitOffset(failedMessageId.getTopicPartition(), failedMessageId.getOffset());
    }

    /**
     * Tests that calling getCurrentState() is based down to the
     * sidelineConsumer appropriately.
     */
    @Test
    public void testGetCurrentState() {
        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final RetryManager mockRetryManager = mock(RetryManager.class);

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(
            Collections.singletonList(new PartitionInfo("foobar", 0, new Node(1, "localhost", 1234), new Node[]{}, new Node[]{}))
        );

        // Define metric record
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(null, mockRetryManager);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                mockTopologyContext,
                mockFactoryManager,
                metricsRecorder,
                mockSidelineConsumer,
                null, null);
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");
        virtualSidelineSpout.open();

        final ConsumerState expectedConsumerState = ConsumerState
            .builder()
            .withPartition(new TopicPartition("myTopic", 0), 200L)
            .build();

        // Setup our mock to return expected value
        when(mockSidelineConsumer.getCurrentState()).thenReturn(expectedConsumerState);

        // Call get current state.
        final ConsumerState result = virtualSidelineSpout.getCurrentState();

        // Verify mock interactions
        verify(mockSidelineConsumer, times(1)).getCurrentState();

        // Verify result
        assertNotNull("result should not be null", result);
        assertEquals("Should be our expected instance", expectedConsumerState, result);
    }

    /**
     * Test the number of partitions consumed from for a multi-instance spout
     *
     * @param numInstances Number of instances
     * @param numPartitions Number of partitions
     * @param instanceIndex The instances index (starts at 0)
     * @param partitionCount Number of partitions to expect
     */
    @Test
    @UseDataProvider("dataProviderForGetPartitions")
    public void testGetPartitions(int numInstances, int numPartitions, int instanceIndex, int partitionCount) {
        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final RetryManager mockRetryManager = mock(RetryManager.class);

        final MockTopologyContext mockTopologyContext = new MockTopologyContext();
        mockTopologyContext.taskIndex = instanceIndex;
        mockTopologyContext.componentTasks = new ArrayList<>();

        for (int i=1; i<=numInstances; i++) {
            mockTopologyContext.componentTasks.add(i);
        }

        final String topic = "foobar";
        final Node leader = new Node(1, "localhost", 1234);
        final List<PartitionInfo> mockPartitions = new ArrayList<>();

        for (int i=1; i<=numPartitions; i++) {
            mockPartitions.add(new PartitionInfo(topic, i, leader, new Node[]{}, new Node[]{}));
        }

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.getPartitions()).thenReturn(mockPartitions);

        // Define metric record
        final MetricsRecorder metricsRecorder = new LogRecorder();
        metricsRecorder.open(topologyConfig, mockTopologyContext);

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(null, mockRetryManager);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
            topologyConfig,
            mockTopologyContext,
            mockFactoryManager,
            metricsRecorder,
            mockSidelineConsumer,
            null,
            null
        );
        virtualSidelineSpout.setVirtualSpoutId("MyConsumerId");

        List<PartitionInfo> partitions = virtualSidelineSpout.getPartitions();

        assertEquals(partitionCount, partitions.size());
    }

    @DataProvider
    public static Object[][] dataProviderForGetPartitions() {
        return new Object[][]{
            // One instance, three partitions, first instance, expect 3 partitions for this spout
            { 1, 3, 0, 3 },
            // Two instances, three partitions, first instance, expect 2 partitions for this spout
            { 2, 3, 0, 2 },
            // Two instances, three partitions, second instance, expect 1 partition for this spout
            { 2, 3, 1, 1 },
            // Three instances, three partitions, third instance, expect 1 partitions for this spout
            { 3, 3, 2, 1 },

        };
    }

    /**
     * Utility method to generate a standard config map.
     */
    private Map getDefaultConfig() {
        final Map defaultConfig = Maps.newHashMap();
        defaultConfig.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:9092"));
        defaultConfig.put(SidelineSpoutConfig.KAFKA_TOPIC, "MyTopic");
        defaultConfig.put(SidelineSpoutConfig.CONSUMER_ID_PREFIX, "TestPrefix");
        defaultConfig.put(SidelineSpoutConfig.PERSISTENCE_ZK_ROOT, "/sideline-spout-test");
        defaultConfig.put(SidelineSpoutConfig.PERSISTENCE_ZK_SERVERS, Lists.newArrayList("localhost:21811"));
        defaultConfig.put(SidelineSpoutConfig.DESERIALIZER_CLASS, Utf8StringDeserializer.class.getName());

        return SidelineSpoutConfig.setDefaults(defaultConfig);
    }

    /**
     * Utility method for creating a mock factory manager.
     */
    private FactoryManager createMockFactoryManager(Deserializer deserializer, RetryManager retryManager) {
        // Create our mock
        FactoryManager factoryManager = mock(FactoryManager.class);

        // If a mocked deserializer not passed in
        if (deserializer == null) {
            // Default to utf8
            deserializer = new Utf8StringDeserializer();
        }
        when(factoryManager.createNewDeserializerInstance()).thenReturn(deserializer);

        // If a mocked failed msg retry manager isn't passed in
        if (retryManager == null) {
            retryManager = new NeverRetryManager();
        }
        when(factoryManager.createNewFailedMsgRetryManagerInstance()).thenReturn(retryManager);

        return factoryManager;
    }
}