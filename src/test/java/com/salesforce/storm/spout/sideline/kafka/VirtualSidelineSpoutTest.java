package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.filter.StaticMessageFilter;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Deserializer;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Utf8StringDeserializer;
import com.salesforce.storm.spout.sideline.trigger.SidelineIdentifier;
import com.salesforce.storm.spout.sideline.mocks.MockTopologyContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.shade.com.google.common.base.Charsets;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 */
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

        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(expectedTopologyConfig, mockTopologyContext, new Utf8StringDeserializer());

        // Verify things got set
        assertNotNull("TopologyConfig should be non-null", virtualSidelineSpout.getTopologyConfig());
        assertNotNull("TopologyContext should be non-null", virtualSidelineSpout.getTopologyContext());

        // Verify the config is correct (and not some empty map)
        assertEquals("Should have correct number of entries", expectedTopologyConfig.size(), virtualSidelineSpout.getTopologyConfig().size());
        assertEquals("Should have correct entries", expectedTopologyConfig, virtualSidelineSpout.getTopologyConfig());

        // Verify the config is immutable and throws exception when you try to modify it
        expectedException.expect(UnsupportedOperationException.class);
        virtualSidelineSpout.getTopologyConfig().put("poop", "value");
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

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(expectedTopologyConfig, new MockTopologyContext(), new Utf8StringDeserializer());

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
     * Note - Setter may go away in liu of being set by the topologyConfig.  getConsumerId() should remain tho.
     */
    @Test
    public void testSetAndGetConsumerId() {
        // Define input
        final String expectedConsumerId = "myConsumerId";

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(Maps.newHashMap(), new MockTopologyContext(), new Utf8StringDeserializer());

        // Set it
        virtualSidelineSpout.setConsumerId(expectedConsumerId);

        // Verify it
        assertEquals("Got expected consumer id", expectedConsumerId, virtualSidelineSpout.getConsumerId());
    }

    /**
     * Test setter and getter.
     */
    @Test
    public void testSetAndGetIsFinished() {
        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(Maps.newHashMap(), new MockTopologyContext(), new Utf8StringDeserializer());

        // Should default to false
        assertFalse("Should default to false", virtualSidelineSpout.isFinished());

        // Set to true
        virtualSidelineSpout.setFinished(true);
        assertTrue("Should be true", virtualSidelineSpout.isFinished());

        // Set back to false
        virtualSidelineSpout.setFinished(false);
        assertFalse("Should not be true", virtualSidelineSpout.isFinished());
    }

    /**
     * Calling open() more than once should throw an exception.
     */
    @Test
    public void testCallingOpenTwiceThrowsException() {
        // Create test config
        Map topologyConfig = Maps.newHashMap();
        topologyConfig.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:9092"));

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, new MockTopologyContext(), new Utf8StringDeserializer(), mockSidelineConsumer);

        // Call it once.
        virtualSidelineSpout.open();

        // Validate that open() on SidelineConsumer is called once.
        verify(mockSidelineConsumer, times(1)).connect();

        // Set expected exception
        expectedException.expect(IllegalStateException.class);
        virtualSidelineSpout.open();
    }

    /**
     * Validate that Open behaves like we expect.
     * @TODO - Need to expand upon this test once we have configuration for more things working.
     */
    @Test
    public void testOpen() {
        // Create test config
        Map topologyConfig = Maps.newHashMap();
        topologyConfig.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:9092"));

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, new MockTopologyContext(), new Utf8StringDeserializer(), mockSidelineConsumer);

        // Call open
        virtualSidelineSpout.open();

        // Validate that open() on SidelineConsumer is called once.
        verify(mockSidelineConsumer, times(1)).connect();
    }

    /**
     * Tests when you call nextTuple() and the underlying consumer.nextRecord() returns null,
     * then nextTuple() should also return null.
     *
     * @TODO - validate that ack is not called underlying consumer
     */
    @Test
    public void testNextTupleWhenConsumerReturnsNull() {
        // Define some inputs
        final ConsumerRecord<byte[], byte[]> expectedConsumerRecord = null;

        // Create test config
        Map topologyConfig = Maps.newHashMap();
        topologyConfig.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:9092"));

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockSidelineConsumer.nextRecord()).thenReturn(expectedConsumerRecord);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, new MockTopologyContext(), new Utf8StringDeserializer(), mockSidelineConsumer);
        virtualSidelineSpout.open();

        // Call nextTuple()
        KafkaMessage result = virtualSidelineSpout.nextTuple();

        // Verify its null
        assertNull("Should be null",  result);

        // TODO: Modify this.
        verify(mockSidelineConsumer, never()).commitOffset(any(), anyLong());
        verify(mockSidelineConsumer, never()).commitOffset(any());
    }

    /**
     * Tests what happens when you call nextTuple(), and the underyling deserializer fails to
     * deserialize (returns null), then nextTuple() should return null.
     *
     * @TODO - validate that the ConsumerRecord is acked in this scenario.
     */
    @Test
    public void testNextTupleWhenSerializerFailsToDeserialize() {
        // Define a deserializer that always returns null
        final Deserializer nullDeserializer = new Deserializer() {
            @Override
            public Values deserialize(String topic, int partition, long offset, byte[] key, byte[] value) {
                return null;
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
        Map topologyConfig = Maps.newHashMap();
        topologyConfig.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:9092"));

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockSidelineConsumer.nextRecord()).thenReturn(expectedConsumerRecord);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, new MockTopologyContext(), nullDeserializer, mockSidelineConsumer);
        virtualSidelineSpout.open();

        // Call nextTuple()
        KafkaMessage result = virtualSidelineSpout.nextTuple();

        // Verify its null
        assertNull("Should be null",  result);
    }

    /**
     * Validates what happens when a message is pulled from the underlying kafka consumer, but it is filtered
     * out by the filter chain.  nextTuple() should return null.
     */
    @Test
    public void testNextTupleReturnsNullWhenFiltered() {
        // Use UTF8 String Deserializer
        // Define a deserializer that always returns null
        final Deserializer stringDeserializer = new Utf8StringDeserializer();

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
        Map topologyConfig = Maps.newHashMap();
        topologyConfig.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:9092"));

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockSidelineConsumer.nextRecord()).thenReturn(expectedConsumerRecord);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
            topologyConfig,
            new MockTopologyContext(),
            stringDeserializer, mockSidelineConsumer
        );
        virtualSidelineSpout.getFilterChain().addStep(new SidelineIdentifier(), new StaticMessageFilter(true));
        virtualSidelineSpout.setConsumerId(expectedConsumerId);
        virtualSidelineSpout.open();

        // Call nextTuple()
        KafkaMessage result = virtualSidelineSpout.nextTuple();

        // Check result
        assertNull("Should be null", result);
    }

    /**
     * Validate what happens if everything works as expected, its deserialized properly, its not filtered.
     */
    @Test
    public void testNextTuple() {
        // Use UTF8 String Deserializer
        // Define a deserializer that always returns null
        final Deserializer stringDeserializer = new Utf8StringDeserializer();

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
        Map topologyConfig = Maps.newHashMap();
        topologyConfig.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:9092"));

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockSidelineConsumer.nextRecord()).thenReturn(expectedConsumerRecord);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, new MockTopologyContext(), stringDeserializer, mockSidelineConsumer);
        virtualSidelineSpout.setConsumerId(expectedConsumerId);
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

    // Things left to test
    public void testAck() {
    }
    public void testFail() {
    }
    public void testActivate() {
    }
    public void testDeactivate() {
    }
    public void testClose() {
    }

}