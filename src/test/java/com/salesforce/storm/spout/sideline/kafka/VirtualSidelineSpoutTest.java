package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.filter.StaticMessageFilter;
import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Deserializer;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Utf8StringDeserializer;
import com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers.FailedMsgRetryManager;
import com.salesforce.storm.spout.sideline.trigger.SidelineIdentifier;
import com.salesforce.storm.spout.sideline.mocks.MockTopologyContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.shade.com.google.common.base.Charsets;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
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
        virtualSidelineSpout.finish();
        assertTrue("Should be true", virtualSidelineSpout.isFinished());
    }

    /**
     * Calling open() more than once should throw an exception.
     */
    @Test
    public void testCallingOpenTwiceThrowsException() {
        // Create test config
        Map topologyConfig = getDefaultConfig();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, new MockTopologyContext(), new Utf8StringDeserializer(), mockSidelineConsumer);
        virtualSidelineSpout.setConsumerId("MyConsumerId");

        // Call it once.
        virtualSidelineSpout.open();

        // Validate that open() on SidelineConsumer is called once.
        verify(mockSidelineConsumer, times(1)).connect(null);

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
        Map topologyConfig = getDefaultConfig();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, new MockTopologyContext(), new Utf8StringDeserializer(), mockSidelineConsumer);
        virtualSidelineSpout.setConsumerId("MyConsumerId");

        // Call open
        virtualSidelineSpout.open();

        // Validate that open() on SidelineConsumer is called once.
        verify(mockSidelineConsumer, times(1)).connect(null);
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
        Map topologyConfig = getDefaultConfig();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockSidelineConsumer.nextRecord()).thenReturn(expectedConsumerRecord);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, new MockTopologyContext(), new Utf8StringDeserializer(), mockSidelineConsumer);
        virtualSidelineSpout.setConsumerId("MyConsumerId");
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
     * Tests what happens when you call nextTuple(), and the underyling deserializer fails to
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

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockSidelineConsumer.nextRecord()).thenReturn(expectedConsumerRecord);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, new MockTopologyContext(), nullDeserializer, mockSidelineConsumer);
        virtualSidelineSpout.setConsumerId("MyConsumerId");
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
        Map topologyConfig = getDefaultConfig();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockSidelineConsumer.nextRecord()).thenReturn(expectedConsumerRecord);

        final StaticMessageFilter filterStep = new StaticMessageFilter();

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(
                topologyConfig,
                new MockTopologyContext(),
                stringDeserializer, mockSidelineConsumer
        );
        virtualSidelineSpout.getFilterChain().addStep(new SidelineIdentifier(), filterStep);
        virtualSidelineSpout.setConsumerId(expectedConsumerId);
        virtualSidelineSpout.open();

        filterStep.setShouldFilter(true);

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
        Map topologyConfig = getDefaultConfig();

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

    /**
     * 1. publish a bunch of messages to a topic with a single partition.
     * 2. create a VirtualSideLineSpout where we explicitly define an ending offset less than the total msgs published
     * 3. Consume from the Spout (call nextTuple())
     * 4. Ensure that we stop getting tuples back after we hit the ending state offset.
     */
    @Test
    public void testNextTupleIgnoresMessagesThatHaveExceededEndingStatePositionSinglePartition() {
        // Use UTF8 String Deserializer
        // Define a deserializer that always returns null
        final Deserializer stringDeserializer = new Utf8StringDeserializer();

        // Define some variables
        final long endingOffset = 4444L;
        final int partition = 4;
        final String topic = "MyTopic";

        // Define before offset
        final long beforeOffset = (endingOffset - 100);
        final long afterOffset = (endingOffset + 100);

        // Create a ConsumerRecord who's offset is BEFORE the ending offset, this should pass
        final ConsumerRecord<byte[], byte[]> consumerRecordBeforeEnd = new ConsumerRecord<>(topic, partition, beforeOffset, "before-key".getBytes(Charsets.UTF_8), "before-value".getBytes(Charsets.UTF_8));

        // These two should exceed the limit (since its >=) and nothing should be returned.
        final ConsumerRecord<byte[], byte[]> consumerRecordEqualEnd = new ConsumerRecord<>(topic, partition, endingOffset, "equal-key".getBytes(Charsets.UTF_8), "equal-value".getBytes(Charsets.UTF_8));
        final ConsumerRecord<byte[], byte[]> consumerRecordAfterEnd = new ConsumerRecord<>(topic, partition, afterOffset, "after-key".getBytes(Charsets.UTF_8), "after-value".getBytes(Charsets.UTF_8));

        // Define expected results returned
        final KafkaMessage expectedKafkaMessageBeforeEndingOffset = new KafkaMessage(new TupleMessageId(topic, partition, beforeOffset, "ConsumerId"), new Values("before-key", "before-value"));

        // Defining our Ending State
        final ConsumerState endingState = new ConsumerState();
        endingState.setOffset(new TopicPartition(topic, partition), endingOffset);

        // Create test config
        Map topologyConfig = getDefaultConfig();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return our values in order.
        when(mockSidelineConsumer.nextRecord()).thenReturn(consumerRecordBeforeEnd, consumerRecordEqualEnd, consumerRecordAfterEnd);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, new MockTopologyContext(), stringDeserializer, mockSidelineConsumer, null, endingState);
        virtualSidelineSpout.setConsumerId("ConsumerId");
        virtualSidelineSpout.open();

        // Call nextTuple()
        KafkaMessage result = virtualSidelineSpout.nextTuple();

        // Check result
        assertNotNull("Should not be null because the offset is under the limit.", result);

        // Validate it
        assertEquals("Found expected topic", topic, result.getTopic());
        assertEquals("Found expected partition", partition, result.getPartition());
        assertEquals("Found expected offset", beforeOffset, result.getOffset());
        assertEquals("Found expected values", new Values("before-key", "before-value"), result.getValues());
        assertEquals("Got expected KafkaMessage", expectedKafkaMessageBeforeEndingOffset, result);

        // Call nextTuple()
        result = virtualSidelineSpout.nextTuple();

        // Check result
        assertNull("Should be null because the offset is equal to the limit.", result);

        // Call nextTuple()
        result = virtualSidelineSpout.nextTuple();

        // Check result
        assertNull("Should be null because the offset is greater than the limit.", result);

        // Validate unsubscribed was called on our mock sidelineConsumer
        // Right now this is called twice... unsure if thats an issue. I don't think it is.
        verify(mockSidelineConsumer, times(2)).unsubscribeTopicPartition(eq(new TopicPartition(topic, partition)));

        // Validate that we called ack on the tuples that were filtered because they exceeded the max offset
        verify(mockSidelineConsumer, times(1)).commitOffset(new TopicPartition(topic, partition), endingOffset);
        verify(mockSidelineConsumer, times(1)).commitOffset(new TopicPartition(topic, partition), afterOffset);
    }

    /**
     * This test does the following:
     *
     * 1. Call nextTuple() -
     *  a. the first time FailedMsgRetryManager should return null, saying it has no failed tuples to replay
     *  b. sideline consumer should return a consumer record, and it should be returned by nextTuple()
     * 2. Call fail() with the message previously returned from nextTuple().
     * 2. Call nextTuple()
     *  a. This time FailedMsgRetryManager should return the failed tuple
     * 3. Call nextTuple()
     *  a. This time FailedMsgRetryManager should return null, saying it has no failed tuples to replay.
     *  b. sideline consumer should return a new consumer record.
     */
    @Test
    public void testCallingFailOnTupleWhenItShouldBeRetriedItGetsRetried() {
        // Use UTF8 String Deserializer
        // Define a deserializer that always returns null
        final Deserializer stringDeserializer = new Utf8StringDeserializer();

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

        // This is a second record coming frmo the consumer
        final long unexpectedOffset = expectedOffset + 2L;
        final String unexpectedKey = "NotMyKey";
        final String unexpectedValue = "NotMyValue";
        final byte[] unexpectedKeyBytes = unexpectedKey.getBytes(Charsets.UTF_8);
        final byte[] unexpectedValueBytes = unexpectedValue.getBytes(Charsets.UTF_8);
        final ConsumerRecord<byte[], byte[]> unexpectedConsumerRecord = new ConsumerRecord<>(expectedTopic, expectedPartition, unexpectedOffset, unexpectedKeyBytes, unexpectedValueBytes);

        // Define unexpected result
        final KafkaMessage unexpectedKafkaMessage = new KafkaMessage(new TupleMessageId(expectedTopic, expectedPartition, unexpectedOffset, expectedConsumerId), new Values(unexpectedKey, unexpectedValue));

        // Create test config
        Map topologyConfig = getDefaultConfig();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockSidelineConsumer.nextRecord()).thenReturn(expectedConsumerRecord, unexpectedConsumerRecord);

        // Create a mock RetryManager
        FailedMsgRetryManager mockRetryManager = mock(FailedMsgRetryManager.class);

        // First time its called, it should return null.
        // The 2nd time it should return our tuple Id.
        // The 3rd time it should return null again.
        when(mockRetryManager.nextFailedMessageToRetry()).thenReturn(null, expectedKafkaMessage.getTupleMessageId(), null);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, new MockTopologyContext(), stringDeserializer, mockSidelineConsumer);
        virtualSidelineSpout.setConsumerId(expectedConsumerId);
        virtualSidelineSpout.setFailedMsgRetryManager(mockRetryManager);
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
     * Test calling ack with null, it should just silently drop it.
     */
    @Test
    public void testAckWithNull() {
        // Create inputs
        final Map expectedTopologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final FailedMsgRetryManager mockRetryManager = mock(FailedMsgRetryManager.class);

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(expectedTopologyConfig, mockTopologyContext, new Utf8StringDeserializer(), mockSidelineConsumer);
        virtualSidelineSpout.setConsumerId("MyConsumerId");
        virtualSidelineSpout.setFailedMsgRetryManager(mockRetryManager);
        virtualSidelineSpout.open();

        // Call ack with null, nothing should explode.
        virtualSidelineSpout.ack(null);

        // No iteractions w/ our mock sideline consumer for committing offsets
        verify(mockSidelineConsumer, never()).commitOffset(any(TopicPartition.class), anyLong());
        verify(mockSidelineConsumer, never()).commitOffset(any(ConsumerRecord.class));
        verify(mockRetryManager, never()).acked(anyObject());
    }

    /**
     * Call ack() with invalid msg type should throw an exception.
     */
    @Test
    public void testAckWithInvalidMsgIdObject() {
        // Create inputs
        final Map expectedTopologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(expectedTopologyConfig, mockTopologyContext, new Utf8StringDeserializer(), mockSidelineConsumer);
        virtualSidelineSpout.setConsumerId("MyConsumerId");
        virtualSidelineSpout.open();

        // Call ack with a string object, it should throw an exception.
        expectedException.expect(IllegalArgumentException.class);
        virtualSidelineSpout.ack("Poop");
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
        final Map expectedTopologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final FailedMsgRetryManager mockRetryManager = mock(FailedMsgRetryManager.class);

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(expectedTopologyConfig, mockTopologyContext, new Utf8StringDeserializer(), mockSidelineConsumer);
        virtualSidelineSpout.setConsumerId("MyConsumerId");
        virtualSidelineSpout.setFailedMsgRetryManager(mockRetryManager);
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
        // Create inputs
        final Map expectedTopologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(expectedTopologyConfig, mockTopologyContext, new Utf8StringDeserializer(), mockSidelineConsumer);
        virtualSidelineSpout.setConsumerId("MyConsumerId");
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
     * Test calling this method with a defined endingState, and the TupleMEssageId's offset is equal to it.
     */
    @Test
    public void testDoesMessageExceedEndingOffsetWhenItEqualsEndingOffset() {
        // Create inputs
        final Map expectedTopologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // Create our test TupleMessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final long expectedOffset = 31332L;
        final String consumerId = "MyConsumerId";
        final TupleMessageId tupleMessageId = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, consumerId);

        // Define our endingState with a position equal to our TupleMessageId
        ConsumerState endingState = new ConsumerState();
        endingState.setOffset(new TopicPartition(expectedTopic, expectedPartition), expectedOffset);

        // Create spout passing in ending state.
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(expectedTopologyConfig, mockTopologyContext, new Utf8StringDeserializer(), mockSidelineConsumer, null, endingState);
        virtualSidelineSpout.setConsumerId("MyConsumerId");
        virtualSidelineSpout.open();

        // Call our method & validate.
        final boolean result = virtualSidelineSpout.doesMessageExceedEndingOffset(tupleMessageId);
        assertTrue("Should be true", result);
    }

    /**
     * Test calling this method with a defined endingState, and the TupleMEssageId's offset is beyond it.
     */
    @Test
    public void testDoesMessageExceedEndingOffsetWhenItDoesExceedEndingOffset() {
        // Create inputs
        final Map expectedTopologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // Create our test TupleMessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final long expectedOffset = 31332L;
        final String consumerId = "MyConsumerId";
        final TupleMessageId tupleMessageId = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, consumerId);

        // Define our endingState with a position less than our TupleMessageId
        ConsumerState endingState = new ConsumerState();
        endingState.setOffset(new TopicPartition(expectedTopic, expectedPartition), (expectedOffset - 100));

        // Create spout passing in ending state.
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(expectedTopologyConfig, mockTopologyContext, new Utf8StringDeserializer(), mockSidelineConsumer, null, endingState);
        virtualSidelineSpout.setConsumerId("MyConsumerId");
        virtualSidelineSpout.open();

        // Call our method & validate.
        final boolean result = virtualSidelineSpout.doesMessageExceedEndingOffset(tupleMessageId);
        assertTrue("Should be true", result);
    }

    /**
     * Test calling this method with a defined endingState, and the TupleMEssageId's offset is before it.
     */
    @Test
    public void testDoesMessageExceedEndingOffsetWhenItDoesNotExceedEndingOffset() {
        // Create inputs
        final Map expectedTopologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // Create our test TupleMessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final long expectedOffset = 31332L;
        final String consumerId = "MyConsumerId";
        final TupleMessageId tupleMessageId = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, consumerId);

        // Define our endingState with a position greater than than our TupleMessageId
        ConsumerState endingState = new ConsumerState();
        endingState.setOffset(new TopicPartition(expectedTopic, expectedPartition), (expectedOffset + 100));

        // Create spout passing in ending state.
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(expectedTopologyConfig, mockTopologyContext, new Utf8StringDeserializer(), mockSidelineConsumer, null, endingState);
        virtualSidelineSpout.setConsumerId("MyConsumerId");
        virtualSidelineSpout.open();

        // Call our method & validate.
        final boolean result = virtualSidelineSpout.doesMessageExceedEndingOffset(tupleMessageId);
        assertFalse("Should be false", result);
    }

    /**
     * Test calling this method with a defined endingState, and then send in a TupleMessageId
     * associated with a partition that doesn't exist in the ending state.
     */
    @Test
    public void testDoesMessageExceedEndingOffsetForAnInvalidPartition() {
        // Create inputs
        final Map expectedTopologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // Create our test TupleMessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final long expectedOffset = 31332L;
        final String consumerId = "MyConsumerId";
        final TupleMessageId tupleMessageId = new TupleMessageId(expectedTopic, expectedPartition, expectedOffset, consumerId);

        // Define our endingState with a position greater than than our TupleMessageId, but on a different partition
        ConsumerState endingState = new ConsumerState();
        endingState.setOffset(new TopicPartition(expectedTopic, expectedPartition + 1), (expectedOffset + 100));

        // Create spout passing in ending state.
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(expectedTopologyConfig, mockTopologyContext, new Utf8StringDeserializer(), mockSidelineConsumer, null, endingState);
        virtualSidelineSpout.setConsumerId("MyConsumerId");
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
        final Map expectedTopologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);
        when(mockSidelineConsumer.unsubscribeTopicPartition(any(TopicPartition.class))).thenReturn(expectedResult);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(expectedTopologyConfig, mockTopologyContext, new Utf8StringDeserializer(), mockSidelineConsumer);
        virtualSidelineSpout.setConsumerId("MyConsumerId");
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

    private Map getDefaultConfig() {
        final Map defaultConfig = Maps.newHashMap();
        defaultConfig.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:9092"));
        defaultConfig.put(SidelineSpoutConfig.KAFKA_TOPIC, "MyTopic");
        defaultConfig.put(SidelineSpoutConfig.CONSUMER_ID_PREFIX, "TestPrefix");

        return defaultConfig;
    }

    /**
     * Test calling close, calls close on underlying consumer.
     */
    @Test
    public void testClose() {
        // Create inputs
        final Map expectedTopologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // Create spout
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(expectedTopologyConfig, mockTopologyContext, new Utf8StringDeserializer(), mockSidelineConsumer);
        virtualSidelineSpout.setConsumerId("MyConsumerId");
        virtualSidelineSpout.open();

        // Verify close hasn't been called yet.
        verify(mockSidelineConsumer, never()).close();

        // Call close
        virtualSidelineSpout.close();

        // Verify close was called
        verify(mockSidelineConsumer, times(1)).close();
    }

    /**
     * This test does the following:
     *
     * 1. Call nextTuple() -
     *  a. the first time FailedMsgRetryManager should return null, saying it has no failed tuples to replay
     *  b. sideline consumer should return a consumer record, and it should be returned by nextTuple()
     * 2. Call fail() with the message previously returned from nextTuple().
     * 2. Call nextTuple()
     *  a. This time FailedMsgRetryManager should return the failed tuple
     * 3. Call nextTuple()
     *  a. This time FailedMsgRetryManager should return null, saying it has no failed tuples to replay.
     *  b. sideline consumer should return a new consumer record.
     */
    @Test
    public void testCallingFailCallsAckOnWhenItShouldNotBeRetried() {
        // Use UTF8 String Deserializer
        // Define a deserializer that always returns null
        final Deserializer stringDeserializer = new Utf8StringDeserializer();

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
        Map topologyConfig = getDefaultConfig();

        // Create a mock SidelineConsumer
        SidelineConsumer mockSidelineConsumer = mock(SidelineConsumer.class);

        // Create a mock RetryManager
        FailedMsgRetryManager mockRetryManager = mock(FailedMsgRetryManager.class);

        // Create spout & open
        VirtualSidelineSpout virtualSidelineSpout = new VirtualSidelineSpout(topologyConfig, new MockTopologyContext(), stringDeserializer, mockSidelineConsumer);
        virtualSidelineSpout.setConsumerId(expectedConsumerId);
        virtualSidelineSpout.setFailedMsgRetryManager(mockRetryManager);
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

}