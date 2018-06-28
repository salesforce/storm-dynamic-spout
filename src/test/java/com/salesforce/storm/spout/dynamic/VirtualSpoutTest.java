/**
 * Copyright (c) 2017, Salesforce.com, Inc.
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

package com.salesforce.storm.spout.dynamic;

import com.google.common.collect.Lists;
import com.salesforce.storm.spout.dynamic.kafka.KafkaConsumerConfig;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerPeerContext;
import com.salesforce.storm.spout.dynamic.consumer.Record;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;
import com.salesforce.storm.spout.sideline.filter.StaticMessageFilter;
import com.salesforce.storm.spout.dynamic.handler.NoopVirtualSpoutHandler;
import com.salesforce.storm.spout.sideline.handler.SidelineVirtualSpoutHandler;
import com.salesforce.storm.spout.dynamic.handler.VirtualSpoutHandler;
import com.salesforce.storm.spout.dynamic.kafka.Consumer;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerState;
import com.salesforce.storm.spout.dynamic.kafka.deserializer.Deserializer;
import com.salesforce.storm.spout.dynamic.kafka.deserializer.Utf8StringDeserializer;
import com.salesforce.storm.spout.dynamic.persistence.InMemoryPersistenceAdapter;
import com.salesforce.storm.spout.dynamic.persistence.ZookeeperPersistenceAdapter;
import com.salesforce.storm.spout.dynamic.retry.NeverRetryManager;
import com.salesforce.storm.spout.dynamic.retry.RetryManager;
import com.salesforce.storm.spout.dynamic.metrics.LogRecorder;
import com.salesforce.storm.spout.dynamic.mocks.MockTopologyContext;
import com.salesforce.storm.spout.dynamic.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;

import com.salesforce.storm.spout.sideline.SidelineVirtualSpoutIdentifier;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test that a {@link VirtualSpout} properly acks, fails and emits data from it's consumer.
 */
public class VirtualSpoutTest {

    @Rule
    public ExpectedException expectedExceptionConstructor = ExpectedException.none();

    /**
     * Verify that constructor args get set appropriately.
     */
    @Test
    public void testConstructor() {
        // Create inputs
        final Map<String, Object> expectedTopologyConfig = getDefaultConfig();
        expectedTopologyConfig.put("Key1", "Value1");
        expectedTopologyConfig.put("Key2", "Value2");
        expectedTopologyConfig.put("Key3", "Value3");

        // Create a mock topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a factory manager
        final FactoryManager factoryManager = new FactoryManager(expectedTopologyConfig);

        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            expectedTopologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            null
        );

        // Verify things got set
        assertNotNull("TopologyConfig should be non-null", virtualSpout.getSpoutConfig());
        assertNotNull("TopologyContext should be non-null", virtualSpout.getTopologyContext());

        // Verify the config is correct (and not some empty map)
        assertEquals("Should have correct number of entries", expectedTopologyConfig.size(), virtualSpout.getSpoutConfig().size());
        assertEquals("Should have correct entries", expectedTopologyConfig, virtualSpout.getSpoutConfig());

        // Verify factory manager set
        assertNotNull("Should have non-null factory manager", virtualSpout.getFactoryManager());
        assertEquals("Should be our instance passed in", factoryManager, virtualSpout.getFactoryManager());

        // Verify the config is immutable and throws exception when you try to modify it
        expectedExceptionConstructor.expect(UnsupportedOperationException.class);
        virtualSpout.getSpoutConfig().put("MyKey", "MyValue");
    }

    /**
     * Verify that getSpoutConfigItem() works as expected.
     */
    @Test
    public void testGetTopologyConfigItem() {
        // Create inputs
        final Map<String, Object> expectedTopologyConfig = getDefaultConfig();
        expectedTopologyConfig.put("Key1", "Value1");
        expectedTopologyConfig.put("Key2", "Value2");
        expectedTopologyConfig.put("Key3", "Value3");

        // Create a mock topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a factory manager
        final FactoryManager factoryManager = new FactoryManager(expectedTopologyConfig);

        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            expectedTopologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            null
        );

        // Verify things got set
        assertNotNull("TopologyConfig should be non-null", virtualSpout.getSpoutConfig());

        // Verify the config is correct (and not some empty map)
        assertEquals("Should have correct number of entries", expectedTopologyConfig.size(), virtualSpout.getSpoutConfig().size());
        assertEquals("Should have correct entries", expectedTopologyConfig, virtualSpout.getSpoutConfig());

        // Check each item
        assertEquals("Value1", virtualSpout.getSpoutConfigItem("Key1"));
        assertEquals("Value2", virtualSpout.getSpoutConfigItem("Key2"));
        assertEquals("Value3", virtualSpout.getSpoutConfigItem("Key3"));

        // Check a random key that doesn't exist
        assertNull(virtualSpout.getSpoutConfigItem("Random Key"));
    }

    /**
     * Test setter and getter
     * Note - Setter may go away in liu of being set by the topologyConfig.  getVirtualSpoutId() should remain tho.
     */
    @Test
    public void testSetAndGetConsumerId() {
        final Map<String, Object> config = getDefaultConfig();

        // Define input
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("myConsumerId");

        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            expectedConsumerId,
            config,
            new MockTopologyContext(),
            new FactoryManager(config),
            new LogRecorder(),
            null,
            null
        );

        // Verify it
        assertEquals("Got expected consumer id", expectedConsumerId, virtualSpout.getVirtualSpoutId());
    }

    /**
     * Test setter and getter.
     */
    @Test
    public void testSetAndGetStopRequested() {
        final Map<String, Object> config = getDefaultConfig();
        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            config,
            new MockTopologyContext(),
            new FactoryManager(config),
            new LogRecorder(),
            null,
            null
        );

        // Should default to false
        assertFalse("Should default to false", virtualSpout.isStopRequested());

        // Set to true
        virtualSpout.requestStop();
        assertTrue("Should be true", virtualSpout.isStopRequested());
    }

    @Rule
    public ExpectedException expectedExceptionCallingOpenTwiceThrowsException = ExpectedException.none();

    /**
     * Calling open() more than once should throw an exception.
     */
    @Test
    public void testCallingOpenTwiceThrowsException() {
        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create mock topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);

        // Create factory manager
        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create virtual spout identifier
        final VirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            virtualSpoutIdentifier,
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            null
        );

        // Call it once.
        virtualSpout.open();

        // Validate that open() on SidelineConsumer is called once.
        verify(mockConsumer, times(1)).open(
            anyMap(),
            eq(virtualSpoutIdentifier),
            any(ConsumerPeerContext.class),
            any(ZookeeperPersistenceAdapter.class
        ), eq(null));

        // Set expected exception
        try {
            expectedExceptionCallingOpenTwiceThrowsException.expect(IllegalStateException.class);
            virtualSpout.open();
        } finally {
            // Ensure that we call close.
            virtualSpout.close();
        }
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

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);

        // Create a mock Deserializer
        final Deserializer mockDeserializer = mock(Deserializer.class);
        final RetryManager mockRetryManager = mock(RetryManager.class);
        final PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(mockDeserializer, mockRetryManager, null, null);
        when(mockFactoryManager.createNewPersistenceAdapterInstance()).thenReturn(mockPersistenceAdapter);
        when(mockFactoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create virtual spout identifier
        final VirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            virtualSpoutIdentifier,
            topologyConfig,
            mockTopologyContext,
            mockFactoryManager,
            new LogRecorder(),
            null,
            null
        );

        // Call open
        virtualSpout.open();

        // Validate that we asked factory manager for a failed msg retry manager
        verify(mockFactoryManager, times(1)).createNewFailedMsgRetryManagerInstance();

        // Validate we called open on the RetryManager
        verify(mockRetryManager, times(1)).open(topologyConfig);

        // Validate that open() on SidelineConsumer is called once.
        verify(mockConsumer, times(1)).open(
            anyMap(),
            eq(virtualSpoutIdentifier),
            any(ConsumerPeerContext.class),
            any(ZookeeperPersistenceAdapter.class),
            eq(null)
        );

        // Call close
        virtualSpout.close();
    }

    /**
     * Tests when you call nextTuple() and the underlying consumer.nextRecord() returns null,
     * then nextTuple() should also return null.
     */
    @Test
    public void testNextTupleWhenConsumerReturnsNull() {
        // Define some inputs
        final Record expectedConsumerRecord = null;

        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);

        // Create factory manager
        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockConsumer.nextRecord()).thenReturn(expectedConsumerRecord);

        // Create spout & open
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            null
        );
        virtualSpout.open();

        // Call nextTuple()
        final Message result = virtualSpout.nextTuple();

        // Verify its null
        assertNull("Should be null",  result);

        // Verify ack is never called on underlying mock consumer
        verify(mockConsumer, never()).commitOffset(anyString(), anyInt(), anyLong());

        // Call close
        virtualSpout.close();
    }

    /**
     * Tests what happens when you call nextTuple(), and the underlying consumer
     * returns null.
     */
    @Test
    public void testNextTupleWhenSerializerFailsToDeserialize() {
        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock Consumer
        final Consumer mockConsumer = mock(Consumer.class);

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(null, null, null, null);
        when(mockFactoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);
        when(mockFactoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return null
        when(mockConsumer.nextRecord()).thenReturn(null);

        // Create spout & open
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            topologyConfig,
            mockTopologyContext,
            mockFactoryManager,
            new LogRecorder(),
            null,
            null
        );
        virtualSpout.open();

        // Call nextTuple()
        final Message result = virtualSpout.nextTuple();

        // Verify its null
        assertNull("Should be null",  result);

        // Call close
        virtualSpout.close();
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
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("myConsumerId");
        final String expectedKey = "MyKey";
        final String expectedValue = "MyValue";
        final Record expectedConsumerRecord = new Record(
            expectedTopic,
            expectedPartition,
            expectedOffset,
            new Values(expectedKey, expectedValue)
        );

        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);
        when(mockConsumer.getCurrentState()).thenReturn(ConsumerState.builder().build());

        // Create factory manager
        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockConsumer.nextRecord()).thenReturn(expectedConsumerRecord);

        final StaticMessageFilter filterStep = new StaticMessageFilter();

        // Create spout & open
        final VirtualSpout virtualSpout = new VirtualSpout(
            expectedConsumerId,
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            null
        );
        virtualSpout.getFilterChain().addStep(new SidelineRequestIdentifier("Foobar"), filterStep);
        virtualSpout.open();

        // Call nextTuple()
        final Message result = virtualSpout.nextTuple();

        // Check result
        assertNull("Should be null", result);

        // Verify ack was called on the tuple
        verify(mockConsumer, times(1)).commitOffset(eq(expectedTopic), eq(expectedPartition), eq(expectedOffset));

        // Call close
        virtualSpout.close();
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
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("myConsumerId");
        final String expectedKey = "MyKey";
        final String expectedValue = "MyValue";
        final Record expectedConsumerRecord = new Record(
            expectedTopic,
            expectedPartition,
            expectedOffset,
            new Values(expectedKey, expectedValue)
        );

        // Define expected result
        final Message expectedMessage = new Message(
            new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId),
            new Values(expectedKey, expectedValue)
        );

        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);

        // Create factory manager
        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockConsumer.nextRecord()).thenReturn(expectedConsumerRecord);

        // Create spout & open
        final VirtualSpout virtualSpout = new VirtualSpout(
            expectedConsumerId,
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            null
        );
        virtualSpout.open();

        // Call nextTuple()
        final Message result = virtualSpout.nextTuple();

        // Check result
        assertNotNull("Should not be null", result);

        // Validate it
        assertEquals("Found expected namespace", expectedTopic, result.getNamespace());
        assertEquals("Found expected partition", expectedPartition, result.getPartition());
        assertEquals("Found expected offset", expectedOffset, result.getOffset());
        assertEquals("Found expected values", new Values(expectedKey, expectedValue), result.getValues());
        assertEquals("Got expected Message", expectedMessage, result);

        // Call close
        virtualSpout.close();
    }

    /**
     * 1. publish a bunch of messages to a topic with a single partition.
     * 2. create a VirtualSpout where we explicitly define an ending offset less than the total msgs published
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
        final Record consumerRecordBeforeEnd = new Record(topic, partition, beforeOffset, new Values("before-key", "before-value"));

        // This ConsumerRecord is EQUAL to the limit, and thus should pass.
        final Record consumerRecordEqualEnd = new Record(topic, partition, endingOffset, new Values("equal-key", "equal-value"));

        // These two should exceed the limit (since its >) and nothing should be returned.
        final Record consumerRecordAfterEnd = new Record(topic, partition, afterOffset, new Values("after-key", "after-value"));
        final Record consumerRecordAfterEnd2 = new Record(topic, partition, afterOffset + 1, new Values("after-key2", "after-value2"));

        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("myConsumerId");

        // Define expected results returned
        final Message expectedMessageBeforeEndingOffset = new Message(
            new MessageId(topic, partition, beforeOffset, expectedConsumerId),
            new Values("before-key", "before-value")
        );
        final Message expectedMessageEqualEndingOffset = new Message(
            new MessageId(topic, partition, endingOffset, expectedConsumerId),
            new Values("equal-key", "equal-value")
        );

        // Defining our Ending State
        final ConsumerState endingState = ConsumerState.builder()
            .withPartition(topic, partition, endingOffset)
            .build();

        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);
        when(mockConsumer.getCurrentState()).thenReturn(ConsumerState.builder().build());

        // Create factory manager
        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return our values in order.
        when(mockConsumer.nextRecord()).thenReturn(
            consumerRecordBeforeEnd,
            consumerRecordEqualEnd,
            consumerRecordAfterEnd,
            consumerRecordAfterEnd2
        );

        // Create spout & open
        final VirtualSpout virtualSpout = new VirtualSpout(
            expectedConsumerId,
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            endingState
        );
        virtualSpout.open();

        // Call nextTuple(), this should return our entry BEFORE the ending offset
        Message result = virtualSpout.nextTuple();

        // Check result
        assertNotNull("Should not be null because the offset is under the limit.", result);

        // Validate it
        assertEquals("Found expected namespace", topic, result.getNamespace());
        assertEquals("Found expected partition", partition, result.getPartition());
        assertEquals("Found expected offset", beforeOffset, result.getOffset());
        assertEquals("Found expected values", new Values("before-key", "before-value"), result.getValues());
        assertEquals("Got expected Message", expectedMessageBeforeEndingOffset, result);

        // Call nextTuple(), this offset should be equal to our ending offset
        // Equal to the end offset should still get emitted.
        result = virtualSpout.nextTuple();

        // Check result
        assertNotNull("Should not be null because the offset is under the limit.", result);

        // Validate it
        assertEquals("Found expected namespace", topic, result.getNamespace());
        assertEquals("Found expected partition", partition, result.getPartition());
        assertEquals("Found expected offset", endingOffset, result.getOffset());
        assertEquals("Found expected values", new Values("equal-key", "equal-value"), result.getValues());
        assertEquals("Got expected Message", expectedMessageEqualEndingOffset, result);

        // Call nextTuple(), this offset should be greater than our ending offset
        // and thus should return null.
        result = virtualSpout.nextTuple();

        // Check result
        assertNull("Should be null because the offset is greater than the limit.", result);

        // Call nextTuple(), again the offset should be greater than our ending offset
        // and thus should return null.
        result = virtualSpout.nextTuple();

        // Check result
        assertNull("Should be null because the offset is greater than the limit.", result);

        // Validate unsubscribed was called on our mock sidelineConsumer
        // Right now this is called twice... unsure if that is an issue. I don't think it is.
        verify(mockConsumer, times(2)).unsubscribeConsumerPartition(eq(new ConsumerPartition(topic, partition)));

        // Validate that we never called ack on the tuples that were filtered because they exceeded the max offset
        verify(mockConsumer, times(0)).commitOffset(topic, partition, afterOffset);
        verify(mockConsumer, times(0)).commitOffset(topic, partition, afterOffset + 1);

        // Call close
        virtualSpout.close();
    }

    /**
     * This test does the following:
     *
     * 1. Call nextTuple() -
     *  a. the first time RetryManager should return null, saying it has no failed tuples to replay
     *  b. consumer should return a consumer record, and it should be returned by nextTuple()
     * 2. Call fail() with the message previously returned from nextTuple().
     * 2. Call nextTuple()
     *  a. This time RetryManager should return the failed tuple
     * 3. Call nextTuple()
     *  a. This time RetryManager should return null, saying it has no failed tuples to replay.
     *  b. consumer should return a new consumer record.
     */
    @Test
    public void testCallingFailOnTupleWhenItShouldBeRetriedItGetsRetried() {
        // This is the record coming from the consumer.
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 3;
        final long expectedOffset = 434323L;
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("myConsumerId");
        final String expectedKey = "MyKey";
        final String expectedValue = "MyValue";
        final Record expectedConsumerRecord = new Record(
            expectedTopic,
            expectedPartition,
            expectedOffset,
            new Values(expectedKey, expectedValue)
        );

        // Define expected result
        final Message expectedMessage = new Message(
            new MessageId(expectedTopic, expectedPartition, expectedOffset, expectedConsumerId),
            new Values(expectedKey, expectedValue)
        );

        // This is a second record coming from the consumer
        final long unexpectedOffset = expectedOffset + 2L;
        final String unexpectedKey = "NotMyKey";
        final String unexpectedValue = "NotMyValue";
        final Record unexpectedConsumerRecord = new Record(
            expectedTopic,
            expectedPartition,
            unexpectedOffset,
            new Values(unexpectedKey, unexpectedValue)
        );

        // Define unexpected result
        final Message unexpectedMessage = new Message(
            new MessageId(expectedTopic, expectedPartition, unexpectedOffset, expectedConsumerId),
            new Values(unexpectedKey, unexpectedValue)
        );

        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);

        // When nextRecord() is called on the mockSidelineConsumer, we need to return a value
        when(mockConsumer.nextRecord()).thenReturn(expectedConsumerRecord, unexpectedConsumerRecord, null);

        // Create a mock RetryManager
        final RetryManager mockRetryManager = mock(RetryManager.class);

        // First time its called, it should return null.
        // The 2nd time it should return our tuple Id.
        // The 3rd time it should return null again.
        when(mockRetryManager.nextFailedMessageToRetry()).thenReturn(null, expectedMessage.getMessageId(), null);

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(null, mockRetryManager, null, null);
        when(mockFactoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create spout & open
        final VirtualSpout virtualSpout = new VirtualSpout(
            expectedConsumerId,
            topologyConfig,
            mockTopologyContext,
            mockFactoryManager,
            new LogRecorder(),
            null,
            null
        );
        virtualSpout.open();

        // Call nextTuple()
        Message result = virtualSpout.nextTuple();

        // Verify we asked the failed message manager, but got nothing back
        verify(mockRetryManager, times(1)).nextFailedMessageToRetry();

        // Check result
        assertNotNull("Should not be null", result);

        // Validate it
        assertEquals("Found expected namespace", expectedTopic, result.getNamespace());
        assertEquals("Found expected partition", expectedPartition, result.getPartition());
        assertEquals("Found expected offset", expectedOffset, result.getOffset());
        assertEquals("Found expected values", new Values(expectedKey, expectedValue), result.getValues());
        assertEquals("Got expected Message", expectedMessage, result);
        assertFalse("Should not be permanently failed", result.isPermanentlyFailed());

        // Now call fail on this
        final MessageId failedMessageId = result.getMessageId();

        // Retry manager should retry this tuple.
        when(mockRetryManager.retryFurther(failedMessageId)).thenReturn(true);

        // failed on retry manager shouldn't have been called yet
        verify(mockRetryManager, never()).failed(anyObject());

        // call fail on our message id
        virtualSpout.fail(failedMessageId);

        // Verify failed calls
        verify(mockRetryManager, times(1)).failed(failedMessageId);

        // Call nextTuple, we should get our failed tuple back.
        result = virtualSpout.nextTuple();

        // verify we got the tuple from failed manager
        verify(mockRetryManager, times(2)).nextFailedMessageToRetry();

        // Check result
        assertNotNull("Should not be null", result);

        // Validate it
        assertEquals("Found expected namespace", expectedTopic, result.getNamespace());
        assertEquals("Found expected partition", expectedPartition, result.getPartition());
        assertEquals("Found expected offset", expectedOffset, result.getOffset());
        assertEquals("Found expected values", new Values(expectedKey, expectedValue), result.getValues());
        assertEquals("Got expected Message", expectedMessage, result);
        assertFalse("Should not be permanently failed", result.isPermanentlyFailed());

        // And call nextTuple() one more time, this time failed manager should return null
        // and consumer returns our unexpected result
        // Call nextTuple, we should get our failed tuple back.
        result = virtualSpout.nextTuple();

        // verify we got the tuple from failed manager
        verify(mockRetryManager, times(3)).nextFailedMessageToRetry();

        // Check result
        assertNotNull("Should not be null", result);

        // Validate it
        assertEquals("Found expected namespace", expectedTopic, result.getNamespace());
        assertEquals("Found expected partition", expectedPartition, result.getPartition());
        assertEquals("Found expected offset", unexpectedOffset, result.getOffset());
        assertEquals("Found expected values", new Values(unexpectedKey, unexpectedValue), result.getValues());
        assertEquals("Got expected Message", unexpectedMessage, result);

        // Next Tuple should return null
        assertNull("No more tuples queued", virtualSpout.nextTuple());

        // Now test calling fail on our failed tuple, retry manager should say this is enough,
        when(mockRetryManager.retryFurther(failedMessageId)).thenReturn(false);
        virtualSpout.fail(failedMessageId);

        // When we call next Tuple() we should get our failed message, marked as permanently failed
        result = virtualSpout.nextTuple();
        assertNotNull("Should have non-null message", result);

        // Validate it
        assertEquals("Found expected namespace", expectedTopic, result.getNamespace());
        assertEquals("Found expected partition", expectedPartition, result.getPartition());
        assertEquals("Found expected offset", expectedOffset, result.getOffset());
        assertEquals("Found expected values", new Values(expectedKey, expectedValue), result.getValues());
        assertEquals("Got expected MessageId", expectedMessage.getMessageId(), result.getMessageId());
        assertTrue("Should be permanently failed", result.isPermanentlyFailed());

        // Call close
        virtualSpout.close();
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

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(null, mockRetryManager, null, null);
        when(mockFactoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            topologyConfig,
            mockTopologyContext,
            mockFactoryManager,
            new LogRecorder(),
            null,
            null
        );
        virtualSpout.open();

        // Call ack with null, nothing should explode.
        virtualSpout.fail(null);

        // No interactions w/ our mocks
        verify(mockRetryManager, never()).retryFurther(anyObject());
        verify(mockRetryManager, never()).acked(anyObject());
        verify(mockRetryManager, never()).failed(anyObject());
        verify(mockConsumer, never()).commitOffset(anyString(), anyInt(), anyLong());

        // Call close
        virtualSpout.close();
    }

    @Rule
    public ExpectedException expectedExceptionFailWithInvalidMsgIdObject = ExpectedException.none();

    /**
     * Call fail() with invalid msg type should throw an exception.
     */
    @Test
    public void testFailWithInvalidMsgIdObject() {
        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);

        // Create factory manager
        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            null
        );
        virtualSpout.open();

        // Call ack with a string object, it should throw an exception.
        try {
            expectedExceptionFailWithInvalidMsgIdObject.expect(IllegalArgumentException.class);
            virtualSpout.fail("This is a String!");
        } finally {
            // Call close
            virtualSpout.close();
        }
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

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(null, mockRetryManager, null, null);
        when(mockFactoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            topologyConfig,
            mockTopologyContext,
            mockFactoryManager,
            new LogRecorder(),
            null,
            null
        );
        virtualSpout.open();

        // Call ack with null, nothing should explode.
        virtualSpout.ack(null);

        // No interactions w/ our mock consumer for committing offsets
        verify(mockConsumer, never()).commitOffset(anyString(), anyInt(), anyLong());
        verify(mockRetryManager, never()).acked(anyObject());

        // Call close
        virtualSpout.close();
    }

    @Rule
    public ExpectedException expectedExceptionAckWithInvalidMsgIdObject = ExpectedException.none();

    /**
     * Call ack() with invalid msg type should throw an exception.
     */
    @Test
    public void testAckWithInvalidMsgIdObject() {
        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);

        // Create factory manager
        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            null
        );
        virtualSpout.open();

        // Call ack with a string object, it should throw an exception.
        try {
            expectedExceptionAckWithInvalidMsgIdObject.expect(IllegalArgumentException.class);
            virtualSpout.ack("This is my String!");
        } finally {
            // Call close
            virtualSpout.close();
        }
    }

    /**
     * Test calling ack, ensure it passes the commit command to its internal consumer.
     */
    @Test
    public void testAck() {
        // Define our msgId
        final String expectedTopicName = "MyTopic";
        final int expectedPartitionId = 33;
        final long expectedOffset = 313376L;
        final MessageId messageId = new MessageId(
            expectedTopicName,
            expectedPartitionId,
            expectedOffset,
            new DefaultVirtualSpoutIdentifier("RandomConsumer")
        );

        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final RetryManager mockRetryManager = mock(RetryManager.class);

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);
        when(mockConsumer.getCurrentState()).thenReturn(ConsumerState.builder().build());

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(null, mockRetryManager, null, null);
        when(mockFactoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            topologyConfig,
            mockTopologyContext,
            mockFactoryManager,
            new LogRecorder(),
            null,
            null
        );
        virtualSpout.open();

        // Never called yet
        verify(mockConsumer, never()).commitOffset(anyString(), anyInt(), anyLong());
        verify(mockRetryManager, never()).acked(anyObject());

        // Call ack with a string object, it should throw an exception.
        virtualSpout.ack(messageId);

        // Verify mock gets called with appropriate arguments
        verify(mockConsumer, times(1)).commitOffset(eq(expectedTopicName), eq(expectedPartitionId), eq(expectedOffset));

        // Gets acked on the failed retry manager
        verify(mockRetryManager, times(1)).acked(messageId);

        // Call close
        virtualSpout.close();
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

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);

        // Create factory manager
        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            null
        );
        virtualSpout.open();

        // Create our test MessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final long expectedOffset = 31332L;
        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");
        final MessageId messageId = new MessageId(expectedTopic, expectedPartition, expectedOffset, consumerId);

        // Call our method & validate.
        final boolean result = virtualSpout.doesMessageExceedEndingOffset(messageId);
        assertFalse("Should always be false", result);

        // Close things out
        virtualSpout.close();
    }

    /**
     * Test calling this method with a defined endingState, and the MessageId's offset is equal to it,
     * it should return false.
     */
    @Test
    public void testDoesMessageExceedEndingOffsetWhenItEqualsEndingOffset() {
        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final Consumer mockConsumer = mock(Consumer.class);

        // Create our test MessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final long expectedOffset = 31332L;
        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("myConsumerId");
        final MessageId messageId = new MessageId(expectedTopic, expectedPartition, expectedOffset, consumerId);

        // Define our endingState with a position equal to our MessageId
        final ConsumerState endingState = ConsumerState.builder()
            .withPartition(expectedTopic, expectedPartition, expectedOffset)
            .build();

        // Create factory manager
        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create spout passing in ending state.
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            endingState
        );
        virtualSpout.open();

        // Call our method & validate.
        final boolean result = virtualSpout.doesMessageExceedEndingOffset(messageId);
        assertFalse("Should be false", result);

        // Call close
        virtualSpout.close();
    }

    /**
     * Test calling this method with a defined endingState, and the MessageId's offset is beyond it.
     */
    @Test
    public void testDoesMessageExceedEndingOffsetWhenItDoesExceedEndingOffset() {
        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final Consumer mockConsumer = mock(Consumer.class);

        // Create our test MessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final long expectedOffset = 31332L;
        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");
        final MessageId messageId = new MessageId(expectedTopic, expectedPartition, expectedOffset, consumerId);

        // Define our endingState with a position less than our MessageId
        final ConsumerState endingState = ConsumerState.builder()
            .withPartition(expectedTopic, expectedPartition, (expectedOffset - 100))
            .build();

        // Create factory manager
        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create spout passing in ending state.
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            endingState
        );
        virtualSpout.open();

        // Call our method & validate.
        final boolean result = virtualSpout.doesMessageExceedEndingOffset(messageId);
        assertTrue("Should be true", result);

        // Call close
        virtualSpout.close();
    }

    /**
     * Test calling this method with a defined endingState, and the MessageId's offset is before it.
     */
    @Test
    public void testDoesMessageExceedEndingOffsetWhenItDoesNotExceedEndingOffset() {
        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final Consumer mockConsumer = mock(Consumer.class);

        // Create our test MessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final long expectedOffset = 31332L;
        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");
        final MessageId messageId = new MessageId(expectedTopic, expectedPartition, expectedOffset, consumerId);

        // Define our endingState with a position greater than than our MessageId
        final ConsumerState endingState = ConsumerState.builder()
            .withPartition(expectedTopic, expectedPartition, (expectedOffset + 100))
            .build();

        // Create factory manager
        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create spout passing in ending state.
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            endingState
        );
        virtualSpout.open();

        // Call our method & validate.
        final boolean result = virtualSpout.doesMessageExceedEndingOffset(messageId);
        assertFalse("Should be false", result);

        // Call close
        virtualSpout.close();
    }

    @Rule
    public ExpectedException expectedExceptionDoesMessageExceedEndingOffsetForAnInvalidPartition = ExpectedException.none();

    /**
     * Test calling this method with a defined endingState, and then send in a MessageId
     * associated with a partition that doesn't exist in the ending state.  It should throw
     * an illegal state exception.
     */
    @Test
    public void testDoesMessageExceedEndingOffsetForAnInvalidPartition() {
        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final Consumer mockConsumer = mock(Consumer.class);

        // Create our test MessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final long expectedOffset = 31332L;
        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");
        final MessageId messageId = new MessageId(expectedTopic, expectedPartition, expectedOffset, consumerId);

        // Define our endingState with a position greater than than our MessageId, but on a different partition
        final ConsumerState endingState = ConsumerState.builder()
            .withPartition(expectedTopic, expectedPartition + 1, (expectedOffset + 100))
            .build();

        // Create factory manager
        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create spout passing in ending state.
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            endingState
        );
        virtualSpout.open();

        // Call our method & validate exception is thrown
        expectedExceptionDoesMessageExceedEndingOffsetForAnInvalidPartition.expect(IllegalStateException.class);
        virtualSpout.doesMessageExceedEndingOffset(messageId);

        // Call close
        virtualSpout.close();
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
        final Consumer mockConsumer = mock(Consumer.class);
        when(mockConsumer.unsubscribeConsumerPartition(any(ConsumerPartition.class))).thenReturn(expectedResult);

        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            null
        );
        virtualSpout.open();

        // Create our test MessageId
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 1;
        final ConsumerPartition topicPartition = new ConsumerPartition(expectedTopic, expectedPartition);

        // Call our method & validate.
        final boolean result = virtualSpout.unsubscribeTopicPartition(topicPartition.namespace(), topicPartition.partition());
        assertEquals("Got expected result from our method", expectedResult, result);

        // Validate mock call
        verify(mockConsumer, times(1)).unsubscribeConsumerPartition(eq(topicPartition));

        // Call close
        virtualSpout.close();
    }

    /**
     * Test calling close, verifies what happens if the completed flag is false.
     */
    @Test
    public void testCloseWithCompletedFlagSetToFalse() throws NoSuchFieldException, IllegalAccessException {
        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final SidelineRequestIdentifier sidelineRequestId = new SidelineRequestIdentifier("SidelineRequestId");

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);

        // Create a mock PersistanceManager & associate with SidelineConsumer.
        final PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);
        when(mockConsumer.getPersistenceAdapter()).thenReturn(mockPersistenceAdapter);

        // Create factory manager
        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            new SidelineVirtualSpoutIdentifier("MyConsumerId", sidelineRequestId),
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            null
        );
        virtualSpout.open();

        // Mark sure is completed field is set to false before calling close
        final Field isCompletedField = virtualSpout.getClass().getDeclaredField("isCompleted");
        isCompletedField.setAccessible(true);
        isCompletedField.set(virtualSpout, false);

        // Verify close hasn't been called yet.
        verify(mockConsumer, never()).close();

        // Call close
        virtualSpout.close();

        // Verify close was called, and state was flushed
        verify(mockConsumer, times(1)).flushConsumerState();
        verify(mockConsumer, times(1)).close();

        // But we never called remove consumer state.
        verify(mockConsumer, never()).removeConsumerState();
    }

    /**
     * Test calling close, verifies what happens if the completed flag is true.
     * Verifies what happens if SidelineRequestIdentifier is set.
     */
    @Test
    public void testCloseWithCompletedFlagSetToTrue() throws NoSuchFieldException, IllegalAccessException {
        // Create inputs
        final Map topologyConfig = getDefaultConfig();
        topologyConfig.put(SpoutConfig.VIRTUAL_SPOUT_HANDLER_CLASS, SidelineVirtualSpoutHandler.class.getName());
        final TopologyContext mockTopologyContext = new MockTopologyContext();
        final SidelineRequestIdentifier sidelineRequestId = new SidelineRequestIdentifier("SidelineRequestId");

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);

        // Create a mock PersistanceManager & associate with SidelineConsumer.
        final PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);
        when(mockConsumer.getPersistenceAdapter()).thenReturn(mockPersistenceAdapter);

        // Create factory manager
        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        final ConsumerState.ConsumerStateBuilder startingStateBuilder = ConsumerState.builder();
        startingStateBuilder.withPartition("foobar", 0, 1L);
        final ConsumerState startingState = startingStateBuilder.build();

        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            new SidelineVirtualSpoutIdentifier("MyConsumerId", sidelineRequestId),
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            startingState,
            null
        );
        virtualSpout.open();

        // Mark sure is completed field is set to true before calling close
        final Field isCompletedField = virtualSpout.getClass().getDeclaredField("isCompleted");
        isCompletedField.setAccessible(true);
        isCompletedField.set(virtualSpout, true);

        // Verify close hasn't been called yet.
        verify(mockConsumer, never()).close();

        // Call close
        virtualSpout.close();

        // Verify close was called, and state was cleared
        verify(mockConsumer, times(1)).removeConsumerState();
        verify(mockConsumer, times(1)).close();

        // But we never called flush consumer state.
        verify(mockConsumer, never()).flushConsumerState();
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
        final Consumer mockConsumer = mock(Consumer.class);

        // Create a mock PersistanceManager & associate with SidelineConsumer.
        final PersistenceAdapter mockPersistenceAdapter = mock(PersistenceAdapter.class);
        when(mockConsumer.getPersistenceAdapter()).thenReturn(mockPersistenceAdapter);

        // Create factory manager
        final FactoryManager factoryManager = spy(new FactoryManager(topologyConfig));
        when(factoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create spout
        final VirtualSpout virtualSpout = new VirtualSpout(
            new SidelineVirtualSpoutIdentifier("MyConsumerId", new SidelineRequestIdentifier("main")),
            topologyConfig,
            mockTopologyContext,
            factoryManager,
            new LogRecorder(),
            null,
            null
        );
        virtualSpout.open();

        // Mark sure is completed field is set to true before calling close
        final Field isCompletedField = virtualSpout.getClass().getDeclaredField("isCompleted");
        isCompletedField.setAccessible(true);
        isCompletedField.set(virtualSpout, true);

        // Verify close hasn't been called yet.
        verify(mockConsumer, never()).close();

        // Call close
        virtualSpout.close();

        // Verify close was called, and state was cleared
        verify(mockConsumer, times(1)).removeConsumerState();
        verify(mockConsumer, times(1)).close();

        // But we never called flush consumer state.
        verify(mockConsumer, never()).flushConsumerState();
    }

    /**
     * This test does the following:
     *   Sets up a VirtualSpout with injected mock retry manager and consumer.
     *   We instruct the mock consumer to return a Record when nextRecord() is called.
     *   We ask VirtualSpout for nextTuple() and it should return us a Message wrapped around the record.
     *   We call fail on this Message's MessageId.
     *   We ask VirtualSpout for nextTuple() and it should return us the same message, but this time marked as permanently failed.
     *   Also at this point it should be marked as acked.
     */
    @Test
    public void testCallingFailCallsAckOnWhenItShouldNotBeRetried() {
        // This is the record coming from the consumer.
        final String expectedTopic = "MyTopic";
        final int expectedPartition = 3;
        final long expectedOffset = 434323L;
        final DefaultVirtualSpoutIdentifier expectedConsumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");
        final String expectedKey = "MyKey";
        final String expectedValue = "MyValue";

        // Define expected values
        final Values expectedValues = new Values(expectedKey, expectedValue);

        // Define expected Record
        final Record expectedRecord = new Record(expectedTopic, expectedPartition, expectedOffset, expectedValues);

        // Create test config
        final Map topologyConfig = getDefaultConfig();

        // Create mock topology context
        final TopologyContext mockTopologyContext = new MockTopologyContext();

        // Create a mock SidelineConsumer
        final Consumer mockConsumer = mock(Consumer.class);

        // Create a mock RetryManager
        final RetryManager mockRetryManager = mock(RetryManager.class);

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(null, mockRetryManager, null, null);
        when(mockFactoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // When the underlying consumer is asked for the next record, it should return our record
        when(mockConsumer.nextRecord()).thenReturn(expectedRecord);

        // Create spout & open
        final VirtualSpout virtualSpout = new VirtualSpout(
            expectedConsumerId,
            topologyConfig,
            mockTopologyContext,
            mockFactoryManager,
            new LogRecorder(),
            null,
            null
        );
        virtualSpout.open();

        // Ask for the next message, it should be our wrapped mockRecord.
        final Message message = virtualSpout.nextTuple();

        // Verify its not permanently failed
        assertFalse("Should not be permanently failed", message.isPermanentlyFailed());

        // Grab messageId out
        final MessageId failedMessageId = message.getMessageId();

        // Retry manager should NOT retry this tuple.
        when(mockRetryManager.retryFurther(failedMessageId)).thenReturn(false);

        // call fail on our message id
        virtualSpout.fail(failedMessageId);

        // Verify since this wasn't retried, it gets acked both by the consumer and the retry manager.
        verify(mockRetryManager, times(1)).acked(failedMessageId);
        verify(mockConsumer, times(1)).commitOffset(
            failedMessageId.getNamespace(),
            failedMessageId.getPartition(),
            failedMessageId.getOffset()
        );

        // Calling nextMessage should give us the permanently failed message
        final Message permanentlyFailedMessage = virtualSpout.nextTuple();
        assertNotNull("Should be non-null", permanentlyFailedMessage);

        // Should be the same messageId
        assertEquals("Should have same messageId as previously", failedMessageId, permanentlyFailedMessage.getMessageId());

        // Should be marked as permanently failed
        assertTrue("Should be marked as permanently failed", permanentlyFailedMessage.isPermanentlyFailed());

        // Call close
        virtualSpout.close();
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
        final Consumer mockConsumer = mock(Consumer.class);

        // Create factory manager
        final FactoryManager mockFactoryManager = createMockFactoryManager(null, mockRetryManager, null, null);
        when(mockFactoryManager.createNewConsumerInstance()).thenReturn(mockConsumer);

        // Create spout & open
        final VirtualSpout virtualSpout = new VirtualSpout(
            new DefaultVirtualSpoutIdentifier("MyConsumerId"),
            topologyConfig,
            mockTopologyContext,
            mockFactoryManager,
            new LogRecorder(),
            // We provide a dud of a starting state so that getConsumerState() is not called twice
            ConsumerState.builder().build(),
            null
        );
        virtualSpout.open();

        final ConsumerState expectedConsumerState = ConsumerState
            .builder()
            .withPartition("myTopic", 0, 200L)
            .build();

        // Setup our mock to return expected value
        when(mockConsumer.getCurrentState()).thenReturn(expectedConsumerState);

        // Call get current state.
        final ConsumerState result = virtualSpout.getCurrentState();

        // Verify mock interactions
        verify(mockConsumer, times(1)).getCurrentState();

        // Verify result
        assertNotNull("result should not be null", result);
        assertEquals("Should be our expected instance", expectedConsumerState, result);

        // Call close
        virtualSpout.close();
    }

    /**
     * Utility method to generate a standard config map.
     */
    private Map<String, Object> getDefaultConfig() {
        final Map<String, Object> defaultConfig = new HashMap<>();

        // Kafka Consumer Config items
        defaultConfig.put(KafkaConsumerConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:9092"));
        defaultConfig.put(KafkaConsumerConfig.KAFKA_TOPIC, "MyTopic");
        defaultConfig.put(KafkaConsumerConfig.CONSUMER_ID_PREFIX, "TestPrefix");
        defaultConfig.put(KafkaConsumerConfig.DESERIALIZER_CLASS, Utf8StringDeserializer.class.getName());

        // DynamicSpout config items
        defaultConfig.put(SpoutConfig.PERSISTENCE_ZK_ROOT, "/sideline-spout-test");
        defaultConfig.put(SpoutConfig.PERSISTENCE_ZK_SERVERS, Lists.newArrayList("localhost:21811"));
        defaultConfig.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, ZookeeperPersistenceAdapter.class.getName());
        defaultConfig.put(
            SidelineConfig.PERSISTENCE_ADAPTER_CLASS,
            com.salesforce.storm.spout.sideline.persistence.InMemoryPersistenceAdapter.class.getName()
        );

        defaultConfig.put(SpoutConfig.METRICS_RECORDER_CLASS, LogRecorder.class.getName());

        return SpoutConfig.setDefaults(defaultConfig);
    }

    /**
     * Utility method for creating a mock factory manager.
     */
    private FactoryManager createMockFactoryManager(
        Deserializer deserializer,
        RetryManager retryManager,
        PersistenceAdapter persistenceAdapter,
        VirtualSpoutHandler virtualSpoutHandler
    ) {
        // Create our mock
        final FactoryManager factoryManager = mock(FactoryManager.class);

        // If a mocked failed msg retry manager isn't passed in
        if (retryManager == null) {
            retryManager = new NeverRetryManager();
        }
        when(factoryManager.createNewFailedMsgRetryManagerInstance()).thenReturn(retryManager);

        if (persistenceAdapter == null) {
            persistenceAdapter = new InMemoryPersistenceAdapter();
        }
        when(factoryManager.createNewPersistenceAdapterInstance()).thenReturn(persistenceAdapter);

        if (virtualSpoutHandler == null) {
            virtualSpoutHandler = new NoopVirtualSpoutHandler();
        }
        when(factoryManager.createVirtualSpoutHandler()).thenReturn(virtualSpoutHandler);
        when(factoryManager.createNewMetricsRecorder()).thenReturn(new LogRecorder());

        return factoryManager;
    }
}