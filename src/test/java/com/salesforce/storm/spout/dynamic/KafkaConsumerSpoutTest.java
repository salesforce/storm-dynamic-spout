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

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.kafka.test.KafkaTestServer;
import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.ProducedKafkaRecord;
import com.salesforce.kafka.test.junit.SharedKafkaTestResource;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.kafka.Consumer;
import com.salesforce.storm.spout.dynamic.kafka.KafkaConsumerConfig;
import com.salesforce.storm.spout.dynamic.kafka.deserializer.Utf8StringDeserializer;
import com.salesforce.storm.spout.dynamic.metrics.LogRecorder;
import com.salesforce.storm.spout.dynamic.mocks.MockTopologyContext;
import com.salesforce.storm.spout.dynamic.mocks.output.MockSpoutOutputCollector;
import com.salesforce.storm.spout.dynamic.mocks.output.SpoutEmission;
import com.salesforce.storm.spout.dynamic.persistence.InMemoryPersistenceAdapter;
import com.salesforce.storm.spout.dynamic.persistence.ZookeeperPersistenceAdapter;
import com.salesforce.storm.spout.dynamic.retry.FailedTuplesFirstRetryManager;
import com.salesforce.storm.spout.dynamic.retry.NeverRetryManager;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Provides End-To-End integration testing of DynamicSpout + Kafka Consumer.
 * This test does not attempt to validate DynamicSpout's behavior that is covered by DynamicSpoutTest.
 */
@RunWith(DataProviderRunner.class)
public class KafkaConsumerSpoutTest {
    // For logging within the test.
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerSpoutTest.class);

    /**
     * Create shared kafka test server.
     */
    @ClassRule
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();

    /**
     * We generate a unique topic name for every test case.
     */
    private String topicName;

    /**
     * This happens once before every test method.
     * Create a new empty namespace with randomly generated name.
     */
    @Before
    public void beforeTest() throws InterruptedException {
        // Generate namespace name
        topicName = KafkaConsumerSpoutTest.class.getSimpleName() + Clock.systemUTC().millis();

        // Create namespace
        getKafkaTestServer().createTopic(topicName);
    }

    /**
     * Our most simple end-2-end test.
     * This test stands up our spout and ask it to consume from our kafka namespace.
     * We publish some data into kafka, and validate that when we call nextTuple() on
     * our spout that we get out our messages that were published into kafka.
     *
     * Uses a single VirtualSpout running the Kafka Consumer.
     *
     * We run this test multiple times using a DataProvider to test using but an implicit/unconfigured
     * output stream name (default), as well as an explicitly configured stream name.
     */
    @Test
    @UseDataProvider("provideStreamIds")
    public void doBasicConsumingTest(final String configuredStreamId, final String expectedStreamId) throws InterruptedException {
        // Define how many tuples we should push into the namespace, and then consume back out.
        final int emitTupleCount = 10;

        // Define our ConsumerId prefix
        final String consumerIdPrefix = "KafkaConsumerSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, null);

        // If we have a stream Id we should be configured with
        if (configuredStreamId != null) {
            // Drop it into our configuration.
            config.put(SpoutConfig.OUTPUT_STREAM_ID, configuredStreamId);
        }
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Some mock storm topology stuff to get going
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        final DynamicSpout spout = new DynamicSpout(new SpoutConfig(config));
        spout.open(config, topologyContext, spoutOutputCollector);

        // Add a VirtualSpout.
        final VirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("Main");
        final VirtualSpout virtualSpout = new VirtualSpout(
            virtualSpoutIdentifier,
            spoutConfig,
            topologyContext,
            new FactoryManager(spoutConfig),
            new LogRecorder(),
            null,
            null
        );
        spout.addVirtualSpout(virtualSpout);

        // Wait for VirtualSpout to start
        waitForVirtualSpouts(spout, 1);

        // Call next tuple, topic is empty, so nothing should get emitted.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 2, 0L);

        // Lets produce some data into the topic
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(emitTupleCount, 0);

        // Now consume tuples generated from the messages we published into kafka.
        final List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, emitTupleCount);

        // Validate the tuples that got emitted are what we expect based on what we published into kafka
        validateTuplesFromSourceMessages(producedRecords, spoutEmissions, expectedStreamId, virtualSpoutIdentifier);

        // Call next tuple a few more times to make sure nothing unexpected shows up.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 3, 0L);

        // Cleanup.
        spout.close();
    }

    /**
     * End-to-End test over the fail() method using the {@link FailedTuplesFirstRetryManager}
     * retry manager.
     *
     * This test stands up our spout and ask it to consume from our kafka namespace.
     * We publish some data into kafka, and validate that when we call nextTuple() on
     * our spout that we get out our messages that were published into kafka.
     *
     * We then fail some tuples and validate that they get replayed.
     * We ack some tuples and then validate that they do NOT get replayed.
     *
     * Just simple consuming from a single VirtualSpout using the KafkaConsumer.
     */
    @Test
    public void doBasicFailTest() throws InterruptedException {
        // Define how many tuples we should push into the namespace, and then consume back out.
        final int emitTupleCount = 10;

        // Define our ConsumerId prefix
        final String consumerIdPrefix = "KafkaConsumerSpout";

        // Define our output stream id
        final String expectedStreamId = "default";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);

        // Configure to use our FailedTuplesFirstRetryManager retry manager.
        config.put(SpoutConfig.RETRY_MANAGER_CLASS, FailedTuplesFirstRetryManager.class.getName());

        // Create spout config
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Some mock stuff to get going
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        final DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Add a VirtualSpout.
        final VirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("Main");
        final VirtualSpout virtualSpout = new VirtualSpout(
            virtualSpoutIdentifier,
            spoutConfig,
            topologyContext,
            new FactoryManager(spoutConfig),
            new LogRecorder(),
            null,
            null
        );
        spout.addVirtualSpout(virtualSpout);

        // Call next tuple, namespace is empty, so should get nothing.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 100L);

        // Lets produce some data into the namespace
        List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(emitTupleCount, 0);

        // Now loop and get our tuples
        List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, emitTupleCount);

        // Now lets validate that what we got out of the spout is what we actually expected.
        validateTuplesFromSourceMessages(producedRecords, spoutEmissions, expectedStreamId, virtualSpoutIdentifier);

        // Call next tuple a few more times to make sure nothing unexpected shows up.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 100L);

        // Now lets fail our tuples.
        failTuples(spout, spoutEmissions);

        // And lets call nextTuple, and we should get the same emissions back out because we called fail on them
        // And our retry manager should replay them first chance it gets.
        spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, emitTupleCount);
        validateTuplesFromSourceMessages(producedRecords, spoutEmissions, expectedStreamId, virtualSpoutIdentifier);

        // Now lets ack 2 different offsets, entries 0 and 3.
        // This means they should never get replayed.
        List<SpoutEmission> ackedEmissions = Lists.newArrayList();
        ackedEmissions.add(spoutEmissions.get(0));
        ackedEmissions.add(spoutEmissions.get(3));
        ackTuples(spout, ackedEmissions);

        // And lets remove their related KafkaRecords
        // Remember to remove in reverse order, because ArrayLists have no gaps in indexes :p
        producedRecords.remove(3);
        producedRecords.remove(0);

        // And lets fail the others
        List<SpoutEmission> failEmissions = Lists.newArrayList(spoutEmissions);
        failEmissions.removeAll(ackedEmissions);
        failTuples(spout, failEmissions);

        // This is how many we failed.
        final int failedTuples = failEmissions.size();

        // If we call nextTuple, we should get back our failed emissions
        final List<SpoutEmission> replayedEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, failedTuples);

        // Validate we don't get anything else
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 0L);

        // Validate the replayed tuples were our failed ones
        validateTuplesFromSourceMessages(producedRecords, replayedEmissions, expectedStreamId, virtualSpoutIdentifier);

        // Now lets ack these
        ackTuples(spout, replayedEmissions);

        // And validate nextTuple gives us nothing new
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 100L);

        // Cleanup.
        spout.close();
    }

    /**
     * This test stands up a spout instance and begins consuming from a namespace.
     * Halfway thru consuming all the messages published in that namespace we will shutdown
     * the spout gracefully.
     *
     * We'll create a new instance of the spout and fire it up, then validate that it resumes
     * consuming from where it left off.
     *
     * Assumptions made in this test:
     *   - single partition namespace
     *   - using ZK persistence manager to maintain state between spout instances/restarts.
     */
    @Test
    public void testResumingForFirehoseVirtualSpout() throws InterruptedException, IOException {
        // Produce 10 messages into kafka (offsets 0->9)
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = Collections.unmodifiableList(produceRecords(10, 0));

        // Create spout
        // Define our output stream id
        final String expectedStreamId = "default";
        final String consumerIdPrefix = "TestKafkaConsumerSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);

        // Use zookeeper persistence manager
        config.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, ZookeeperPersistenceAdapter.class.getName());

        // Create spout config
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Some mock stuff to get going
        TopologyContext topologyContext = new MockTopologyContext();
        MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        DynamicSpout spout = new DynamicSpout(new SpoutConfig(config));
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Add a VirtualSpout.
        final VirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("Main");
        VirtualSpout virtualSpout = new VirtualSpout(
            virtualSpoutIdentifier,
            spoutConfig,
            topologyContext,
            new FactoryManager(spoutConfig),
            new LogRecorder(),
            null,
            null
        );
        spout.addVirtualSpout(virtualSpout);

        // Call next tuple 6 times, getting offsets 0,1,2,3,4,5
        final List<SpoutEmission> spoutEmissions = Collections.unmodifiableList(consumeTuplesFromSpout(spout, spoutOutputCollector, 6));

        // Validate its the messages we expected
        validateEmission(producedRecords.get(0), spoutEmissions.get(0), virtualSpoutIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(1), spoutEmissions.get(1), virtualSpoutIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(2), spoutEmissions.get(2), virtualSpoutIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(3), spoutEmissions.get(3), virtualSpoutIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(4), spoutEmissions.get(4), virtualSpoutIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(5), spoutEmissions.get(5), virtualSpoutIdentifier, expectedStreamId);

        // We will ack offsets in the following order: 2,0,1,3,5
        // This should give us a completed offset of [0,1,2,3] <-- last committed offset should be 3
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(2)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(0)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(1)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(3)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(5)));

        // Stop the spout.
        // A graceful shutdown of the spout should have the consumer state flushed to the persistence layer.
        spout.close();

        // Create fresh new spoutOutputCollector & topology context
        topologyContext = new MockTopologyContext();
        spoutOutputCollector = new MockSpoutOutputCollector();

        // Create fresh new instance of spout & call open all with the same config
        spout = new DynamicSpout(spoutConfig);
        spout.open(config, topologyContext, spoutOutputCollector);

        // Create and add virtual spout
        virtualSpout = new VirtualSpout(
            virtualSpoutIdentifier,
            spoutConfig,
            topologyContext,
            new FactoryManager(spoutConfig),
            new LogRecorder(),
            null,
            null
        );
        spout.addVirtualSpout(virtualSpout);

        // Call next tuple to get remaining tuples out.
        // It should give us offsets [4,5,6,7,8,9]
        final List<SpoutEmission> spoutEmissionsAfterResume = Collections.unmodifiableList(
            consumeTuplesFromSpout(spout, spoutOutputCollector, 6)
        );

        // Validate no further tuples
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 0L);

        // Validate its the tuples we expect [4,5,6,7,8,9]
        validateEmission(producedRecords.get(4), spoutEmissionsAfterResume.get(0), virtualSpoutIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(5), spoutEmissionsAfterResume.get(1), virtualSpoutIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(6), spoutEmissionsAfterResume.get(2), virtualSpoutIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(7), spoutEmissionsAfterResume.get(3), virtualSpoutIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(8), spoutEmissionsAfterResume.get(4), virtualSpoutIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(9), spoutEmissionsAfterResume.get(5), virtualSpoutIdentifier, expectedStreamId);

        // Ack all tuples.
        ackTuples(spout, spoutEmissionsAfterResume);

        // Stop the spout.
        // A graceful shutdown of the spout should have the consumer state flushed to the persistence layer.
        spout.close();

        // Create fresh new spoutOutputCollector & topology context
        topologyContext = new MockTopologyContext();
        spoutOutputCollector = new MockSpoutOutputCollector();

        // Create fresh new instance of spout & call open all with the same config
        spout = new DynamicSpout(new SpoutConfig(config));
        spout.open(config, topologyContext, spoutOutputCollector);

        // Create new VirtualSpout instance and add it.
        virtualSpout = new VirtualSpout(
            virtualSpoutIdentifier,
            spoutConfig,
            topologyContext,
            new FactoryManager(spoutConfig),
            new LogRecorder(),
            null,
            null
        );
        spout.addVirtualSpout(virtualSpout);

        // Validate no further tuples, as we acked all the things.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 3000L);

        // And done.
        spout.close();
    }

    /**
     * This is an integration test of multiple Kafka Consumers.
     * We stand up a topic with 4 partitions.
     * We then have a consumer size of 2.
     * We run the test once using consumerIndex 0
     *   - Verify we only consume from partitions 0 and 1
     * We run the test once using consumerIndex 1
     *   - Verify we only consume from partitions 2 and 3
     * @param taskIndex What taskIndex to run the test with.
     */
    @Test
    @UseDataProvider("providerOfTaskIds")
    public void testConsumeWithConsumerGroupEvenNumberOfPartitions(final int taskIndex) {
        final int numberOfMsgsPerPartition = 10;

        // Create a namespace with 4 partitions
        topicName = "testConsumeWithConsumerGroupEvenNumberOfPartitions" + Clock.systemUTC().millis();
        getKafkaTestServer().createTopic(topicName, 4);

        // Define some topicPartitions
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
        if (taskIndex == 0) {
            // If we're consumerIndex 0, we expect partitionIds 0 or 1
            expectedPartitions = Lists.newArrayList(partition0 , partition1);
        } else if (taskIndex == 1) {
            // If we're consumerIndex 0, we expect partitionIds 2 or 3
            expectedPartitions = Lists.newArrayList(partition2 , partition3);
        } else {
            throw new RuntimeException("Invalid input to test");
        }

        // Create spout
        // Define our output stream id
        final String expectedStreamId = "default";
        final String consumerIdPrefix = "TestKafkaConsumerSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);

        // Use zookeeper persistence manager
        config.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, ZookeeperPersistenceAdapter.class.getName());

        // Create spout config
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Create topology context, set our task index
        MockTopologyContext topologyContext = new MockTopologyContext();
        topologyContext.taskId = taskIndex;
        topologyContext.taskIndex = taskIndex;

        // Say that we have 2 tasks, ids 0 and 1
        topologyContext.componentTasks = Collections.unmodifiableList(Lists.newArrayList(0, 1));

        MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create our spout and call open().
        DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Create and add virtualSpout
        final VirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("Main");
        final VirtualSpout virtualSpout = new VirtualSpout(
            virtualSpoutIdentifier,
            spoutConfig,
            topologyContext,
            new FactoryManager(spoutConfig),
            new LogRecorder(),
            null,
            null
        );
        spout.addVirtualSpout(virtualSpout);

        // Wait for our virtual spout to start
        waitForVirtualSpouts(spout, 1);

        // Call next tuple 21 times, getting offsets 0-9 on the first partition, 0-10 on the 2nd partition
        final List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, (numberOfMsgsPerPartition * 2) + 1);

        // Validate they all came from the correct partitions
        for (SpoutEmission spoutEmission : spoutEmissions) {
            assertNotNull("Has non-null tupleId", spoutEmission.getMessageId());

            // Validate it came from the right place
            final MessageId messageId = (MessageId) spoutEmission.getMessageId();
            final ConsumerPartition consumerPartition = new ConsumerPartition(messageId.getNamespace(), messageId.getPartition());
            assertTrue("Came from expected partition", expectedPartitions.contains(consumerPartition));
        }

        // Validate we don't have any other emissions
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 5, 0L);

        // Lets ack our tuples
        ackTuples(spout, spoutEmissions);

        // Close
        spout.close();
    }

    /**
     * This is an integration test of multiple Kafka Consumers.
     * We stand up a topic with 4 partitions.
     * We then have a consumer size of 2.
     * We run the test once using consumerIndex 0
     *   - Verify we only consume from partitions 0 and 1
     * We run the test once using consumerIndex 1
     *   - Verify we only consume from partitions 2 and 3
     * @param taskIndex What taskIndex to run the test with.
     */
    @Test
    @UseDataProvider("providerOfTaskIds")
    public void testConsumeWithConsumerGroupOddNumberOfPartitions(final int taskIndex) {
        final int numberOfMsgsPerPartition = 10;

        // Create a namespace with 4 partitions
        topicName = "testConsumeWithConsumerGroupOddNumberOfPartitions" + Clock.systemUTC().millis();
        getKafkaTestServer().createTopic(topicName, 5);

        // Define some topicPartitions
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
        final int expectedNumberOfTuplesToConsume;
        if (taskIndex == 0) {
            // If we're consumerIndex 0, we expect partitionIds 0,1, or 2
            expectedPartitions = Lists.newArrayList(partition0 , partition1, partition2);

            // We expect to get out 31 tuples
            expectedNumberOfTuplesToConsume = 31;
        } else if (taskIndex == 1) {
            // If we're consumerIndex 0, we expect partitionIds 3 or 4
            expectedPartitions = Lists.newArrayList(partition3 , partition4);

            // We expect to get out 21 tuples
            expectedNumberOfTuplesToConsume = 21;
        } else {
            throw new RuntimeException("Invalid input to test");
        }

        // Create spout
        // Define our output stream id
        final String expectedStreamId = "default";
        final String consumerIdPrefix = "TestKafkaConsumerSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);

        // Use zookeeper persistence manager
        config.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, ZookeeperPersistenceAdapter.class.getName());

        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Create topology context, set our task index
        MockTopologyContext topologyContext = new MockTopologyContext();
        topologyContext.taskId = taskIndex;
        topologyContext.taskIndex = taskIndex;

        // Say that we have 2 tasks, ids 0 and 1
        topologyContext.componentTasks = Collections.unmodifiableList(Lists.newArrayList(0,1));

        MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create our spout and call open().
        DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Create and add virtualSpout
        final VirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("Main");
        final VirtualSpout virtualSpout = new VirtualSpout(
            virtualSpoutIdentifier,
            spoutConfig,
            topologyContext,
            new FactoryManager(spoutConfig),
            new LogRecorder(),
            null,
            null
        );
        spout.addVirtualSpout(virtualSpout);

        // Wait for our virtual spout to start
        waitForVirtualSpouts(spout, 1);

        // Call next tuple , getting offsets 0-9 on the even partitions, 0-10 on the odd partitions
        final List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, expectedNumberOfTuplesToConsume);

        // Validate they all came from the correct partitions
        for (SpoutEmission spoutEmission : spoutEmissions) {
            assertNotNull("Has non-null tupleId", spoutEmission.getMessageId());

            // Validate it came from the right place
            final MessageId messageId = (MessageId) spoutEmission.getMessageId();
            final ConsumerPartition consumerPartition = new ConsumerPartition(messageId.getNamespace(), messageId.getPartition());
            assertTrue("Came from expected partition", expectedPartitions.contains(consumerPartition));
        }

        // Validate we don't have any other emissions
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 5, 0L);

        // Lets ack our tuples
        ackTuples(spout, spoutEmissions);

        // Close
        spout.close();
    }

    /**
     * Provides task ids 0 and 1.
     */
    @DataProvider
    public static Object[][] providerOfTaskIds() {
        return new Object[][]{
            {0},
            {1}
        };
    }

    // Helper methods

    /**
     * Given a list of KafkaRecords that got published into Kafka, compare it against a list of SpoutEmissions that
     * got emitted by the spout, and make sure everything matches up to what we expected.
     *
     * @param sourceProducerRecord - The KafkaRecord messages published into kafka.
     * @param spoutEmission - The SpoutEmissions we got out of the spout
     * @param expectedVirtualSpoutId - What virtualSpoutId these emissions should be associated with.
     * @param expectedOutputStreamId - What stream these emissions should have been emitted down.
     */
    private void validateEmission(
        final ProducedKafkaRecord<byte[], byte[]> sourceProducerRecord,
        final SpoutEmission spoutEmission,
        final VirtualSpoutIdentifier expectedVirtualSpoutId,
        final String expectedOutputStreamId
    ) {
        // Now find its corresponding tuple
        assertNotNull("Not null sanity check", spoutEmission);
        assertNotNull("Not null sanity check", sourceProducerRecord);

        // Validate Message Id
        assertNotNull("Should have non-null messageId", spoutEmission.getMessageId());
        assertTrue("Should be instance of MessageId", spoutEmission.getMessageId() instanceof MessageId);

        // Grab the messageId and validate it
        final MessageId messageId = (MessageId) spoutEmission.getMessageId();
        assertEquals("Expected Topic Name in MessageId", sourceProducerRecord.getTopic(), messageId.getNamespace());
        assertEquals("Expected PartitionId found", sourceProducerRecord.getPartition(), messageId.getPartition());
        assertEquals("Expected MessageOffset found", sourceProducerRecord.getOffset(), messageId.getOffset());
        assertEquals("Expected Source Consumer Id", expectedVirtualSpoutId, messageId.getSrcVirtualSpoutId());

        // Validate Tuple Contents
        List<Object> tupleValues = spoutEmission.getTuple();
        assertNotNull("Tuple Values should not be null", tupleValues);
        assertFalse("Tuple Values should not be empty", tupleValues.isEmpty());

        // For now the values in the tuple should be 'key' and 'value', this may change.
        assertEquals("Should have 2 values in the tuple", 2, tupleValues.size());
        assertEquals("Found expected 'key' value", new String(sourceProducerRecord.getKey(), Charsets.UTF_8), tupleValues.get(0));
        assertEquals("Found expected 'value' value", new String(sourceProducerRecord.getValue(), Charsets.UTF_8), tupleValues.get(1));

        // Validate Emit Parameters
        assertEquals("Got expected streamId", expectedOutputStreamId, spoutEmission.getStreamId());
    }

    /**
     * Utility method to ack tuples on a spout.  This will wait for the underlying VirtualSpout instance
     * to actually ack them before returning.
     *
     * @param spout - the Spout instance to ack tuples on.
     * @param spoutEmissions - The SpoutEmissions we want to ack.
     */
    private void ackTuples(final DynamicSpout spout, List<SpoutEmission> spoutEmissions) {
        if (spoutEmissions.isEmpty()) {
            throw new RuntimeException("You cannot ack an empty list!  You probably have a bug in your test.");
        }

        // Ack each one.
        for (SpoutEmission emission: spoutEmissions) {
            spout.ack(emission.getMessageId());
        }

        // Grab reference to message bus.
        final MessageBus messageBus = (MessageBus) spout.getMessageBus();

        // Acking tuples is an async process, so we need to make sure they get picked up
        // and processed before continuing.
        await()
            .atMost(6500, TimeUnit.MILLISECONDS)
            .until(() -> {
                // Wait for our tuples to get popped off the acked queue.
                return messageBus.ackSize() == 0;
            }, equalTo(true));
    }

    /**
     * Utility method to fail tuples on a spout.  This will wait for the underlying VirtualSpout instance
     * to actually fail them before returning.
     *
     * @param spout - the Spout instance to ack tuples on.
     * @param spoutEmissions - The SpoutEmissions we want to ack.
     */
    private void failTuples(final DynamicSpout spout, List<SpoutEmission> spoutEmissions) {
        if (spoutEmissions.isEmpty()) {
            throw new RuntimeException("You cannot fail an empty list!  You probably have a bug in your test.");
        }

        // Fail each one.
        for (SpoutEmission emission: spoutEmissions) {
            spout.fail(emission.getMessageId());
        }

        // Grab reference to message bus.
        final MessageBus messageBus = (MessageBus) spout.getMessageBus();

        // Failing tuples is an async process, so we need to make sure they get picked up
        // and processed before continuing.
        await()
            .atMost(6500, TimeUnit.MILLISECONDS)
            .until(() -> {
                // Wait for our tuples to get popped off the fail queue.
                return messageBus.failSize() == 0;
            }, equalTo(true));
    }

    /**
     * Utility method that calls nextTuple() on the passed in spout, and then validates that it never emitted anything.
     * @param spout - The spout instance to call nextTuple() on.
     * @param collector - The spout's output collector that would receive the tuples if any were emitted.
     * @param numberOfAttempts - How many times to call nextTuple()
     */
    private void validateNextTupleEmitsNothing(
        DynamicSpout spout,
        MockSpoutOutputCollector collector,
        int numberOfAttempts,
        long delayInMs
    ) {
        try {
            Thread.sleep(delayInMs);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Try a certain number of times
        final int originalSize = collector.getEmissions().size();
        for (int x = 0; x < numberOfAttempts; x++) {
            // Call next Tuple
            spout.nextTuple();

            // If we get an unexpected emission
            if (originalSize != collector.getEmissions().size()) {
                // Lets log it
                logger.error("Got an unexpected emission: {}", collector.getEmissions().get(collector.getEmissions().size() - 1));
            }
            assertEquals("No new tuple emits on iteration " + (x + 1), originalSize, collector.getEmissions().size());
        }
    }

    /**
     * Utility method that calls nextTuple() on the passed in spout, and then returns new tuples that the spout emitted.
     * @param spout - The spout instance to call nextTuple() on.
     * @param collector - The spout's output collector that would receive the tuples if any were emitted.
     * @param numberOfTuples - How many new tuples we expect to get out of the spout instance.
     */
    private List<SpoutEmission> consumeTuplesFromSpout(DynamicSpout spout, MockSpoutOutputCollector collector, int numberOfTuples) {
        logger.info("[TEST] Attempting to consume {} tuples from spout", numberOfTuples);

        // Create a new list for the emissions we expect to get back
        List<SpoutEmission> newEmissions = Lists.newArrayList();

        // Determine how many emissions are already in the collector
        final int existingEmissionsCount = collector.getEmissions().size();

        // Call next tuple N times
        for (int x = 0; x < numberOfTuples; x++) {
            // Async call spout.nextTuple() because it can take a bit to fill the buffer.
            await()
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> {
                    // Ask for next tuple
                    spout.nextTuple();

                    // Return how many tuples have been emitted so far
                    // It should be equal to our loop count + 1
                    return collector.getEmissions().size();
                }, equalTo(existingEmissionsCount + x + 1));

            // Should have some emissions
            assertEquals("SpoutOutputCollector should have emissions", (existingEmissionsCount + x + 1), collector.getEmissions().size());

            // Add our new emission to our return list
            newEmissions.add(collector.getEmissions().get(existingEmissionsCount + x));
        }

        // Log them for reference.
        logger.info("Found new emissions: {}", newEmissions);
        return newEmissions;
    }

    /**
     * Given a list of produced kafka messages, and a list of tuples that got emitted,
     * make sure that the tuples match up with what we expected to get sent out.
     *  @param producedRecords the original records produced into kafka.
     * @param spoutEmissions the tuples that got emitted out from the spout
     * @param expectedStreamId the stream id that we expected the tuples to get emitted out on.
     * @param expectedVirtualSpoutId virtual spout id we expected
     */
    private void validateTuplesFromSourceMessages(
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecords,
        final List<SpoutEmission> spoutEmissions,
        final String expectedStreamId,
        final VirtualSpoutIdentifier expectedVirtualSpoutId
    ) {
        // Sanity check, make sure we have the same number of each.
        assertEquals(
            "Should have same number of tuples as original messages, Produced Count: "
            + producedRecords.size() + " Emissions Count: " + spoutEmissions.size(),
            producedRecords.size(),
            spoutEmissions.size()
        );

        // Iterator over what got emitted
        final Iterator<SpoutEmission> emissionIterator = spoutEmissions.iterator();

        // Loop over what we produced into kafka
        for (ProducedKafkaRecord<byte[], byte[]> producedRecord: producedRecords) {
            // Now find its corresponding tuple from our iterator
            final SpoutEmission spoutEmission = emissionIterator.next();

            // validate that they match
            validateEmission(producedRecord, spoutEmission, expectedVirtualSpoutId, expectedStreamId);
        }
    }

    /**
     * Waits for virtual spouts to close out.
     * @param spout - The spout instance
     * @param howManyVirtualSpoutsWeWantLeft - Wait until this many virtual spouts are left running.
     */
    private void waitForVirtualSpouts(DynamicSpout spout, int howManyVirtualSpoutsWeWantLeft) {
        await()
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> spout.getSpoutCoordinator().getTotalSpouts(), equalTo(howManyVirtualSpoutsWeWantLeft));
        assertEquals(
            "We should have " + howManyVirtualSpoutsWeWantLeft + " virtual spouts running",
            howManyVirtualSpoutsWeWantLeft,
            spout.getSpoutCoordinator().getTotalSpouts()
        );
    }

    /**
     * helper method to produce records into kafka.
     */
    private List<ProducedKafkaRecord<byte[], byte[]>> produceRecords(int numberOfRecords, int partitionId) {
        KafkaTestUtils kafkaTestUtils = new KafkaTestUtils(getKafkaTestServer());
        return kafkaTestUtils.produceRecords(numberOfRecords, topicName, partitionId);
    }

    /**
     * Generates a Storm Topology configuration with some sane values for our test scenarios.
     *
     * @param consumerIdPrefix - consumerId prefix to use.
     * @param configuredStreamId - What streamId we should emit tuples out of.
     */
    private Map<String, Object> getDefaultConfig(final String consumerIdPrefix, final String configuredStreamId) {
        // Generate a unique zkRootNode for each test
        final String uniqueZkRootNode = "/kafkaconsumer-spout-test/testRun" + System.currentTimeMillis();

        final Map<String, Object> config = new HashMap<>();

        // Kafka Consumer config items
        config.put(SpoutConfig.CONSUMER_CLASS, Consumer.class.getName());
        config.put(KafkaConsumerConfig.DESERIALIZER_CLASS, Utf8StringDeserializer.class.getName());
        config.put(KafkaConsumerConfig.KAFKA_TOPIC, topicName);
        config.put(KafkaConsumerConfig.CONSUMER_ID_PREFIX, consumerIdPrefix);
        config.put(KafkaConsumerConfig.KAFKA_BROKERS, Lists.newArrayList(getKafkaTestServer().getKafkaConnectString()));

        // DynamicSpout config items
        config.put(SpoutConfig.RETRY_MANAGER_CLASS, NeverRetryManager.class.getName());
        config.put(SpoutConfig.PERSISTENCE_ZK_SERVERS, Lists.newArrayList(getKafkaTestServer().getZookeeperConnectString()));
        config.put(SpoutConfig.PERSISTENCE_ZK_ROOT, uniqueZkRootNode);

        // Use In Memory Persistence manager, if you need state persistence, over ride this in your test.
        config.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, InMemoryPersistenceAdapter.class.getName());

        // Configure SpoutCoordinator thread to run every 1 second
        config.put(SpoutConfig.MONITOR_THREAD_INTERVAL_MS, 1000L);

        // Configure flushing consumer state every 1 second
        config.put(SpoutConfig.CONSUMER_STATE_FLUSH_INTERVAL_MS, 1000L);

        // For now use the Log Recorder
        config.put(SpoutConfig.METRICS_RECORDER_CLASS, LogRecorder.class.getName());

        // If we have a stream Id we should be configured with
        if (configuredStreamId != null) {
            // Drop it into our configuration.
            config.put(SpoutConfig.OUTPUT_STREAM_ID, configuredStreamId);
        }

        return config;
    }

    /**
     * Provides various StreamIds to test emitting out of.
     */
    @DataProvider
    public static Object[][] provideStreamIds() {
        return new Object[][]{
            // No explicitly defined streamId should use the default streamId.
            { null, Utils.DEFAULT_STREAM_ID },

            // Explicitly defined streamId should get used as is.
            { "SpecialStreamId", "SpecialStreamId" }
        };
    }

    /**
     * Simple accessor.
     */
    private KafkaTestServer getKafkaTestServer() {
        return sharedKafkaTestResource.getKafkaTestServer();
    }
}
