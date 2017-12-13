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
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.consumer.Record;
import com.salesforce.storm.spout.dynamic.kafka.KafkaConsumerConfig;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.utils.SharedKafkaTestResource;
import com.salesforce.storm.spout.sideline.SidelineSpout;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;
import com.salesforce.storm.spout.sideline.filter.StaticMessageFilter;
import com.salesforce.storm.spout.sideline.handler.SidelineSpoutHandler;
import com.salesforce.storm.spout.sideline.handler.SidelineVirtualSpoutHandler;
import com.salesforce.storm.spout.dynamic.kafka.Consumer;
import com.salesforce.storm.spout.dynamic.utils.KafkaTestServer;
import com.salesforce.storm.spout.dynamic.kafka.deserializer.Utf8StringDeserializer;
import com.salesforce.storm.spout.dynamic.persistence.ZookeeperPersistenceAdapter;
import com.salesforce.storm.spout.dynamic.retry.FailedTuplesFirstRetryManager;
import com.salesforce.storm.spout.dynamic.retry.NeverRetryManager;
import com.salesforce.storm.spout.dynamic.metrics.LogRecorder;
import com.salesforce.storm.spout.dynamic.mocks.MockTopologyContext;
import com.salesforce.storm.spout.dynamic.mocks.output.MockSpoutOutputCollector;
import com.salesforce.storm.spout.dynamic.mocks.output.SpoutEmission;
import com.salesforce.storm.spout.dynamic.persistence.InMemoryPersistenceAdapter;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.StaticTrigger;
import com.salesforce.storm.spout.dynamic.utils.KafkaTestUtils;
import com.salesforce.storm.spout.dynamic.utils.ProducedKafkaRecord;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.storm.generated.StreamInfo;
import com.google.common.base.Charsets;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsGetter;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * End to End integration testing of Sideline Spout under various scenarios.
 */
// TODO: Remove sideline specific stuff from this test and create a separate test for those use cases.
@RunWith(DataProviderRunner.class)
public class DynamicSpoutTest {
    // For logging within the test.
    private static final Logger logger = LoggerFactory.getLogger(DynamicSpoutTest.class);

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
        topicName = DynamicSpoutTest.class.getSimpleName() + Clock.systemUTC().millis();

        // Create namespace
        getKafkaTestServer().createTopic(topicName);
    }

    @Rule
    public ExpectedException expectedExceptionMissingRequiredConfigurationConsumerIdPrefix = ExpectedException.none();

    /**
     * Validates that we require the ConsumerIdPrefix configuration value,
     * and if its missing we toss an IllegalStateException.
     */
    @Test
    public void testMissingRequiredConfigurationConsumerIdPrefix() {
        // Create our config missing the consumerIdPrefix
        final Map<String, Object> config = getDefaultConfig(null, null);
        config.remove(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX);

        // Some mock stuff to get going
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        final SidelineSpout spout = new SidelineSpout(config);

        // When we call open, we expect illegal state exception about our missing configuration item
        expectedExceptionMissingRequiredConfigurationConsumerIdPrefix.expect(IllegalStateException.class);
        expectedExceptionMissingRequiredConfigurationConsumerIdPrefix.expectMessage(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX);

        // Call open
        try {
            spout.open(config, topologyContext, spoutOutputCollector);
        } finally {
            spout.close();
        }
    }

    /**
     * Our most simple end-2-end test.
     * This test stands up our spout and ask it to consume from our kafka namespace.
     * We publish some data into kafka, and validate that when we call nextTuple() on
     * our spout that we get out our messages that were published into kafka.
     *
     * This does not make use of any side lining logic, just simple consuming from the
     * 'fire hose' consumer.
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
        final String consumerIdPrefix = "TestSidelineSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, null);

        // If we have a stream Id we should be configured with
        if (configuredStreamId != null) {
            // Drop it into our configuration.
            config.put(SpoutConfig.OUTPUT_STREAM_ID, configuredStreamId);
        }

        // Some mock storm topology stuff to get going
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        final DynamicSpout spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Call next tuple, namespace is empty, so nothing should get emitted.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 2, 0L);

        // Lets produce some data into the namespace
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(emitTupleCount, 0);

        // Now consume tuples generated from the messages we published into kafka.
        final List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, emitTupleCount);

        // Validate the tuples that got emitted are what we expect based on what we published into kafka
        validateTuplesFromSourceMessages(producedRecords, spoutEmissions, expectedStreamId, consumerIdPrefix + ":main", false);

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
     * This does not make use of any side lining logic, just simple consuming from the
     * 'fire hose' namespace.
     */
    @Test
    public void doBasicFailTest() throws InterruptedException {
        // Define how many tuples we should push into the namespace, and then consume back out.
        final int emitTupleCount = 10;

        // Define our ConsumerId prefix
        final String consumerIdPrefix = "SidelineSpout";

        // Define our output stream id
        final String expectedStreamId = "default";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);

        // Configure to use our FailedTuplesFirstRetryManager retry manager.
        config.put(SpoutConfig.RETRY_MANAGER_CLASS, FailedTuplesFirstRetryManager.class.getName());

        // Some mock stuff to get going
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        final DynamicSpout spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Call next tuple, namespace is empty, so should get nothing.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 100L);

        // Lets produce some data into the namespace
        List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(emitTupleCount, 0);

        // Now loop and get our tuples
        List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, emitTupleCount);

        // Now lets validate that what we got out of the spout is what we actually expected.
        validateTuplesFromSourceMessages(producedRecords, spoutEmissions, expectedStreamId, consumerIdPrefix + ":main", false);

        // Call next tuple a few more times to make sure nothing unexpected shows up.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 100L);

        // Now lets fail our tuples.
        failTuples(spout, spoutEmissions);

        // And lets call nextTuple, and we should get the same emissions back out because we called fail on them
        // And our retry manager should replay them first chance it gets.
        spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, emitTupleCount);
        validateTuplesFromSourceMessages(producedRecords, spoutEmissions, expectedStreamId, consumerIdPrefix + ":main", false);

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
        validateTuplesFromSourceMessages(producedRecords, replayedEmissions, expectedStreamId, consumerIdPrefix + ":main", false);

        // Now lets ack these
        ackTuples(spout, replayedEmissions);

        // And validate nextTuple gives us nothing new
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 100L);

        // Cleanup.
        spout.close();
    }

    /**
     * End-to-End test over the fail() method using the {@link NeverRetryManager}
     * retry manager.
     *
     * This test stands up our DynamicSpout with a single VirtualSpout using a "Mock Consumer"
     * We inject some data into mock consumer, and validate that when we call nextTuple() on
     * our spout that we get out our messages.
     *
     * We then fail some of those tuples and validate that they get marked as permanently failed.
     * This means that they should be emitted out the "failed" stream and should never be replayed
     * out the standard output stream.
     */
    @Test
    public void doFailWithPermanentlyFailedMessagesTest() throws InterruptedException {
        // Define how many tuples we should push into the namespace, and then consume back out.
        final int emitTupleCount = 10;

        // Define our ConsumerId prefix
        final String consumerIdPrefix = "DynamicSpout";

        // Define our output stream id
        final String expectedStreamId = "default";
        final String expectedFailedStreamId = "failed";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);

        // Configure to use our FailedTuplesFirstRetryManager retry manager.
        // This implementation will always replay failed tuples first.
        config.put(SpoutConfig.RETRY_MANAGER_CLASS, NeverRetryManager.class.getName());

        // Some mock stuff to get going
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        final DynamicSpout spout = new DynamicSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());
        assertEquals(
            "Should be using appropriate failed output stream id",
            expectedFailedStreamId,
            spout.getPermanentlyFailedOutputStreamId()
        );

        final String virtualSpoutName = "MyVSpoutId" + System.currentTimeMillis();

        // Create new unique VSpoutId
        final VirtualSpoutIdentifier virtualSpoutIdentifier =
            new DefaultVirtualSpoutIdentifier(virtualSpoutName);

        // Add a VirtualSpout.
        final VirtualSpout virtualSpout = new VirtualSpout(
            virtualSpoutIdentifier,
            config,
            topologyContext,
            new FactoryManager(config),
            new LogRecorder(),
            null,
            null
        );
        spout.addVirtualSpout(virtualSpout);

        // Wait for VirtualSpout to start
        waitForVirtualSpouts(spout, 1);

        // Call next tuple, consumer is empty, so nothing should get emitted.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 2, 0L);

        // Lets produce some data into the mock consumer
        List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(emitTupleCount, 0);

        // Now loop and get our tuples
        List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, emitTupleCount);

        // Now lets validate that what we got out of the spout is what we actually expected.
        validateTuplesFromSourceMessages(producedRecords, spoutEmissions, expectedStreamId, virtualSpoutName, false);

        // Call next tuple a few more times to make sure nothing unexpected shows up.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 100L);

        // Now lets fail our tuples.
        failTuples(spout, spoutEmissions);

        // And lets call nextTuple, Our retry manager should permanently fail them.
        // This means we should get more or less the same emissions, but out the permanently failed stream.
        // They should also be un anchored.
        spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, emitTupleCount);
        validateTuplesFromSourceMessages(producedRecords, spoutEmissions, expectedFailedStreamId, virtualSpoutName, true);

        // Validate we don't get anything else
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 0L);

        // Cleanup.
        spout.close();
    }

    /**
     * Our most basic End 2 End test that includes basic sidelining.
     * First we stand up our spout and produce some records into the kafka namespace its consuming from.
     * Records 1 and 2 we get out of the spout.
     * Then we enable sidelining, and call nextTuple(), the remaining records should not be emitted.
     * We stop sidelining, this should cause a virtual spout to be started within the spout.
     * Calling nextTuple() should get back the records that were previously skipped.
     * We produce additional records into the namespace.
     * Call nextTuple() and we should get them out.
     */
    @Test
    public void doTestWithSidelining() throws InterruptedException {
        // How many records to publish into kafka per go.
        final int numberOfRecordsToPublish = 3;

        // Define our ConsumerId prefix
        final String consumerIdPrefix = "TestSidelineSpout";

        // Define our output stream id
        final String expectedStreamId = "default";

        // Create our Config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);

        // Create some stand-in mocks.
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create our spout, add references to our static trigger, and call open().
        final DynamicSpout spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Produce records into kafka
        List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(numberOfRecordsToPublish, 0);

        // Wait for our 'firehose' spout instance should pull these 3 records in when we call nextTuple().
        // Consuming from kafka is an async process de-coupled from the call to nextTuple().  Because of this it could
        // take several calls to nextTuple() before the messages are pulled in from kafka behind the scenes and available
        // to be emitted.
        // Grab out the emissions so we can validate them.
        List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, numberOfRecordsToPublish);

        // Validate the tuples are what we published into kafka
        validateTuplesFromSourceMessages(producedRecords, spoutEmissions, expectedStreamId, consumerIdPrefix + ":main", false);

        // Lets ack our tuples, this should commit offsets 0 -> 2. (0,1,2)
        ackTuples(spout, spoutEmissions);

        // Sanity test, we should have a single VirtualSpout instance at this point, the fire hose instance
        assertEquals("Should have a single VirtualSpout instance", 1, spout.getCoordinator().getTotalSpouts());

        // Create a static message filter, this allows us to easily start filtering messages.
        // It should filter ALL messages
        final SidelineRequest request = new SidelineRequest(new SidelineRequestIdentifier("test"), new StaticMessageFilter());

        // Send a new start request with our filter.
        // This means that our starting offset for the sideline'd data should start at offset 3 (we acked offsets 0, 1, 2)
        StaticTrigger.sendStartRequest(request);

        // Produce another 3 records into kafka.
        producedRecords = produceRecords(numberOfRecordsToPublish, 0);

        // We basically want the time that would normally pass before we check that there are no new tuples
        // Call next tuple, it should NOT receive any tuples because
        // all tuples are filtered.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 3000L);

        // Send a stop sideline request
        StaticTrigger.sendStopRequest(request);

        // We need to wait a bit for the sideline spout instance to spin up
        waitForVirtualSpouts(spout, 2);

        // Then ask the spout for tuples, we should get back the tuples that were produced while
        // sidelining was active.  These tuples should come from the VirtualSpout started by the Stop request.
        spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, numberOfRecordsToPublish);

        // We should validate these emissions
        validateTuplesFromSourceMessages(
            producedRecords,
            spoutEmissions,
            expectedStreamId,
            consumerIdPrefix + ":sideline:" + StaticTrigger.getCurrentSidelineRequestIdentifier(),
            false
        );

        // Call next tuple a few more times to be safe nothing else comes in.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 0L);

        // Validate that VirtualSpouts are NOT closed out, but still waiting for unacked tuples.
        // We should have 2 instances at this point, the firehose, and 1 sidelining instance.
        assertEquals("We should have 2 virtual spouts running", 2, spout.getCoordinator().getTotalSpouts());

        // Lets ack our messages.
        ackTuples(spout, spoutEmissions);

        // Validate that VirtualSpouts instance closes out once finished acking all processed tuples.
        // We need to wait for the monitor thread to run to clean it up.
        waitForVirtualSpouts(spout, 1);

        // Produce some more records, verify they come in the firehose.
        producedRecords = produceRecords(numberOfRecordsToPublish, 0);

        // Wait up to 5 seconds, our 'firehose' spout instance should pull these 3 records in when we call nextTuple().
        spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, numberOfRecordsToPublish);

        // Loop over what we produced into kafka and validate them
        validateTuplesFromSourceMessages(producedRecords, spoutEmissions, expectedStreamId, consumerIdPrefix + ":main", false);

        // Close out
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
    public void testResumingForFirehoseVirtualSpout() throws InterruptedException, IOException, KeeperException {
        // Produce 10 messages into kafka (offsets 0->9)
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = Collections.unmodifiableList(produceRecords(10, 0));

        // Create spout
        // Define our output stream id
        final String expectedStreamId = "default";
        final String consumerIdPrefix = "TestSidelineSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);

        // Use zookeeper persistence manager
        config.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, ZookeeperPersistenceAdapter.class.getName());

        // Some mock stuff to get going
        TopologyContext topologyContext = new MockTopologyContext();
        MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        DynamicSpout spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Call next tuple 6 times, getting offsets 0,1,2,3,4,5
        final List<SpoutEmission> spoutEmissions = Collections.unmodifiableList(consumeTuplesFromSpout(spout, spoutOutputCollector, 6));

        // Validate its the messages we expected
        validateEmission(producedRecords.get(0), spoutEmissions.get(0), consumerIdPrefix + ":main", expectedStreamId, false);
        validateEmission(producedRecords.get(1), spoutEmissions.get(1), consumerIdPrefix + ":main", expectedStreamId, false);
        validateEmission(producedRecords.get(2), spoutEmissions.get(2), consumerIdPrefix + ":main", expectedStreamId, false);
        validateEmission(producedRecords.get(3), spoutEmissions.get(3), consumerIdPrefix + ":main", expectedStreamId, false);
        validateEmission(producedRecords.get(4), spoutEmissions.get(4), consumerIdPrefix + ":main", expectedStreamId, false);
        validateEmission(producedRecords.get(5), spoutEmissions.get(5), consumerIdPrefix + ":main", expectedStreamId, false);

        // We will ack offsets in the following order: 2,0,1,3,5
        // This should give us a completed offset of [0,1,2,3] <-- last committed offset should be 3
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
        spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // Call next tuple to get remaining tuples out.
        // It should give us offsets [4,5,6,7,8,9]
        final List<SpoutEmission> spoutEmissionsAfterResume = Collections.unmodifiableList(
            consumeTuplesFromSpout(spout, spoutOutputCollector, 6)
        );

        // Validate no further tuples
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 0L);

        // Validate its the tuples we expect [4,5,6,7,8,9]
        validateEmission(producedRecords.get(4), spoutEmissionsAfterResume.get(0), consumerIdPrefix + ":main", expectedStreamId, false);
        validateEmission(producedRecords.get(5), spoutEmissionsAfterResume.get(1), consumerIdPrefix + ":main", expectedStreamId, false);
        validateEmission(producedRecords.get(6), spoutEmissionsAfterResume.get(2), consumerIdPrefix + ":main", expectedStreamId, false);
        validateEmission(producedRecords.get(7), spoutEmissionsAfterResume.get(3), consumerIdPrefix + ":main", expectedStreamId, false);
        validateEmission(producedRecords.get(8), spoutEmissionsAfterResume.get(4), consumerIdPrefix + ":main", expectedStreamId, false);
        validateEmission(producedRecords.get(9), spoutEmissionsAfterResume.get(5), consumerIdPrefix + ":main", expectedStreamId, false);

        // Ack all tuples.
        ackTuples(spout, spoutEmissionsAfterResume);

        // Stop the spout.
        // A graceful shutdown of the spout should have the consumer state flushed to the persistence layer.
        spout.close();

        // Create fresh new spoutOutputCollector & topology context
        topologyContext = new MockTopologyContext();
        spoutOutputCollector = new MockSpoutOutputCollector();

        // Create fresh new instance of spout & call open all with the same config
        spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // Validate no further tuples, as we acked all the things.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 3000L);

        // And done.
        spout.close();
    }

    /**
     * This test stands up a spout instance and tests sidelining.
     * Half way thru consuming the tuples that should be emitted from the sidelined VirtualSpout
     * we stop the spout, create a new instance and restart it.  If things are working correctly
     * the sidelined VirtualSpout should resume from where it left off.
     */
    @Test
    public void testResumingSpoutWhileSidelinedVirtualSpoutIsActive() throws InterruptedException {
        // Produce 10 messages into kafka (offsets 0->9)
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = Collections.unmodifiableList(produceRecords(10, 0));

        // Create spout
        // Define our output stream id
        final String expectedStreamId = "default";
        final String consumerIdPrefix = "TestSidelineSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);

        // Use zookeeper persistence manager
        config.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, ZookeeperPersistenceAdapter.class.getName());
        config.put(
            SidelineConfig.PERSISTENCE_ADAPTER_CLASS,
            com.salesforce.storm.spout.sideline.persistence.ZookeeperPersistenceAdapter.class.getName()
        );

        // Some mock stuff to get going
        TopologyContext topologyContext = new MockTopologyContext();
        MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);

        // Create our spout, add references to our static trigger, and call open().
        DynamicSpout spout = new SidelineSpout(config);

        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Call next tuple 6 times, getting offsets 0,1,2,3,4,5
        final List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, 6);

        // Validate its the messages we expected
        validateEmission(producedRecords.get(0), spoutEmissions.get(0), consumerIdPrefix + ":main", expectedStreamId, false);
        validateEmission(producedRecords.get(1), spoutEmissions.get(1), consumerIdPrefix + ":main", expectedStreamId, false);
        validateEmission(producedRecords.get(2), spoutEmissions.get(2), consumerIdPrefix + ":main", expectedStreamId, false);
        validateEmission(producedRecords.get(3), spoutEmissions.get(3), consumerIdPrefix + ":main", expectedStreamId, false);
        validateEmission(producedRecords.get(4), spoutEmissions.get(4), consumerIdPrefix + ":main", expectedStreamId, false);
        validateEmission(producedRecords.get(5), spoutEmissions.get(5), consumerIdPrefix + ":main", expectedStreamId, false);

        // We will ack offsets in the following order: 2,0,1,3,5
        // This should give us a completed offset of [0,1,2,3] <-- last committed offset should be 3
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(2)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(0)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(1)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(3)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(5)));

        // Create a static message filter, this allows us to easily start filtering messages.
        // It should filter ALL messages
        final SidelineRequest request = new SidelineRequest(new SidelineRequestIdentifier("test"), new StaticMessageFilter());

        // Send a new start request with our filter.
        // This means that our starting offset for the sideline'd data should start at offset 3 (we acked offsets 0, 1, 2)
        StaticTrigger.sendStartRequest(request);

        final SidelineRequestIdentifier sidelineRequestIdentifier = StaticTrigger.getCurrentSidelineRequestIdentifier();

        // Produce 5 more messages into kafka, should be offsets [10,11,12,13,14]
        final List<ProducedKafkaRecord<byte[], byte[]>> additionalProducedRecords = produceRecords(5, 0);

        // Call nextTuple() 4 more times, we should get the remaining first 10 records because they were already buffered.
        spoutEmissions.addAll(consumeTuplesFromSpout(spout, spoutOutputCollector, 4));

        // We'll validate them
        validateTuplesFromSourceMessages(producedRecords, spoutEmissions, expectedStreamId, consumerIdPrefix + ":main", false);

        // But if we call nextTuple() 5 more times, we should never get the additional 5 records we produced.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 5, 100L);

        // Lets not ack any more tuples from the fire hose, that means the last completed
        // offset on the fire hose spout should still be 3.

        // Stop the spout.
        // A graceful shutdown of the spout should have the consumer state flushed to the persistence layer.
        spout.close();

        // A little debug log
        logger.info("=== Starting spout again");
        logger.info("=== This verifies that when we resume, we pickup started sideling requests and continue filtering");

        // Create new Spout instance and start
        topologyContext = new MockTopologyContext();
        spoutOutputCollector = new MockSpoutOutputCollector();

        // Create our spout, add references to our static trigger, and call open().
        spout = new SidelineSpout(config);

        spout.open(config, topologyContext, spoutOutputCollector);

        // Wait 3 seconds, then verify we have a single virtual spouts running
        Thread.sleep(3000L);
        waitForVirtualSpouts(spout, 1);

        // Call nextTuple() 20 times, we should get no tuples, last committed offset was 3, so this means we asked for
        // offsets [4,5,6,7,8,9,10,11,12,13,14] <- last committed offset now 14 on firehose.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 20, 100L);

        // Send a stop sideline request
        StaticTrigger.sendStopRequest(request);

        // Verify 2 VirtualSpouts are running
        waitForVirtualSpouts(spout, 2);

        // Call nextTuple() 3 times
        List<SpoutEmission> sidelinedEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, 3);

        // Verify we get offsets [4,5,6] by validating the tuples
        validateEmission(
            producedRecords.get(4),
            sidelinedEmissions.get(0),
            consumerIdPrefix + ":sideline:" + sidelineRequestIdentifier,
            expectedStreamId,
            false
        );
        validateEmission(
            producedRecords.get(5),
            sidelinedEmissions.get(1),
            consumerIdPrefix + ":sideline:" + sidelineRequestIdentifier,
            expectedStreamId,
            false
        );
        validateEmission(
            producedRecords.get(6),
            sidelinedEmissions.get(2),
            consumerIdPrefix + ":sideline:" + sidelineRequestIdentifier,
            expectedStreamId,
            false
        );

        // Ack offsets [4,5,6] => committed offset should be 6 now on sideline consumer.
        ackTuples(spout, sidelinedEmissions);

        // Shut down spout.
        spout.close();

        // A little debug log
        logger.info("=== Starting spout again");
        logger.info("=== This verifies that when we resume a side line virtual spout, we resume at the proper offset based on state");

        // Create new spout instance and start
        topologyContext = new MockTopologyContext();
        spoutOutputCollector = new MockSpoutOutputCollector();

        // Create our spout, add references to our static trigger, and call open().
        spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // Verify we have a 2 virtual spouts running
        waitForVirtualSpouts(spout, 2);

        // Since last committed offset should be 6,
        // Call nextTuple() 8 times to get offsets [7,8,9,10,11,12,13,14]
        sidelinedEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, 8);

        // Verify we get offsets [7,8,9,10,11,12,13,14] by validating the tuples
        // Gather up the expected records
        List<ProducedKafkaRecord<byte[], byte[]>> sidelineKafkaRecords = Lists.newArrayList();
        sidelineKafkaRecords.addAll(producedRecords.subList(7, 10));
        sidelineKafkaRecords.addAll(additionalProducedRecords);

        // Validate em.
        validateTuplesFromSourceMessages(
            sidelineKafkaRecords,
            sidelinedEmissions,
            expectedStreamId,
            consumerIdPrefix + ":sideline:" + sidelineRequestIdentifier,
            false
        );

        // call nextTuple() several times, get nothing back
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 0L);

        // Ack offsets [4,5,6,7,8,9,10,11,12,13,14] => committed offset should be 14 now on sideline consumer.
        ackTuples(spout, sidelinedEmissions);

        // Verify 2nd VirtualSpout shuts off
        waitForVirtualSpouts(spout, 1);
        logger.info("=== Virtual Spout should be closed now... just fire hose left!");

        // Produce 5 messages into Kafka namespace with offsets [15,16,17,18,19]
        List<ProducedKafkaRecord<byte[], byte[]>> lastProducedRecords = produceRecords(5, 0);

        // Call nextTuple() 5 times,
        List<SpoutEmission> lastSpoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, 5);

        // verify we get the tuples [15,16,17,18,19]
        validateTuplesFromSourceMessages(lastProducedRecords, lastSpoutEmissions, expectedStreamId, consumerIdPrefix + ":main", false);

        // Ack offsets [15,16,18] => Committed offset should be 16
        ackTuples(spout, Lists.newArrayList(
            lastSpoutEmissions.get(0), lastSpoutEmissions.get(1), lastSpoutEmissions.get(3)
        ));

        // Verify no more tuples
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 0L);

        // Stop spout
        spout.close();

        // Create new spout instance and start.
        topologyContext = new MockTopologyContext();
        spoutOutputCollector = new MockSpoutOutputCollector();

        // A little debug log
        logger.info("=== Starting spout for last time");
        logger.info("=== This last bit verifies that we don't resume finished sideline requests");


        // Create our spout, add references to our static trigger, and call open().
        spout = new SidelineSpout(config);

        spout.open(config, topologyContext, spoutOutputCollector);

        // Verify we have a single 1 virtual spouts running,
        // This makes sure that we don't resume a previously completed sideline request.
        Thread.sleep(3000);
        waitForVirtualSpouts(spout, 1);

        // Call nextTuple() 3 times,
        // verify we get offsets [17,18,19]
        lastSpoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, 3);

        // Validate we got the right offset [17,18,19]
        validateTuplesFromSourceMessages(
            lastProducedRecords.subList(2,5),
            lastSpoutEmissions,
            expectedStreamId,
            consumerIdPrefix + ":main",
            false
        );

        // Verify no more tuples
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 0L);

        // Stop spout.
        spout.close();
    }

    /**
     * This is an integration test of multiple SidelineConsumers.
     * We stand up a namespace with 4 partitions.
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
        final String consumerIdPrefix = "TestSidelineSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);

        // Use zookeeper persistence manager
        config.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, ZookeeperPersistenceAdapter.class.getName());

        // Create topology context, set our task index
        MockTopologyContext topologyContext = new MockTopologyContext();
        topologyContext.taskId = taskIndex;
        topologyContext.taskIndex = taskIndex;

        // Say that we have 2 tasks, ids 0 and 1
        topologyContext.componentTasks = Collections.unmodifiableList(Lists.newArrayList(0,1));

        MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create our spout, add references to our static trigger, and call open().
        DynamicSpout spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

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
     * This is an integration test of multiple SidelineConsumers.
     * We stand up a namespace with 4 partitions.
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
        final String consumerIdPrefix = "TestSidelineSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);

        // Use zookeeper persistence manager
        config.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, ZookeeperPersistenceAdapter.class.getName());

        // Create topology context, set our task index
        MockTopologyContext topologyContext = new MockTopologyContext();
        topologyContext.taskId = taskIndex;
        topologyContext.taskIndex = taskIndex;

        // Say that we have 2 tasks, ids 0 and 1
        topologyContext.componentTasks = Collections.unmodifiableList(Lists.newArrayList(0,1));

        MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create our spout, add references to our static trigger, and call open().
        DynamicSpout spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

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

    /**
     * Tests that pending errors get reported via OutputCollector.
     */
    @Test
    public void testReportErrors() {
        // Define config
        //final Map<String, Object> config = getDefaultConfig("ConsumerIdPrefix", "StreamId");
        final Map<String, Object> config = new HashMap<>();
        config.put(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX, "ConsumerIdPrefix");
        config.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, InMemoryPersistenceAdapter.class.getName());

        // Create mocks
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector mockSpoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout
        final DynamicSpout spout = new DynamicSpout(config);

        // Call open
        spout.open(config, topologyContext, mockSpoutOutputCollector);

        // Hook into error queue, and queue some errors
        final Throwable exception1 = new RuntimeException("My RuntimeException");
        final Throwable exception2 = new Exception("My Exception");

        // "Report" our exceptions
        spout.getCoordinator().getReportedErrorsQueue().add(exception1);
        spout.getCoordinator().getReportedErrorsQueue().add(exception2);

        // Call next tuple a couple times, validate errors get reported.
        await()
            .atMost(30, TimeUnit.SECONDS)
            .until(() -> {
                // Call next tuple
                spout.nextTuple();

                return mockSpoutOutputCollector.getReportedErrors().size() >= 2;
            });

        // Validate
        final List<Throwable> reportedErrors = mockSpoutOutputCollector.getReportedErrors();
        assertEquals("Should have 2 reported errors", 2, reportedErrors.size());
        assertTrue("Contains first exception", reportedErrors.contains(exception1));
        assertTrue("Contains second exception", reportedErrors.contains(exception2));

        // Call close
        spout.close();
    }

    /**
     * Verifies that you do not define an output stream via the SidelineSpoutConfig
     * declareOutputFields() method with default to using 'default' stream.
     */
    @Test
    @UseDataProvider("provideOutputFields")
    public void testDeclareOutputFields_without_stream(final Object inputFields, final String[] expectedFields) {
        // Create config with null stream id config option.
        final Map<String,Object> config = getDefaultConfig("SidelineSpout-", null);

        // Define our output fields as key and value.
        config.put(SpoutConfig.OUTPUT_FIELDS, inputFields);

        final OutputFieldsGetter declarer = new OutputFieldsGetter();

        // Create spout, but don't call open
        final SidelineSpout spout = new SidelineSpout(config);

        // call declareOutputFields
        spout.declareOutputFields(declarer);

        // Validate results.
        final Map<String, StreamInfo> fieldsDeclaration = declarer.getFieldsDeclaration();

        // Validate standard output stream
        assertTrue(fieldsDeclaration.containsKey(Utils.DEFAULT_STREAM_ID));
        assertEquals(
            fieldsDeclaration.get(Utils.DEFAULT_STREAM_ID).get_output_fields(),
            Lists.newArrayList(expectedFields)
        );

        // Validate permanently failed output stream
        final String defaultFailedStreamId = "failed";
        assertTrue(fieldsDeclaration.containsKey(defaultFailedStreamId));
        assertEquals(
            fieldsDeclaration.get(defaultFailedStreamId).get_output_fields(),
            Lists.newArrayList(expectedFields)
        );

        // Should only have 2 streams defined
        assertEquals("Should only have 2 streams defined", 2, fieldsDeclaration.size());

        // Call close on spout
        spout.close();
    }

    /**
     * Verifies that you can define an output stream via the SidelineSpoutConfig and it gets used
     * in the declareOutputFields() method.
     */
    @Test
    @UseDataProvider("provideOutputFields")
    public void testDeclareOutputFields_with_stream(final Object inputFields, final String[] expectedFields) {
        final String streamId = "foobar";
        final String failedStreamId = "failed";
        final Map<String,Object> config = getDefaultConfig("SidelineSpout-", streamId);

        // Define our output fields as key and value.
        config.put(SpoutConfig.OUTPUT_FIELDS, inputFields);

        final OutputFieldsGetter declarer = new OutputFieldsGetter();

        // Create spout, but do not call open.
        final SidelineSpout spout = new SidelineSpout(config);

        // call declareOutputFields
        spout.declareOutputFields(declarer);

        final Map<String, StreamInfo> fieldsDeclaration = declarer.getFieldsDeclaration();

        assertTrue(fieldsDeclaration.containsKey(streamId));
        assertEquals(
            fieldsDeclaration.get(streamId).get_output_fields(),
            Lists.newArrayList(expectedFields)
        );

        // Validate permanently failed output stream
        assertTrue(fieldsDeclaration.containsKey(failedStreamId));
        assertEquals(
            fieldsDeclaration.get(failedStreamId).get_output_fields(),
            Lists.newArrayList(expectedFields)
        );

        // Should only have 2 streams defined
        assertEquals("Should only have 2 streams defined", 2, fieldsDeclaration.size());

        spout.close();
    }

    /**
     * Provides various inputs to be split.
     */
    @DataProvider
    public static Object[][] provideOutputFields() throws InstantiationException, IllegalAccessException {
        return new Object[][] {
            // String inputs, these get split and trimmed.
            { "key,value", new String[] {"key", "value"} },
            { "key, value", new String[] {"key", "value"} },
            { " key    , value  ,", new String[] {"key", "value"} },

            // List of Strings, used as is.
            { Lists.newArrayList("key", "value"), new String[] { "key", "value"} },
            { Lists.newArrayList("  key  ", " value"), new String[] { "  key  ", " value"} },
            { Lists.newArrayList("key,value", "another"), new String[] { "key,value", "another"} },

            // Fields inputs, used as is.
            { new Fields("key", "value"), new String[] { "key", "value" } },
            { new Fields(" key ", "    value"), new String[] { " key ", "    value" } },
            { new Fields("key,value ", "another"), new String[] { "key,value ", "another" } },
        };
    }

    /**
     * Noop, just doing coverage!  These methods don't actually
     * do anything right now anyways.
     */
    @Test
    public void testActivate() {
        final SidelineSpout spout = new SidelineSpout(Maps.newHashMap());
        spout.activate();
    }

    /**
     * Noop, just doing coverage!  These methods don't actually
     * do anything right now anyways.
     */
    @Test
    public void testDeactivate() {
        final SidelineSpout spout = new SidelineSpout(Maps.newHashMap());
        spout.deactivate();
    }

    // Helper methods

    /**
     * Given a list of KafkaRecords that got published into Kafka, compare it against a list of SpoutEmissions that
     * got emitted by the spout, and make sure everything matches up to what we expected.
     *
     * @param sourceProducerRecord - The KafkaRecord messages published into kafka.
     * @param spoutEmission - The SpoutEmissions we got out of the spout
     * @param expectedConsumerId - What consumerId these emissions should be associated with.
     * @param expectedOutputStreamId - What stream these emissions should have been emitted down.
     */
    private void validateEmission(
        final ProducedKafkaRecord<byte[], byte[]> sourceProducerRecord,
        final SpoutEmission spoutEmission,
        final String expectedConsumerId,
        final String expectedOutputStreamId,
        final boolean shouldBePermanentlyFailed
    ) {
        // Now find its corresponding tuple
        assertNotNull("Not null sanity check", spoutEmission);
        assertNotNull("Not null sanity check", sourceProducerRecord);

        // Grab the messageId and validate it
        final MessageId messageId = (MessageId) spoutEmission.getMessageId();

        // If we are permanently failed
        if (shouldBePermanentlyFailed) {
            // Then we should have no messageId associated.
            assertNull("Permanently failed messages should have null messageId", messageId);
        } else {
            // Validate Message Id
            assertNotNull("Should have non-null messageId", spoutEmission.getMessageId());
            assertTrue("Should be instance of MessageId", spoutEmission.getMessageId() instanceof MessageId);

            assertEquals("Expected Topic Name in MessageId", sourceProducerRecord.getTopic(), messageId.getNamespace());
            assertEquals("Expected PartitionId found", sourceProducerRecord.getPartition(), messageId.getPartition());
            assertEquals("Expected MessageOffset found", sourceProducerRecord.getOffset(), messageId.getOffset());

            // TODO: Should revisit this and refactor the test to properly pass around identifiers for validation
            assertEquals("Expected Source Consumer Id", expectedConsumerId, messageId.getSrcVirtualSpoutId().toString());
        }

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
        // Acking tuples is an async process, so we need to make sure they get picked up
        // and processed before continuing.
        await()
            .atMost(6500, TimeUnit.MILLISECONDS)
            .until(() -> {
                // Wait for our tuples to get popped off the acked queue.
                Map<VirtualSpoutIdentifier, Queue<MessageId>> queueMap = spout.getCoordinator().getAckedTuplesQueue();
                for (VirtualSpoutIdentifier key : queueMap.keySet()) {
                    // If any queue has entries, return false
                    if (!queueMap.get(key).isEmpty()) {
                        logger.debug("Ack queue {} has {}", key, queueMap.get(key).size());
                        return false;
                    }
                }
                // If all entries are empty, return true
                return true;
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

        // Failing tuples is an async process, so we need to make sure they get picked up
        // and processed before continuing.
        await()
            .atMost(6500, TimeUnit.MILLISECONDS)
            .until(() -> {
                // Wait for our tuples to get popped off the fail queue.
                Map<VirtualSpoutIdentifier, Queue<MessageId>> queueMap = spout.getCoordinator().getFailedTuplesQueue();
                for (VirtualSpoutIdentifier key : queueMap.keySet()) {
                    // If any queue has entries, return false
                    if (!queueMap.get(key).isEmpty()) {
                        logger.debug("Fail queue {} has {}", key, queueMap.get(key).size());
                        return false;
                    }
                }
                // If all entries are empty, return true
                return true;
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
     * @param expectedConsumerId consumer id we expected
     */
    private void validateTuplesFromSourceMessages(
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecords,
        final List<SpoutEmission> spoutEmissions,
        final String expectedStreamId,
        final String expectedConsumerId,
        final boolean shouldBePermanentlyFailed
    ) {
        // Sanity check, make sure we have the same number of each.
        assertEquals(
            "Should have same number of tuples as original messages, Produced Count: " + producedRecords.size()
            + " Emissions Count: " + spoutEmissions.size(),
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
            validateEmission(producedRecord, spoutEmission, expectedConsumerId, expectedStreamId, shouldBePermanentlyFailed);
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
            .until(() -> spout.getCoordinator().getTotalSpouts(), equalTo(howManyVirtualSpoutsWeWantLeft));
        assertEquals(
            "We should have " + howManyVirtualSpoutsWeWantLeft + " virtual spouts running",
            howManyVirtualSpoutsWeWantLeft,
            spout.getCoordinator().getTotalSpouts()
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
        final String uniqueZkRootNode = "/sideline-spout-test/testRun" + System.currentTimeMillis();

        final Map<String, Object> config = SpoutConfig.setDefaults(Maps.newHashMap());

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

        // TODO: Separate the dependencies on this from this test!!!
        config.put(SidelineConfig.PERSISTENCE_ZK_SERVERS, Lists.newArrayList(getKafkaTestServer().getZookeeperConnectString()));
        config.put(SidelineConfig.PERSISTENCE_ZK_ROOT, uniqueZkRootNode);
        // Use In Memory Persistence manager, if you need state persistence, over ride this in your test.
        config.put(
            SidelineConfig.PERSISTENCE_ADAPTER_CLASS,
            com.salesforce.storm.spout.sideline.persistence.InMemoryPersistenceAdapter.class.getName()
        );

        // Configure SpoutMonitor thread to run every 1 second
        config.put(SpoutConfig.MONITOR_THREAD_INTERVAL_MS, 1000L);

        // Configure flushing consumer state every 1 second
        config.put(SpoutConfig.CONSUMER_STATE_FLUSH_INTERVAL_MS, 1000L);

        // For now use the Log Recorder
        config.put(SpoutConfig.METRICS_RECORDER_CLASS, LogRecorder.class.getName());

        config.put(SpoutConfig.SPOUT_HANDLER_CLASS, SidelineSpoutHandler.class.getName());

        config.put(SpoutConfig.VIRTUAL_SPOUT_HANDLER_CLASS, SidelineVirtualSpoutHandler.class.getName());

        config.put(SidelineConfig.TRIGGER_CLASS, StaticTrigger.class.getName());

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