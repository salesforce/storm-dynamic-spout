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
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.mocks.MockConsumer;
import com.salesforce.storm.spout.dynamic.mocks.MockSpoutHandler;
import com.salesforce.storm.spout.dynamic.retry.FailedTuplesFirstRetryManager;
import com.salesforce.storm.spout.dynamic.retry.NeverRetryManager;
import com.salesforce.storm.spout.dynamic.metrics.LogRecorder;
import com.salesforce.storm.spout.dynamic.mocks.MockTopologyContext;
import com.salesforce.storm.spout.dynamic.mocks.output.MockSpoutOutputCollector;
import com.salesforce.storm.spout.dynamic.mocks.output.SpoutEmission;
import com.salesforce.storm.spout.dynamic.persistence.InMemoryPersistenceAdapter;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsGetter;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * End to End integration testing of DynamicSpout under various scenarios.
 */
@RunWith(DataProviderRunner.class)
public class DynamicSpoutTest {
    // For logging within the test.
    private static final Logger logger = LoggerFactory.getLogger(DynamicSpoutTest.class);

    /**
     * We generate a unique topic name for every test case.
     */
    private String topicName;

    /**
     * We keep track of offsets we generate.
     */
    private long offset = 0;

    /**
     * This happens once before every test method.
     * Create a new empty namespace with randomly generated name.
     */
    @Before
    public void beforeTest() throws InterruptedException {
        // Generate namespace name
        topicName = DynamicSpoutTest.class.getSimpleName() + Clock.systemUTC().millis();

        // Reset our MockConsumer
        MockConsumer.reset();
    }

    /**
     * By default expect no exceptions.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Validates that we require the ConsumerIdPrefix configuration value.
     * and if its missing we toss an IllegalStateException during open()
     */
    @Test
    public void testMissingRequiredConfigurationConsumerIdPrefix() {
        // Create our config missing the consumerIdPrefix
        final Map<String, Object> config = getDefaultConfig(null, null);

        // Unset VirtualSpoutIdPrefix.
        config.remove(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX);

        // Define spout config
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Some mock stuff to get going
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        final DynamicSpout spout = new DynamicSpout(spoutConfig);

        // When we call open, we expect illegal state exception about our missing configuration item
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX);

        // Call open
        spout.open(config, topologyContext, spoutOutputCollector);
    }

    /**
     * Test you cannot open DynamicSpout multiple times.
     */
    @Test
    public void testCannotOpenMultipleTimes() throws InterruptedException {
        // Define our ConsumerId prefix
        final String consumerIdPrefix = "TestDynamicSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, null);
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Some mock storm topology stuff to get going
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        final DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(config, topologyContext, spoutOutputCollector);

        try {
            expectedException.expect(IllegalStateException.class);
            expectedException.expectMessage("opened");
            spout.open(config, topologyContext, spoutOutputCollector);
        } finally {
            // Cleanup.
            spout.close();
        }
    }

    /**
     * Test adding a VirtualSpout, verifying that the spout exists, and then removing it.
     */
    @Test
    public void testAddRemoveHasSpout() {
        // Define our ConsumerId prefix
        final String consumerIdPrefix = "TestDynamicSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, null);
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Some mock storm topology stuff to get going
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        final DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(config, topologyContext, spoutOutputCollector);

        // Create new unique VSpoutId
        final VirtualSpoutIdentifier virtualSpoutIdentifier =
            new DefaultVirtualSpoutIdentifier("MyVSpoutId" + System.currentTimeMillis());

        // Create new unique VSpoutId
        final VirtualSpoutIdentifier fakeSpoutIdentifier =
            new DefaultVirtualSpoutIdentifier("Fake" + System.currentTimeMillis());

        // We shouldn't have the spout yet
        assertFalse("Should not have spout yet", spout.hasVirtualSpout(virtualSpoutIdentifier));
        assertFalse("Should not have fake spout", spout.hasVirtualSpout(fakeSpoutIdentifier));

        // Add a VirtualSpout.
        final VirtualSpout virtualSpout = new VirtualSpout(
            virtualSpoutIdentifier,
            spoutConfig.toMap(),
            topologyContext,
            new FactoryManager(spoutConfig.toMap()),
            new LogRecorder(),
            null,
            null
        );
        spout.addVirtualSpout(virtualSpout);

        // Wait for VirtualSpout to start
        waitForVirtualSpouts(spout, 1);

        // We should have the spout now
        assertTrue("Should have spout", spout.hasVirtualSpout(virtualSpoutIdentifier));
        assertFalse("Should not have fake spout", spout.hasVirtualSpout(fakeSpoutIdentifier));

        // Now lets remove it
        spout.removeVirtualSpout(virtualSpoutIdentifier);

        // Wait for VirtualSpout to start
        waitForVirtualSpouts(spout, 0);

        // We should no longer have the spout
        assertFalse("Should not have spout", spout.hasVirtualSpout(virtualSpoutIdentifier));
        assertFalse("Should not have fake spout", spout.hasVirtualSpout(fakeSpoutIdentifier));

        // Cleanup.
        spout.close();
    }

    /**
     * Our most simple end-2-end test.
     * This test stands up our spout and we add a single VirtualSpout using a "Mock Consumer."
     * We instruct the "Mock Consumer" to "produce" some Records, and validate that when we call nextTuple() on
     * our spout that we get out those tuples.
     *
     * We run this test multiple times using a DataProvider to test using but an implicit/unconfigured
     * output stream name (default), as well as an explicitly configured stream name.
     */
    @Test
    @UseDataProvider("provideStreamIds")
    public void doBasicNextTupleTest(final String configuredStreamId, final String expectedStreamId) throws InterruptedException {
        // Define how many tuples we should push into the namespace, and then consume back out.
        final int emitTupleCount = 10;

        // Define our ConsumerId prefix
        final String consumerIdPrefix = "TestDynamicSpout";

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
        final DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Create new unique VSpoutId
        final VirtualSpoutIdentifier virtualSpoutIdentifier =
            new DefaultVirtualSpoutIdentifier("MyVSpoutId" + System.currentTimeMillis());

        // Add a VirtualSpout.
        final VirtualSpout virtualSpout = new VirtualSpout(
            virtualSpoutIdentifier,
            spoutConfig.toMap(),
            topologyContext,
            new FactoryManager(spoutConfig.toMap()),
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
        final List<Record> producedRecords = produceRecords(emitTupleCount, topicName, virtualSpout.getVirtualSpoutId());

        // Now consume tuples generated from the messages we published into kafka.
        final List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, emitTupleCount);

        // Validate the tuples that got emitted are what we expect based on what we published into kafka
        validateTuplesFromSourceMessages(producedRecords, spoutEmissions, expectedStreamId, virtualSpoutIdentifier);

        // Call next tuple a few more times to make sure nothing unexpected shows up.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 3, 0L);

        // Lets remove the VirtualSpout and wait for it to shut down.
        spout.removeVirtualSpout(virtualSpoutIdentifier);
        waitForVirtualSpouts(spout, 0);

        // Cleanup.
        spout.close();
    }

    /**
     * End-to-End test over the fail() method using the {@link FailedTuplesFirstRetryManager}
     * retry manager.
     *
     * This test stands up our DynamicSpout with a single VirtualSpout using a "Mock Consumer"
     * We inject some data into mock consumer, and validate that when we call nextTuple() on
     * our spout that we get out our messages.
     *
     * We then fail some of those tuples and validate that they get replayed.
     * We ack some tuples and then validate that they do NOT get replayed.
     */
    @Test
    public void doBasicFailTest() throws InterruptedException {
        // Define how many tuples we should push into the namespace, and then consume back out.
        final int emitTupleCount = 10;

        // Define our ConsumerId prefix
        final String consumerIdPrefix = "DynamicSpout";

        // Define our output stream id
        final String expectedStreamId = "default";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);

        // Configure to use our FailedTuplesFirstRetryManager retry manager.
        // This implementation will always replay failed tuples first.
        config.put(SpoutConfig.RETRY_MANAGER_CLASS, FailedTuplesFirstRetryManager.class.getName());
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Some mock stuff to get going
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        final DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Create new unique VSpoutId
        final VirtualSpoutIdentifier virtualSpoutIdentifier =
            new DefaultVirtualSpoutIdentifier("MyVSpoutId" + System.currentTimeMillis());

        // Add a VirtualSpout.
        final VirtualSpout virtualSpout = new VirtualSpout(
            virtualSpoutIdentifier,
            spoutConfig.toMap(),
            topologyContext,
            new FactoryManager(spoutConfig.toMap()),
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
        final List<Record> producedRecords = produceRecords(emitTupleCount, topicName, virtualSpout.getVirtualSpoutId());

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
     * This test stands up a DynamicSpout instance running a single VirtualSpout that uses a "Mock Consumer".
     * We'll inject some data into the Mock Consumer and get it back out using nextTuple().
     * We'll then ack those tuples and validate that the ack notifications make it back
     * to the Mock Consumer.
     */
    @Test
    public void testAckingTuples() throws InterruptedException, IOException {
        // Create spout
        // Define our output stream id
        final String expectedStreamId = "default";
        final String consumerIdPrefix = "TestDynamicSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Some mock stuff to get going
        TopologyContext topologyContext = new MockTopologyContext();
        MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(config, topologyContext, spoutOutputCollector);

        // Create new unique VSpoutId
        final VirtualSpoutIdentifier virtualSpoutIdentifier =
            new DefaultVirtualSpoutIdentifier("MyVSpoutId" + System.currentTimeMillis());

        // Add a VirtualSpout.
        final VirtualSpout virtualSpout = new VirtualSpout(
            virtualSpoutIdentifier,
            spoutConfig.toMap(),
            topologyContext,
            new FactoryManager(spoutConfig.toMap()),
            new LogRecorder(),
            null,
            null
        );
        spout.addVirtualSpout(virtualSpout);

        // Wait for VirtualSpout to start
        waitForVirtualSpouts(spout, 1);

        // Inject 10 messages into MockConsumer (offsets 0->9)
        final List<Record> producedRecords = produceRecords(10, topicName, virtualSpoutIdentifier);

        // Call next tuple 6 times, getting offsets 0,1,2,3,4,5
        final List<SpoutEmission> spoutEmissions = Collections.unmodifiableList(consumeTuplesFromSpout(spout, spoutOutputCollector, 6));

        // Validate its the messages we expected
        validateEmission(producedRecords.get(0), spoutEmissions.get(0), virtualSpoutIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(1), spoutEmissions.get(1), virtualSpoutIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(2), spoutEmissions.get(2), virtualSpoutIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(3), spoutEmissions.get(3), virtualSpoutIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(4), spoutEmissions.get(4), virtualSpoutIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(5), spoutEmissions.get(5), virtualSpoutIdentifier, expectedStreamId);

        // We will ack offsets 0-4
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(0)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(1)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(2)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(3)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(4)));

        // Wait for them to make it to the MockConsumer.
        await()
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> MockConsumer.getCommitted(virtualSpoutIdentifier).size(), equalTo(5));

        // Validate that we've committed the appropriate offsets.
        validateAckedTuples(virtualSpoutIdentifier, topicName, 0, 1, 2, 3, 4);

        // Stop the spout.
        // A graceful shutdown of the spout should have the consumer state flushed to the persistence layer.
        spout.close();
    }

    /**
     * This is an integration test of multiple VirtualSpouts.
     * - Start 2 VirtualSpouts
     * - Inject records for both
     * - Call nextTuple() and validate that we get them out.
     * - Call ack() and validate that each VirtualSpout receives the ack()
     * - Call fail() and validate that each VirtualSpout receives the fail()
     */
    @Test
    public void testNextTupleAndAckUsingMultipleVirtualSpouts() {
        final int numberOfMsgsPerVirtualSpout = 5;

        // Create two topics
        final String topic1 = "VSpout1Topic";
        final String topic2 = "VSpout2Topic";

        // Create two VirtualSpoutIdentifiers
        final VirtualSpoutIdentifier vspoutId1 = new DefaultVirtualSpoutIdentifier("VSpout1");
        final VirtualSpoutIdentifier vspoutId2 = new DefaultVirtualSpoutIdentifier("VSpout2");

        // Inject 5 msgs for each VirtualSpout
        produceRecords(numberOfMsgsPerVirtualSpout, topic1, vspoutId1);
        produceRecords(numberOfMsgsPerVirtualSpout, topic2, vspoutId2);

        // Create spout
        // Define our output stream id
        final String expectedStreamId = "default";
        final String consumerIdPrefix = "TestDynamicSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Create topology context, set our task index
        MockTopologyContext topologyContext = new MockTopologyContext();
        MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create our spout,
        DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Create two virtual Spouts
        final VirtualSpout virtualSpout1 = new VirtualSpout(
            vspoutId1,
            spoutConfig.toMap(),
            topologyContext,
            new FactoryManager(spoutConfig.toMap()),
            new LogRecorder(),
            null,
            null
        );
        // Add a VirtualSpout.
        final VirtualSpout virtualSpout2 = new VirtualSpout(
            vspoutId2,
            spoutConfig.toMap(),
            topologyContext,
            new FactoryManager(spoutConfig.toMap()),
            new LogRecorder(),
            null,
            null
        );

        // Add them
        spout.addVirtualSpout(virtualSpout1);
        spout.addVirtualSpout(virtualSpout2);

        // Wait for our virtual spout to start
        waitForVirtualSpouts(spout, 2);

        // Call next tuple 20 times, getting offsets 0-4 from the first vSpout, 5-9 on the 2nd vSpout
        final List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, (numberOfMsgsPerVirtualSpout * 2));

        // Validate they all came from the correct virtualSpouts
        for (SpoutEmission spoutEmission : spoutEmissions) {
            assertNotNull("Has non-null tupleId", spoutEmission.getMessageId());

            // Validate it came from the right place
            final MessageId messageId = (MessageId) spoutEmission.getMessageId();

            if (messageId.getSrcVirtualSpoutId() == vspoutId1) {
                assertTrue("Should have offset >= 0 and <= 4", messageId.getOffset() >= 0 && messageId.getOffset() <= 4);
                assertEquals("Should come from partition 0", 0, messageId.getPartition());
                assertEquals("Should come from our namespace", topic1, messageId.getNamespace());
            } else if (messageId.getSrcVirtualSpoutId() == vspoutId2) {
                assertTrue("Should have offset >= 5 and <= 9", messageId.getOffset() >= 5 && messageId.getOffset() <= 9);
                assertEquals("Should come from partition 0", 0, messageId.getPartition());
                assertEquals("Should come from our namespace", topic2, messageId.getNamespace());
            } else {
                // Fail
                assertFalse("Got unknown VirtualSpoutId! " + messageId.getSrcVirtualSpoutId(), true);
            }
        }

        // Validate we don't have any other emissions
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 5, 0L);

        // Lets ack our tuples
        ackTuples(spout, spoutEmissions);

        // Wait for them to make it to the MockConsumer.
        await()
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> MockConsumer.getCommitted(vspoutId1).size(), equalTo(numberOfMsgsPerVirtualSpout));
        await()
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> MockConsumer.getCommitted(vspoutId2).size(), equalTo(numberOfMsgsPerVirtualSpout));

        // Now validate them
        validateAckedTuples(vspoutId1, topic1, 0,1,2,3,4);
        validateAckedTuples(vspoutId2, topic2, 5,6,7,8,9);

        // Close
        spout.close();
    }

    /**
     * Tests that pending errors get reported via OutputCollector.
     */
    @Test
    public void testReportErrors() {
        // Define config
        final Map<String, Object> config = new HashMap<>();
        config.put(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX, "ConsumerIdPrefix");
        config.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, InMemoryPersistenceAdapter.class.getName());
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Create mocks
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector mockSpoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout
        final DynamicSpout spout = new DynamicSpout(spoutConfig);

        // Call open
        spout.open(config, topologyContext, mockSpoutOutputCollector);

        // Hook into error queue, and queue some errors
        final Throwable exception1 = new RuntimeException("My RuntimeException");
        final Throwable exception2 = new Exception("My Exception");

        // "Report" our exceptions
        final MessageBus messageBus = (MessageBus) spout.getMessageBus();
        messageBus.publishError(exception1);
        messageBus.publishError(exception2);

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
     * Verifies that you do not define an output stream via the SpoutConfig
     * declareOutputFields() method with default to using 'default' stream.
     */
    @Test
    @UseDataProvider("provideOutputFields")
    public void testDeclareOutputFields_without_stream(final Object inputFields, final String[] expectedFields) {
        // Create config with null stream id config option.
        final Map<String,Object> config = getDefaultConfig("DynamicSpout-", null);

        // Define our output fields as key and value.
        config.put(SpoutConfig.OUTPUT_FIELDS, inputFields);

        final SpoutConfig spoutConfig = new SpoutConfig(config);

        final OutputFieldsGetter declarer = new OutputFieldsGetter();

        // Create spout, but don't call open
        final DynamicSpout spout = new DynamicSpout(spoutConfig);

        // call declareOutputFields
        spout.declareOutputFields(declarer);

        // Validate results.
        final Map<String, StreamInfo> fieldsDeclaration = declarer.getFieldsDeclaration();

        assertTrue(fieldsDeclaration.containsKey(Utils.DEFAULT_STREAM_ID));
        assertEquals(
            fieldsDeclaration.get(Utils.DEFAULT_STREAM_ID).get_output_fields(),
            Lists.newArrayList(expectedFields)
        );

        spout.close();
    }

    /**
     * Verifies that you can define an output stream via the SpoutConfig and it gets used
     * in the declareOutputFields() method.
     */
    @Test
    @UseDataProvider("provideOutputFields")
    public void testDeclareOutputFields_with_stream(final Object inputFields, final String[] expectedFields) {
        final String streamId = "foobar";
        final Map<String,Object> config = getDefaultConfig("DynamicSpout-", streamId);

        // Define our output fields as key and value.
        config.put(SpoutConfig.OUTPUT_FIELDS, inputFields);

        final SpoutConfig spoutConfig = new SpoutConfig(config);

        final OutputFieldsGetter declarer = new OutputFieldsGetter();

        // Create spout, but do not call open.
        final DynamicSpout spout = new DynamicSpout(spoutConfig);

        // call declareOutputFields
        spout.declareOutputFields(declarer);

        final Map<String, StreamInfo> fieldsDeclaration = declarer.getFieldsDeclaration();

        assertTrue(fieldsDeclaration.containsKey(streamId));
        assertEquals(
            fieldsDeclaration.get(streamId).get_output_fields(),
            Lists.newArrayList(expectedFields)
        );

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
     * Smoke test to ensure that SpoutHandler hooks get called at the appropriate times.
     */
    @Test
    public void testSpoutHandlerHooks() {
        final Map<String, Object> config = getDefaultConfig("MyPrefix", "default");
        config.put(SpoutConfig.SPOUT_HANDLER_CLASS, MockSpoutHandler.class.getName());
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Create mocks
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector mockSpoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout
        final DynamicSpout spout = new DynamicSpout(spoutConfig);

        // Call open
        final Map topologyConfig = new HashMap();
        topologyConfig.put("Test", "Value");
        spout.open(topologyConfig, topologyContext, mockSpoutOutputCollector);

        // Grab our SpoutHandler
        final MockSpoutHandler mockSpoutHandler = (MockSpoutHandler) spout.getSpoutHandler();
        assertNotNull("Should have created SpoutHandler", mockSpoutHandler);

        // Ensure that open() hook was called with appropriate config
        assertTrue("Should have called open() hook", mockSpoutHandler.isHasCalledOpen());
        assertEquals("Appropriate SpoutConfig passed", spoutConfig, mockSpoutHandler.getSpoutConfig());

        // Validate onSpoutOpen() hook was called
        assertEquals("Should have been called once", 1, mockSpoutHandler.getOpenedSpouts().size());
        final MockSpoutHandler.OpenedSpoutParams parameters = mockSpoutHandler.getOpenedSpouts().get(0);
        assertEquals("Got called with right spout", spout, parameters.getSpout());
        assertEquals("Got called with right topology config", topologyConfig, parameters.getConfig());
        assertEquals("Got called with right topology context", topologyContext, parameters.getTopologyContext());

        // Call activate on spout
        assertEquals("Never called", 0, mockSpoutHandler.getActivatedSpouts().size());
        spout.activate();

        // Ensure activate hook called.
        assertEquals("Activated Spout called once", 1, mockSpoutHandler.getActivatedSpouts().size());
        assertEquals("Called with appropriate argument", spout, mockSpoutHandler.getActivatedSpouts().get(0));

        // Call deactivate on spout
        assertEquals("Never called deactivate hook", 0, mockSpoutHandler.getDeactivatedSpouts().size());
        spout.deactivate();

        // Ensure deactivate hook called.
        assertEquals("Deactivated Spout called once", 1, mockSpoutHandler.getDeactivatedSpouts().size());
        assertEquals("Called with appropriate argument", spout, mockSpoutHandler.getDeactivatedSpouts().get(0));

        // Call close
        assertFalse("Should not have called close yet", mockSpoutHandler.isHasCalledClosed());
        spout.close();

        // Ensure close hook called.
        assertTrue("Close hook called", mockSpoutHandler.isHasCalledClosed());
        assertEquals("Called with appropriate argument", spout, mockSpoutHandler.getClosedSpouts().get(0));
    }

    // Helper methods

    /**
     * Given a list of KafkaRecords that got published into Kafka, compare it against a list of SpoutEmissions that
     * got emitted by the spout, and make sure everything matches up to what we expected.
     *
     * @param sourceRecord - The KafkaRecord messages published into kafka.
     * @param spoutEmission - The SpoutEmissions we got out of the spout
     * @param expectedVirtualSpoutId - What VirtualSpoutId these emissions should be associated with.
     * @param expectedOutputStreamId - What stream these emissions should have been emitted down.
     */
    private void validateEmission(
        final Record sourceRecord,
        final SpoutEmission spoutEmission,
        final VirtualSpoutIdentifier expectedVirtualSpoutId,
        final String expectedOutputStreamId
    ) {
        // Now find its corresponding tuple
        assertNotNull("Not null sanity check", spoutEmission);
        assertNotNull("Not null sanity check", sourceRecord);

        // Validate Message Id
        assertNotNull("Should have non-null messageId", spoutEmission.getMessageId());
        assertTrue("Should be instance of MessageId", spoutEmission.getMessageId() instanceof MessageId);

        // Grab the messageId and validate it
        final MessageId messageId = (MessageId) spoutEmission.getMessageId();
        assertEquals("Expected Topic Name in MessageId", sourceRecord.getNamespace(), messageId.getNamespace());
        assertEquals("Expected PartitionId found", sourceRecord.getPartition(), messageId.getPartition());
        assertEquals("Expected MessageOffset found", sourceRecord.getOffset(), messageId.getOffset());
        assertEquals("Expected Source Consumer Id", expectedVirtualSpoutId, messageId.getSrcVirtualSpoutId());

        // Validate Tuple Contents
        List<Object> tupleValues = spoutEmission.getTuple();
        assertNotNull("Tuple Values should not be null", tupleValues);
        assertFalse("Tuple Values should not be empty", tupleValues.isEmpty());

        // For now the values in the tuple should be 'key' and 'value', this may change.
        assertEquals("Should have 2 values in the tuple", 2, tupleValues.size());
        assertEquals("Found expected 'key' value", sourceRecord.getValues().get(0), tupleValues.get(0));
        assertEquals("Found expected 'value' value", sourceRecord.getValues().get(1), tupleValues.get(1));

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
     * Utility method to validate that the underlying MockConsumer got the right ack notification.
     *
     * @param virtualSpoutIdentifier What virtualSpout identifier to validate.
     * @param expectedNamespace The expected namespace the acked tuples should be under.
     * @param offsets Array of expected offsets.
     */
    private void validateAckedTuples(
        final VirtualSpoutIdentifier virtualSpoutIdentifier,
        final String expectedNamespace,
        final long... offsets) {
        // Validate that we've committed the appropriate offsets.
        final List<MockConsumer.CommittedState> committed = MockConsumer.getCommitted(virtualSpoutIdentifier);
        assertEquals("Should have right number of offsets", committed.size(), offsets.length);
        for (final long expectedOffset : offsets) {
            boolean found = false;
            for (final MockConsumer.CommittedState committedState : committed) {
                // Look for our specific committed offset.
                if (committedState.getNamespace().equals(expectedNamespace)
                    && committedState.getPartition() == 0
                    && committedState.getOffset() == expectedOffset) {

                    found = true;
                    break;
                }
            }
            assertTrue("Found correct offset", found);
        }
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
        final List<Record> producedRecords,
        final List<SpoutEmission> spoutEmissions,
        final String expectedStreamId,
        final VirtualSpoutIdentifier expectedVirtualSpoutId
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
        for (Record producedRecord: producedRecords) {
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
    private List<Record> produceRecords(
        final int numberOfRecords,
        final String topic,
        final VirtualSpoutIdentifier virtualSpoutIdentifier) {
        // Generate random & unique data
        final List<Record> records = new ArrayList<>();
        for (int x = 0; x < numberOfRecords; x++) {
            // Construct key and value
            long myOffset = offset++;
            String key = "key" + myOffset;
            String value = "value" + myOffset;

            // Add to map
            records.add(new Record(topic, 0, myOffset, new Values(key, value)));
        }
        MockConsumer.injectRecords(virtualSpoutIdentifier, records);
        return records;
    }

    /**
     * Generates a Storm Topology configuration with some sane values for our test scenarios.
     *  @param consumerIdPrefix - consumerId prefix to use.
     * @param configuredStreamId - What streamId we should emit tuples out of.
     */
    private Map<String, Object> getDefaultConfig(
        final String consumerIdPrefix,
        final String configuredStreamId) {

        final Map<String, Object> config = new HashMap<>();

        // Kafka Consumer config items
        config.put(SpoutConfig.CONSUMER_CLASS, MockConsumer.class.getName());
        config.put(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX, consumerIdPrefix);

        // DynamicSpout config items
        config.put(SpoutConfig.RETRY_MANAGER_CLASS, NeverRetryManager.class.getName());

        // Use In Memory Persistence manager
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
}