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

package com.salesforce.storm.spout.sideline;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.kafka.test.KafkaTestServer;
import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.ProducedKafkaRecord;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.DynamicSpout;
import com.salesforce.storm.spout.dynamic.MessageBus;
import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.coordinator.SpoutCoordinator;
import com.salesforce.storm.spout.dynamic.filter.StaticMessageFilter;
import com.salesforce.storm.spout.dynamic.kafka.Consumer;
import com.salesforce.storm.spout.dynamic.kafka.KafkaConsumerConfig;
import com.salesforce.storm.spout.dynamic.kafka.deserializer.Utf8StringDeserializer;
import com.salesforce.storm.spout.dynamic.metrics.LogRecorder;
import com.salesforce.storm.spout.dynamic.mocks.MockTopologyContext;
import com.salesforce.storm.spout.dynamic.mocks.output.MockSpoutOutputCollector;
import com.salesforce.storm.spout.dynamic.mocks.output.SpoutEmission;
import com.salesforce.storm.spout.dynamic.persistence.InMemoryPersistenceAdapter;
import com.salesforce.storm.spout.dynamic.persistence.ZookeeperPersistenceAdapter;
import com.salesforce.storm.spout.dynamic.retry.NeverRetryManager;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.StaticTrigger;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.time.Clock;
import java.util.Collections;
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
 * Provides End-To-End integration test coverage for SidelineSpout specific functionality.
 * This test does not attempt to validate all pieces of DynamicSpout, as that is covered by DynamicSpoutTest.
 * Although not required to use SidelineSpout, some of these tests use the Kafka consumer as a dependency to validate
 * behavior.
 */
@RunWith(DataProviderRunner.class)
public class SidelineSpoutTest {
    // For logging within the test.
    private static final Logger logger = LoggerFactory.getLogger(SidelineSpoutTest.class);

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
        topicName = SidelineSpoutTest.class.getSimpleName() + Clock.systemUTC().millis();

        // Create namespace
        getKafkaTestServer().createTopic(topicName);
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
        final VirtualSpoutIdentifier firehoseIdentifier = new DefaultVirtualSpoutIdentifier(consumerIdPrefix + ":main");

        // Create our Config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix);

        // Create some stand-in mocks.
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create our spout, add references to our static trigger, and call open().
        final SidelineSpout spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // wait for firehose vspout to start
        waitForVirtualSpouts(spout, 1);

        // Produce records into kafka
        List<ProducedKafkaRecord<byte[], byte[]>> producedRecords = produceRecords(numberOfRecordsToPublish, 0);

        // Wait for our 'firehose' spout instance should pull these 3 records in when we call nextTuple().
        // Consuming from kafka is an async process de-coupled from the call to nextTuple().  Because of this it could
        // take several calls to nextTuple() before the messages are pulled in from kafka behind the scenes and available
        // to be emitted.
        // Grab out the emissions so we can validate them.
        List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, numberOfRecordsToPublish);

        // Validate the tuples are what we published into kafka
        validateTuplesFromSourceMessages(producedRecords, spoutEmissions, firehoseIdentifier);

        // Lets ack our tuples, this should commit offsets 0 -> 2. (0,1,2)
        ackTuples(spout, spoutEmissions);

        // Sanity test, we should have a single VirtualSpout instance at this point, the fire hose instance
        assertTrue("Should have a single VirtualSpout instance", spout.hasVirtualSpout(firehoseIdentifier));

        // Create a static message filter, this allows us to easily start filtering messages.
        // It should filter ALL messages
        final SidelineRequest request = new SidelineRequest(new SidelineRequestIdentifier("test"), new StaticMessageFilter());
        final SidelineVirtualSpoutIdentifier sidelineIdentifier = new SidelineVirtualSpoutIdentifier(consumerIdPrefix, request.id);

        // Send a new start request with our filter.
        // This means that our starting offset for the sideline'd data should start at offset 3 (we acked offsets 0, 1, 2)
        StaticTrigger.sendStartRequest(request);

        // Produce another 3 records into kafka.
        producedRecords = produceRecords(numberOfRecordsToPublish, 0);

        // We basically want the time that would normally pass before we check that there are no new tuples
        // Call next tuple, it should NOT receive any tuples because
        // all tuples are filtered.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 3000L);

        // TODO: Validate something in here
        StaticTrigger.sendResumeRequest(request);

        // Send a stop sideline request
        StaticTrigger.sendResolveRequest(request);

        // Wait for the sideline vspout to start,
        waitForVirtualSpouts(spout, 2);
        assertTrue("Has sideline spout instance", spout.hasVirtualSpout(sidelineIdentifier));

        // Then ask the spout for tuples, we should get back the tuples that were produced while
        // sidelining was active.  These tuples should come from the VirtualSpout started by the Stop request.
        spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, numberOfRecordsToPublish);

        // We should validate these emissions
        validateTuplesFromSourceMessages(
            producedRecords,
            spoutEmissions,
            sidelineIdentifier
        );

        // Call next tuple a few more times to be safe nothing else comes in.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 0L);

        // Validate that VirtualSpouts are NOT closed out, but still waiting for unacked tuples.
        // We should have 2 instances at this point, the firehose, and 1 sidelining instance.
        waitForVirtualSpouts(spout, 2);

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
        validateTuplesFromSourceMessages(producedRecords, spoutEmissions, firehoseIdentifier);

        // Close out
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
        final VirtualSpoutIdentifier firehoseIdentifier = new DefaultVirtualSpoutIdentifier(consumerIdPrefix + ":main");

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix);

        // Use zookeeper persistence manager
        config.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, ZookeeperPersistenceAdapter.class.getName());
        config.put(
            SidelineConfig.PERSISTENCE_ADAPTER_CLASS,
            com.salesforce.storm.spout.sideline.persistence.ZookeeperPersistenceAdapter.class.getName()
        );

        // Some mock stuff to get going
        TopologyContext topologyContext = new MockTopologyContext();
        MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create our spout, add references to our static trigger, and call open().
        SidelineSpout spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // Call next tuple 6 times, getting offsets 0,1,2,3,4,5
        final List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, 6);

        // Validate its the messages we expected
        validateEmission(producedRecords.get(0), spoutEmissions.get(0), firehoseIdentifier, "default");
        validateEmission(producedRecords.get(1), spoutEmissions.get(1), firehoseIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(2), spoutEmissions.get(2), firehoseIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(3), spoutEmissions.get(3), firehoseIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(4), spoutEmissions.get(4), firehoseIdentifier, expectedStreamId);
        validateEmission(producedRecords.get(5), spoutEmissions.get(5), firehoseIdentifier, expectedStreamId);

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

        // Produce 5 more messages into kafka, should be offsets [10,11,12,13,14]
        final List<ProducedKafkaRecord<byte[], byte[]>> additionalProducedRecords = produceRecords(5, 0);

        // Call nextTuple() 4 more times, we should get the remaining first 10 records because they were already buffered.
        spoutEmissions.addAll(consumeTuplesFromSpout(spout, spoutOutputCollector, 4));

        // We'll validate them
        validateTuplesFromSourceMessages(producedRecords, spoutEmissions, firehoseIdentifier);

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

        // TODO: Validate something in here
        StaticTrigger.sendResumeRequest(request);

        // Send a stop sideline request
        StaticTrigger.sendResolveRequest(request);
        final SidelineVirtualSpoutIdentifier sidelineIdentifier = new SidelineVirtualSpoutIdentifier(consumerIdPrefix, request.id);

        // Verify 2 VirtualSpouts are running
        waitForVirtualSpouts(spout, 2);

        // Call nextTuple() 3 times
        List<SpoutEmission> sidelinedEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, 3);

        // Verify we get offsets [4,5,6] by validating the tuples
        validateEmission(
            producedRecords.get(4),
            sidelinedEmissions.get(0),
            sidelineIdentifier,
            expectedStreamId
        );
        validateEmission(
            producedRecords.get(5),
            sidelinedEmissions.get(1),
            sidelineIdentifier,
            expectedStreamId
        );
        validateEmission(
            producedRecords.get(6),
            sidelinedEmissions.get(2),
            sidelineIdentifier,
            expectedStreamId
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
            sidelineIdentifier
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
        validateTuplesFromSourceMessages(lastProducedRecords, lastSpoutEmissions, firehoseIdentifier);

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
            firehoseIdentifier
        );

        // Verify no more tuples
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 0L);

        // Stop spout.
        spout.close();
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
    private void ackTuples(final SidelineSpout spout, final List<SpoutEmission> spoutEmissions) {
        if (spoutEmissions.isEmpty()) {
            throw new RuntimeException("You cannot ack an empty list!  You probably have a bug in your test.");
        }

        // Ack each one.
        for (SpoutEmission emission: spoutEmissions) {
            spout.ack(emission.getMessageId());
        }

        // Make method accessible.
        try {
            // TODO find better way to do this w/o reflections.
            final Field field = DynamicSpout.class.getDeclaredField("messageBus");
            field.setAccessible(true);

            // Grab reference to message bus.
            final MessageBus messageBus = (MessageBus) field.get(spout);

            // Acking tuples is an async process, so we need to make sure they get picked up
            // and processed before continuing.
            await()
                .atMost(6500, TimeUnit.MILLISECONDS)
                .until(() -> {
                    // Wait for our tuples to get popped off the acked queue.
                    return messageBus.ackSize() == 0;
                }, equalTo(true));

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
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
     * @param expectedVirtualSpoutId virtual spout id we expected
     */
    private void validateTuplesFromSourceMessages(
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecords,
        final List<SpoutEmission> spoutEmissions,
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
            validateEmission(producedRecord, spoutEmission, expectedVirtualSpoutId, "default");
        }
    }

    /**
     * Waits for virtual spouts to close out.
     * @param spout - The spout instance
     * @param howManyVirtualSpoutsWeWantLeft - Wait until this many virtual spouts are left running.
     */
    private void waitForVirtualSpouts(final SidelineSpout spout, final int howManyVirtualSpoutsWeWantLeft) {
        try {
            // TODO find better way to do this avoiding reflections
            final Field field = DynamicSpout.class.getDeclaredField("spoutCoordinator");
            field.setAccessible(true);
            final SpoutCoordinator spoutCoordinator = (SpoutCoordinator) field.get(spout);

            await()
                .atMost(5, TimeUnit.SECONDS)
                .until(spoutCoordinator::getTotalSpouts, equalTo(howManyVirtualSpoutsWeWantLeft));
            assertEquals(
                "We should have " + howManyVirtualSpoutsWeWantLeft + " virtual spouts running",
                howManyVirtualSpoutsWeWantLeft,
                spoutCoordinator.getTotalSpouts()
            );
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
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
     */
    private Map<String, Object> getDefaultConfig(final String consumerIdPrefix) {
        // Generate a unique zkRootNode for each test
        final String uniqueZkRootNode = "/sideline-spout-test/testRun" + System.currentTimeMillis();

        final Map<String, Object> config = SpoutConfig.setDefaults(SidelineConfig.setDefaults(Maps.newHashMap()));

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

        // Enable sideline options
        config.put(SidelineConfig.TRIGGER_CLASS, StaticTrigger.class.getName());
        config.put(SidelineConfig.PERSISTENCE_ZK_SERVERS, Lists.newArrayList(getKafkaTestServer().getZookeeperConnectString()));
        config.put(SidelineConfig.PERSISTENCE_ZK_ROOT, uniqueZkRootNode);
        // Use In Memory Persistence manager, if you need state persistence, over ride this in your test.
        config.put(
            SidelineConfig.PERSISTENCE_ADAPTER_CLASS,
            com.salesforce.storm.spout.sideline.persistence.InMemoryPersistenceAdapter.class.getName()
        );

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
