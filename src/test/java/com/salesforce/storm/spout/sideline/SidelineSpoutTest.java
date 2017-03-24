package com.salesforce.storm.spout.sideline;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.filter.StaticMessageFilter;
import com.salesforce.storm.spout.sideline.kafka.KafkaTestServer;
import com.salesforce.storm.spout.sideline.kafka.SidelineConsumerTest;
import com.salesforce.storm.spout.sideline.kafka.retryManagers.FailedTuplesFirstRetryManager;
import com.salesforce.storm.spout.sideline.mocks.MockTopologyContext;
import com.salesforce.storm.spout.sideline.mocks.output.MockSpoutOutputCollector;
import com.salesforce.storm.spout.sideline.mocks.output.SpoutEmission;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.StaticTrigger;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.shade.com.google.common.base.Charsets;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsGetter;
import org.apache.storm.utils.Utils;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * End to End integration testing of Sideline Spout under various scenarios.
 */
@RunWith(DataProviderRunner.class)
public class SidelineSpoutTest {
    // For logging within the test.
    private static final Logger logger = LoggerFactory.getLogger(SidelineSpoutTest.class);

    // Our internal Kafka and Zookeeper Server, used to test against.
    private KafkaTestServer kafkaTestServer;

    // Gets set to our randomly generated topic created for the test.
    private String topicName;

    /**
     * By default, no exceptions should be thrown.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
        topicName = SidelineConsumerTest.class.getSimpleName() + Clock.systemUTC().millis();

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
     * Validates that we require the ConsumerIdPrefix configuration value,
     * and if its missing we toss an IllegalStateException.
     */
    @Test
    public void testMissingRequiredConfigurationConsumerIdPrefix() {
        // Create our config missing the consumerIdPrefix
        final Map<String, Object> config = getDefaultConfig(null, null);
        config.remove(SidelineSpoutConfig.CONSUMER_ID_PREFIX);

        // Some mock stuff to get going
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        final SidelineSpout spout = new SidelineSpout(config);

        // When we call open, we expect illegal state exception about our missing configuration item
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage(containsString(SidelineSpoutConfig.CONSUMER_ID_PREFIX));

        // Call open
        spout.open(config, topologyContext, spoutOutputCollector);
    }

    /**
     * Our most simple end-2-end test.
     * This test stands up our spout and ask it to consume from our kafka topic.
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
        // Define how many tuples we should push into the topic, and then consume back out.
        final int emitTupleCount = 10;

        // Define our ConsumerId prefix
        final String consumerIdPrefix = "TestSidelineSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, null);

        // If we have a stream Id we should be configured with
        if (configuredStreamId != null) {
            // Drop it into our configuration.
            config.put(SidelineSpoutConfig.OUTPUT_STREAM_ID, configuredStreamId);
        }

        // Some mock storm topology stuff to get going
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        final SidelineSpout spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Call next tuple, topic is empty, so nothing should get emitted.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 2, 0L);

        // Lets produce some data into the topic
        final List<KafkaRecord<byte[], byte[]>> producedRecords = produceRecords(emitTupleCount);

        // Now consume tuples generated from the messages we published into kafka.
        final List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, emitTupleCount);

        // Validate the tuples that got emitted are what we expect based on what we published into kafka
        validateTuplesFromSourceKafkaMessages(producedRecords, spoutEmissions, expectedStreamId);

        // Call next tuple a few more times to make sure nothing unexpected shows up.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 3, 0L);

        // Cleanup.
        spout.close();
    }

    /**
     * End-to-End test over the fail() method using the {@link FailedTuplesFirstRetryManager}
     * retry manager.
     *
     * This test stands up our spout and ask it to consume from our kafka topic.
     * We publish some data into kafka, and validate that when we call nextTuple() on
     * our spout that we get out our messages that were published into kafka.
     *
     * We then fail some tuples and validate that they get replayed.
     * We ack some tuples and then validate that they do NOT get replayed.
     *
     * This does not make use of any side lining logic, just simple consuming from the
     * 'fire hose' topic.
     */
    @Test
    public void doBasicFailTest() throws InterruptedException {
        // Define how many tuples we should push into the topic, and then consume back out.
        final int emitTupleCount = 10;

        // Define our ConsumerId prefix
        final String consumerIdPrefix = "SidelineSpout-";

        // Define our output stream id
        final String expectedStreamId = "default";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);

        // Configure to use our FailedTuplesFirstRetryManager retry manager.
        config.put(SidelineSpoutConfig.RETRY_MANAGER_CLASS, FailedTuplesFirstRetryManager.class.getName());

        // Some mock stuff to get going
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        final SidelineSpout spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Call next tuple, topic is empty, so should get nothing.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 100L);

        // Lets produce some data into the topic
        List<KafkaRecord<byte[], byte[]>> producedRecords = produceRecords(emitTupleCount);

        // Now loop and get our tuples
        List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, emitTupleCount);

        // Now lets validate that what we got out of the spout is what we actually expected.
        validateTuplesFromSourceKafkaMessages(producedRecords, spoutEmissions, expectedStreamId);

        // Call next tuple a few more times to make sure nothing unexpected shows up.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 100L);

        // Now lets fail our tuples.
        failTuples(spout, spoutEmissions);

        // And lets call nextTuple, and we should get the same emissions back out because we called fail on them
        // And our retry manager should replay them first chance it gets.
        spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, emitTupleCount);
        validateTuplesFromSourceKafkaMessages(producedRecords, spoutEmissions, expectedStreamId);

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
        validateTuplesFromSourceKafkaMessages(producedRecords, replayedEmissions, expectedStreamId);

        // Now lets ack these
        ackTuples(spout, replayedEmissions);

        // And validate nextTuple gives us nothing new
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 100L);

        // Cleanup.
        spout.close();
    }

    /**
     * Our most basic End 2 End test that includes basic sidelining.
     * First we stand up our spout and produce some records into the kafka topic its consuming from.
     * Records 1 and 2 we get out of the spout.
     * Then we enable sidelining, and call nextTuple(), the remaining records should not be emitted.
     * We stop sidelining, this should cause a virtual spout to be started within the spout.
     * Calling nextTuple() should get back the records that were previously skipped.
     * We produce additional records into the topic.
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

        // Create a static trigger for being able to easily make start and stop requests.
        final StaticTrigger staticTrigger = new StaticTrigger();

        // Create our spout, add references to our static trigger, and call open().
        final SidelineSpout spout = new SidelineSpout(config);
        spout.setStartingTrigger(staticTrigger);
        spout.setStoppingTrigger(staticTrigger);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Produce records into kafka
        List<KafkaRecord<byte[], byte[]>> producedRecords = produceRecords(numberOfRecordsToPublish);

        // Wait for our 'firehose' spout instance should pull these 3 records in when we call nextTuple().
        // Consuming from kafka is an async process de-coupled from the call to nextTuple().  Because of this it could
        // take several calls to nextTuple() before the messages are pulled in from kafka behind the scenes and available
        // to be emitted.
        // Grab out the emissions so we can validate them.
        List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, numberOfRecordsToPublish);

        // Validate the tuples are what we published into kafka
        validateTuplesFromSourceKafkaMessages(producedRecords, spoutEmissions, expectedStreamId);

        // Lets ack our tuples, this should commit offsets 0 -> 2. (0,1,2)
        ackTuples(spout, spoutEmissions);

        // Sanity test, we should have a single VirtualSpout instance at this point, the fire hose instance
        assertEquals("Should have a single VirtualSpout instance", 1, spout.getCoordinator().getTotalSpouts());

        // Create a static message filter, this allows us to easily start filtering messages.
        // It should filter ALL messages
        final StaticMessageFilter staticMessageFilter = new StaticMessageFilter();

        final SidelineRequest request = new SidelineRequest(staticMessageFilter);

        // Send a new start request with our filter.
        // This means that our starting offset for the sideline'd data should start at offset 3 (we acked offsets 0, 1, 2)
        staticTrigger.sendStartRequest(request);

        // Produce another 3 records into kafka.
        producedRecords = produceRecords(numberOfRecordsToPublish);

        // We basically want the time that would normally pass before we check that there are no new tuples
        // Call next tuple, it should NOT receive any tuples because
        // all tuples are filtered.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 3000L);

        // Send a stop sideline request
        staticTrigger.sendStopRequest(request);

        // We need to wait a bit for the sideline spout instance to spin up and start consuming
        spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, numberOfRecordsToPublish);

        // We should validate these emissions
        validateTuplesFromSourceKafkaMessages(producedRecords, spoutEmissions, expectedStreamId);

        // Call next tuple a few more times to be safe nothing else comes in.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 0L);

        // Validate that virtualsideline spouts are NOT closed out, but still waiting for unacked tuples.
        // We should have 2 instances at this point, the firehose, and 1 sidelining instance.
        assertEquals("We should have 2 virtual spouts running", 2, spout.getCoordinator().getTotalSpouts());

        // Lets ack our messages.
        ackTuples(spout, spoutEmissions);

        // Validate that virtualsideline spout instance closes out once finished acking all processed tuples.
        // We need to wait for the monitor thread to run to clean it up.
        waitForVirtualSpouts(spout, 1);

        // Produce some more records, verify they come in the firehose.
        producedRecords = produceRecords(numberOfRecordsToPublish);

        // Wait up to 5 seconds, our 'firehose' spout instance should pull these 3 records in when we call nextTuple().
        spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, numberOfRecordsToPublish);

        // Loop over what we produced into kafka and validate them
        validateTuplesFromSourceKafkaMessages(producedRecords, spoutEmissions, expectedStreamId);

        // Close out
        spout.close();
    }

    /**
     * This test stands up a spout instance and begins consuming from a topic.
     * Halfway thru consuming all the messages published in that topic we will shutdown
     * the spout gracefully.
     *
     * We'll create a new instance of the spout and fire it up, then validate that it resumes
     * consuming from where it left off.
     *
     * Assumptions made in this test:
     *   - single partition topic
     *   - using ZK persistence manager to maintain state between spout instances/restarts.
     */
    @Test
    public void testResumingForFirehoseVirtualSpout() throws InterruptedException, IOException, KeeperException {
        // Produce 10 messages into kafka (offsets 0->9)
        final List<KafkaRecord<byte[], byte[]>> producedRecords = Collections.unmodifiableList(produceRecords(10));

        // Create spout
        // Define our output stream id
        final String expectedStreamId = "default";
        final String consumerIdPrefix = "TestSidelineSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);

        // Use zookeeper persistence manager
        config.put(SidelineSpoutConfig.PERSISTENCE_MANAGER_CLASS, "com.salesforce.storm.spout.sideline.persistence.ZookeeperPersistenceManager");

        // Some mock stuff to get going
        TopologyContext topologyContext = new MockTopologyContext();
        MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        SidelineSpout spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Call next tuple 6 times, getting offsets 0,1,2,3,4,5
        final List<SpoutEmission> spoutEmissions = Collections.unmodifiableList(consumeTuplesFromSpout(spout, spoutOutputCollector, 6));

        // Validate its the messages we expected
        validateEmission(producedRecords.get(0), spoutEmissions.get(0), consumerIdPrefix, expectedStreamId);
        validateEmission(producedRecords.get(1), spoutEmissions.get(1), consumerIdPrefix, expectedStreamId);
        validateEmission(producedRecords.get(2), spoutEmissions.get(2), consumerIdPrefix, expectedStreamId);
        validateEmission(producedRecords.get(3), spoutEmissions.get(3), consumerIdPrefix, expectedStreamId);
        validateEmission(producedRecords.get(4), spoutEmissions.get(4), consumerIdPrefix, expectedStreamId);
        validateEmission(producedRecords.get(5), spoutEmissions.get(5), consumerIdPrefix, expectedStreamId);

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
        spout = null;

        // Create fresh new spoutOutputCollector & topology context
        topologyContext = new MockTopologyContext();
        spoutOutputCollector = new MockSpoutOutputCollector();

        // Create fresh new instance of spout & call open all with the same config
        spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // Call next tuple to get remaining tuples out.
        // It should give us offsets [4,5,6,7,8,9]
        final List<SpoutEmission> spoutEmissionsAfterResume = Collections.unmodifiableList(consumeTuplesFromSpout(spout, spoutOutputCollector, 6));

        // Validate no further tuples
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 0L);

        // Validate its the tuples we expect [4,5,6,7,8,9]
        validateEmission(producedRecords.get(4), spoutEmissionsAfterResume.get(0), consumerIdPrefix, expectedStreamId);
        validateEmission(producedRecords.get(5), spoutEmissionsAfterResume.get(1), consumerIdPrefix, expectedStreamId);
        validateEmission(producedRecords.get(6), spoutEmissionsAfterResume.get(2), consumerIdPrefix, expectedStreamId);
        validateEmission(producedRecords.get(7), spoutEmissionsAfterResume.get(3), consumerIdPrefix, expectedStreamId);
        validateEmission(producedRecords.get(8), spoutEmissionsAfterResume.get(4), consumerIdPrefix, expectedStreamId);
        validateEmission(producedRecords.get(9), spoutEmissionsAfterResume.get(5), consumerIdPrefix, expectedStreamId);

        // Ack all tuples.
        ackTuples(spout, spoutEmissionsAfterResume);

        // Stop the spout.
        // A graceful shutdown of the spout should have the consumer state flushed to the persistence layer.
        spout.close();
        spout = null;

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
        final List<KafkaRecord<byte[], byte[]>> producedRecords = Collections.unmodifiableList(produceRecords(10));

        // Create spout
        // Define our output stream id
        final String expectedStreamId = "default";
        final String consumerIdPrefix = "TestSidelineSpout";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, expectedStreamId);

        // Use zookeeper persistence manager
        config.put(SidelineSpoutConfig.PERSISTENCE_MANAGER_CLASS, "com.salesforce.storm.spout.sideline.persistence.ZookeeperPersistenceManager");

        // Some mock stuff to get going
        TopologyContext topologyContext = new MockTopologyContext();
        MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create a static trigger for being able to easily make start and stop requests.
        StaticTrigger staticTrigger = new StaticTrigger();

        // Create our spout, add references to our static trigger, and call open().
        SidelineSpout spout = new SidelineSpout(config);
        spout.setStartingTrigger(staticTrigger);
        spout.setStoppingTrigger(staticTrigger);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Call next tuple 6 times, getting offsets 0,1,2,3,4,5
        final List<SpoutEmission> spoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, 6);

        // Validate its the messages we expected
        validateEmission(producedRecords.get(0), spoutEmissions.get(0), consumerIdPrefix, expectedStreamId);
        validateEmission(producedRecords.get(1), spoutEmissions.get(1), consumerIdPrefix, expectedStreamId);
        validateEmission(producedRecords.get(2), spoutEmissions.get(2), consumerIdPrefix, expectedStreamId);
        validateEmission(producedRecords.get(3), spoutEmissions.get(3), consumerIdPrefix, expectedStreamId);
        validateEmission(producedRecords.get(4), spoutEmissions.get(4), consumerIdPrefix, expectedStreamId);
        validateEmission(producedRecords.get(5), spoutEmissions.get(5), consumerIdPrefix, expectedStreamId);

        // We will ack offsets in the following order: 2,0,1,3,5
        // This should give us a completed offset of [0,1,2,3] <-- last committed offset should be 3
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(2)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(0)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(1)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(3)));
        ackTuples(spout, Lists.newArrayList(spoutEmissions.get(5)));

        // Create a static message filter, this allows us to easily start filtering messages.
        // It should filter ALL messages
        final SidelineRequest request = new SidelineRequest(new StaticMessageFilter());

        // Send a new start request with our filter.
        // This means that our starting offset for the sideline'd data should start at offset 3 (we acked offsets 0, 1, 2)
        staticTrigger.sendStartRequest(request);

        // Produce 5 more messages into kafka, should be offsets [10,11,12,13,14]
        List<KafkaRecord<byte[], byte[]>> additionalProducedRecords = produceRecords(5);

        // Call nextTuple() 4 more times, we should get the remaining first 10 records because they were already buffered.
        spoutEmissions.addAll(consumeTuplesFromSpout(spout, spoutOutputCollector, 4));

        // We'll validate them
        validateTuplesFromSourceKafkaMessages(producedRecords, spoutEmissions, expectedStreamId);

        // But if we call nextTuple() 5 more times, we should never get the additional 5 records we produced.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 5, 100L);

        // Lets not ack any more tuples from the fire hose, that means the last completed
        // offset on the fire hose spout should still be 3.

        // Stop the spout.
        // A graceful shutdown of the spout should have the consumer state flushed to the persistence layer.
        spout.close();
        spout = null;

        // TODO:
        // Inspect Firehose consumer state, it should show largest offset being 3
        // Inspect Sideline Request: It should show starting at offset #3

        // A little debug log
        logger.info("=== Starting spout again");
        logger.info("=== This verifies that when we resume, we pickup started sideling requests and continue filtering");

        // Create new Spout instance and start
        topologyContext = new MockTopologyContext();
        spoutOutputCollector = new MockSpoutOutputCollector();

        // Create a new static trigger for being able to easily make start and stop requests.
        staticTrigger = new StaticTrigger();

        // Create our spout, add references to our static trigger, and call open().
        spout = new SidelineSpout(config);
        spout.setStartingTrigger(staticTrigger);
        spout.setStoppingTrigger(staticTrigger);
        spout.open(config, topologyContext, spoutOutputCollector);

        // Wait 3 seconds, then verify we have a single virtual spouts running
        Thread.sleep(3000L);
        waitForVirtualSpouts(spout, 1);

        // Call nextTuple() 20 times, we should get no tuples, last committed offset was 3, so this means we asked for
        // offsets [4,5,6,7,8,9,10,11,12,13,14] <- last committed offset now 14 on firehose.
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 20, 100L);

        // Send a stop sideline request
        staticTrigger.sendStopRequest(request);

        // Verify 2 VirtualSpouts are running
        waitForVirtualSpouts(spout, 2);

        // Call nextTuple() 3 times
        List<SpoutEmission> sidelinedEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, 3);

        // Verify we get offsets [4,5,6] by validating the tuples
        validateEmission(producedRecords.get(4), sidelinedEmissions.get(0), "NotUsed", expectedStreamId);
        validateEmission(producedRecords.get(5), sidelinedEmissions.get(1), "NotUsed", expectedStreamId);
        validateEmission(producedRecords.get(6), sidelinedEmissions.get(2), "NotUsed", expectedStreamId);

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

        // Create a new static trigger for being able to easily make start and stop requests.
        staticTrigger = new StaticTrigger();

        // Create our spout, add references to our static trigger, and call open().
        spout = new SidelineSpout(config);
        spout.setStartingTrigger(staticTrigger);
        spout.setStoppingTrigger(staticTrigger);
        spout.open(config, topologyContext, spoutOutputCollector);

        // Verify we have a 2 virtual spouts running
        waitForVirtualSpouts(spout, 2);

        // Since last committed offset should be 6,
        // Call nextTuple() 8 times to get offsets [7,8,9,10,11,12,13,14]
        sidelinedEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, 8);

        // Verify we get offsets [7,8,9,10,11,12,13,14] by validating the tuples
        // Gather up the expected records
        List<KafkaRecord<byte[], byte[]>> sidelineKafkaRecords = Lists.newArrayList();
        sidelineKafkaRecords.addAll(producedRecords.subList(7, 10));
        sidelineKafkaRecords.addAll(additionalProducedRecords);

        // Validate em.
        validateTuplesFromSourceKafkaMessages(sidelineKafkaRecords, sidelinedEmissions, expectedStreamId);

        // call nextTuple() several times, get nothing back
        validateNextTupleEmitsNothing(spout, spoutOutputCollector, 10, 0L);

        // Ack offsets [4,5,6,7,8,9,10,11,12,13,14] => committed offset should be 14 now on sideline consumer.
        ackTuples(spout, sidelinedEmissions);

        // Verify 2nd VirtualSpout shuts off
        waitForVirtualSpouts(spout, 1);
        logger.info("=== Virtual Spout should be closed now... just fire hose left!");

        // Produce 5 messages into Kafka topic with offsets [15,16,17,18,19]
        List<KafkaRecord<byte[], byte[]>> lastProducedRecords = produceRecords(5);

        // Call nextTuple() 5 times,
        List<SpoutEmission> lastSpoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, 5);

        // verify we get the tuples [15,16,17,18,19]
        validateTuplesFromSourceKafkaMessages(lastProducedRecords, lastSpoutEmissions, expectedStreamId);

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

        // Create a new static trigger for being able to easily make start and stop requests.
        staticTrigger = new StaticTrigger();

        // A little debug log
        logger.info("=== Starting spout for last time");
        logger.info("=== This last bit verifies that we don't resume finished sideline requests");

        // Create our spout, add references to our static trigger, and call open().
        spout = new SidelineSpout(config);
        spout.setStartingTrigger(staticTrigger);
        spout.setStoppingTrigger(staticTrigger);
        spout.open(config, topologyContext, spoutOutputCollector);

        // Verify we have a single 1 virtual spouts running,
        // This makes sure that we don't resume a previously completed sideline request.
        Thread.sleep(3000);
        waitForVirtualSpouts(spout, 1);

        // Call nextTuple() 3 times,
        // verify we get offsets [17,18,19]
        lastSpoutEmissions = consumeTuplesFromSpout(spout, spoutOutputCollector, 3);

        // Validate we got the right offset [17,18,19]
        validateTuplesFromSourceKafkaMessages(lastProducedRecords.subList(2,5), lastSpoutEmissions, expectedStreamId);

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
     * @param expectedConsumerId - What consumerId these emissions should be associated with.
     * @param expectedOutputStreamId - What stream these emissions should have been emitted down.
     */
    private void validateEmission(final KafkaRecord<byte[], byte[]> sourceProducerRecord, final SpoutEmission spoutEmission, final String expectedConsumerId, final String expectedOutputStreamId) {
        // Now find its corresponding tuple
        assertNotNull("Not null sanity check", spoutEmission);
        assertNotNull("Not null sanity check", sourceProducerRecord);

        // Validate Message Id
        assertNotNull("Should have non-null messageId", spoutEmission.getMessageId());
        assertTrue("Should be instance of TupleMessageId", spoutEmission.getMessageId() instanceof TupleMessageId);

        // Grab the messageId and validate it
        final TupleMessageId messageId = (TupleMessageId) spoutEmission.getMessageId();
        assertEquals("Expected Topic Name in MessageId", sourceProducerRecord.getTopic(), messageId.getTopic());
        assertEquals("Expected PartitionId found", sourceProducerRecord.getPartition(), messageId.getPartition());
        assertEquals("Expected MessageOffset found", sourceProducerRecord.getOffset(), messageId.getOffset());

        // TODO - how do we get the consumer id for sideline spouts since they're auto-generated?
        //assertEquals("Expected Source Consumer Id", expectedConsumerId, messageId.getSrcVirtualSpoutId());

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
    private void ackTuples(final SidelineSpout spout, List<SpoutEmission> spoutEmissions) {
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
                Map<String, Queue<TupleMessageId>> queueMap = spout.getCoordinator().getAckedTuplesQueue();
                for (String key : queueMap.keySet()) {
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
    private void failTuples(final SidelineSpout spout, List<SpoutEmission> spoutEmissions) {
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
                Map<String, Queue<TupleMessageId>> queueMap = spout.getCoordinator().getFailedTuplesQueue();
                for (String key : queueMap.keySet()) {
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
     * Verifies that you do not define an output stream via the SidelineSpoutConfig
     * declareOutputFields() method with default to using 'default' stream.
     */
    @Test
    public void testDeclareOutputFields_without_stream() {
        // Create config with null stream id config option.
        final Map<String,Object> config = getDefaultConfig("SidelineSpout-", null);

        final OutputFieldsGetter declarer = new OutputFieldsGetter();

        // Create spout, but don't call open
        final SidelineSpout spout = new SidelineSpout(config);

        // call declareOutputFields
        spout.declareOutputFields(declarer);

        // Validate results.
        final Map<String, StreamInfo> fieldsDeclaration = declarer.getFieldsDeclaration();

        assertTrue(fieldsDeclaration.containsKey(Utils.DEFAULT_STREAM_ID));
        assertEquals(
            fieldsDeclaration.get(Utils.DEFAULT_STREAM_ID).get_output_fields(),
            Lists.newArrayList("key", "value")
        );

        spout.close();
    }

    /**
     * Verifies that you can define an output stream via the SidelineSpoutConfig and it gets used
     * in the declareOutputFields() method.
     */
    @Test
    public void testDeclareOutputFields_with_stream() {
        final String streamId = "foobar";
        final Map<String,Object> config = getDefaultConfig("SidelineSpout-", streamId);

        final OutputFieldsGetter declarer = new OutputFieldsGetter();

        // Create spout, but do not call open.
        final SidelineSpout spout = new SidelineSpout(config);

        // call declareOutputFields
        spout.declareOutputFields(declarer);

        final Map<String, StreamInfo> fieldsDeclaration = declarer.getFieldsDeclaration();

        assertTrue(fieldsDeclaration.containsKey(streamId));
        assertEquals(
            fieldsDeclaration.get(streamId).get_output_fields(),
            Lists.newArrayList("key", "value")
        );

        spout.close();
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

    /**
     * Utility method that calls nextTuple() on the passed in spout, and then validates that it never emitted anything.
     * @param spout - The spout instance to call nextTuple() on.
     * @param collector - The spout's output collector that would receive the tuples if any were emitted.
     * @param numberOfAttempts - How many times to call nextTuple()
     */
    private void validateNextTupleEmitsNothing(SidelineSpout spout, MockSpoutOutputCollector collector, int numberOfAttempts, long delayInMs) {
        try {
            Thread.sleep(delayInMs);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Try a certain number of times
        final int originalSize = collector.getEmissions().size();
        for (int x=0; x<numberOfAttempts; x++) {
            spout.nextTuple();
            assertEquals("No new tuple emits", originalSize, collector.getEmissions().size());
        }
    }

    /**
     * Utility method that calls nextTuple() on the passed in spout, and then returns new tuples that the spout emitted.
     * @param spout - The spout instance to call nextTuple() on.
     * @param collector - The spout's output collector that would receive the tuples if any were emitted.
     * @param numberOfTuples - How many new tuples we expect to get out of the spout instance.
     */
    private List<SpoutEmission> consumeTuplesFromSpout(SidelineSpout spout, MockSpoutOutputCollector collector, int numberOfTuples) {
        logger.info("[TEST] Attempting to consume {} tuples from spout", numberOfTuples);

        // Create a new list for the emissions we expect to get back
        List<SpoutEmission> newEmissions = Lists.newArrayList();

        // Determine how many emissions are already in the collector
        final int existingEmissionsCount = collector.getEmissions().size();

        // Call next tuple N times
        for (int x=0; x<numberOfTuples; x++) {
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
     *
     * @param producedRecords - The original records produced into kafka.
     * @param spoutEmissions - The tuples that got emitted out from the spout
     * @param expectedStreamId - The stream id that we expected the tuples to get emitted out on.
     */
    private void validateTuplesFromSourceKafkaMessages(List<KafkaRecord<byte[], byte[]>> producedRecords, List<SpoutEmission> spoutEmissions, final String expectedStreamId) {
        // Define some expected values for validation
        final String expectedConsumerId = "NOT VALIDATED YET";

        // Sanity check, make sure we have the same number of each.
        assertEquals("Should have same number of tuples as original messages, Produced Count: " + producedRecords.size() + " Emissions Count: " + spoutEmissions.size(), producedRecords.size(), spoutEmissions.size());

        // Iterator over what got emitted
        final Iterator<SpoutEmission> emissionIterator = spoutEmissions.iterator();

        // Loop over what we produced into kafka
        for (KafkaRecord<byte[], byte[]> producedRecord: producedRecords) {
            // Now find its corresponding tuple from our iterator
            final SpoutEmission spoutEmission = emissionIterator.next();

            // validate that they match
            validateEmission(producedRecord, spoutEmission, expectedConsumerId, expectedStreamId);
        }
    }

    /**
     * Waits for virtual spouts to close out.
     * @param spout - The spout instance
     * @param howManyVirtualSpoutsWeWantLeft - Wait until this many virtual spouts are left running.
     */
    private void waitForVirtualSpouts(SidelineSpout spout, int howManyVirtualSpoutsWeWantLeft) {
        await()
                .atMost(5, TimeUnit.SECONDS)
                .until(() -> {
                    return spout.getCoordinator().getTotalSpouts();
                }, equalTo(howManyVirtualSpoutsWeWantLeft));
        assertEquals("We should have " + howManyVirtualSpoutsWeWantLeft + " virtual spouts running", howManyVirtualSpoutsWeWantLeft, spout.getCoordinator().getTotalSpouts());
    }

    private List<KafkaRecord<byte[], byte[]>> produceRecords(int numberOfRecords) {
        // This holds the records we produced
        List<ProducerRecord<byte[], byte[]>> producedRecords = Lists.newArrayList();

        // This holds futures returned
        List<Future<RecordMetadata>> producerFutures = Lists.newArrayList();

        KafkaProducer producer = kafkaTestServer.getKafkaProducer("org.apache.kafka.common.serialization.ByteArraySerializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        for (int x=0; x<numberOfRecords; x++) {
            // Construct key and value
            long timeStamp = Clock.systemUTC().millis();
            String key = "key" + timeStamp;
            String value = "value" + timeStamp;

            // Construct filter
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, key.getBytes(Charsets.UTF_8), value.getBytes(Charsets.UTF_8));
            producedRecords.add(record);

            // Send it.
            producerFutures.add(producer.send(record));
        }

        // Publish to the topic and close.
        producer.flush();
        logger.info("Produce completed");
        producer.close();

        // Loop thru the futures, and build KafkaRecord objects
        List<KafkaRecord<byte[], byte[]>> kafkaRecords = Lists.newArrayList();
        try {
            for (int x=0; x<numberOfRecords; x++) {
                final RecordMetadata metadata = producerFutures.get(x).get();
                final ProducerRecord producerRecord = producedRecords.get(x);

                kafkaRecords.add(KafkaRecord.newInstance(metadata, producerRecord));
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return kafkaRecords;
    }

    /**
     * Generates a Storm Topology configuration with some sane values for our test scenarios.
     *
     * @param consumerIdPrefix - consumerId prefix to use.
     * @param configuredStreamId - What streamId we should emit tuples out of.
     */
    private Map<String, Object> getDefaultConfig(final String consumerIdPrefix, final String configuredStreamId) {
        final Map<String, Object> config = Maps.newHashMap();
        config.put(SidelineSpoutConfig.DESERIALIZER_CLASS, "com.salesforce.storm.spout.sideline.kafka.deserializer.Utf8StringDeserializer");
        config.put(SidelineSpoutConfig.RETRY_MANAGER_CLASS, "com.salesforce.storm.spout.sideline.kafka.retryManagers.NeverRetryManager");
        config.put(SidelineSpoutConfig.KAFKA_TOPIC, topicName);
        config.put(SidelineSpoutConfig.CONSUMER_ID_PREFIX, consumerIdPrefix);
        config.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort()));
        config.put(SidelineSpoutConfig.PERSISTENCE_ZK_SERVERS, Lists.newArrayList("localhost:" + kafkaTestServer.getZkServer().getPort()));
        config.put(SidelineSpoutConfig.PERSISTENCE_ZK_ROOT, "/sideline-spout-test");

        // Use In Memory Persistence manager, if you need state persistence, over ride this in your test.
        config.put(SidelineSpoutConfig.PERSISTENCE_MANAGER_CLASS, "com.salesforce.storm.spout.sideline.persistence.InMemoryPersistenceManager");

        // Configure SpoutMonitor thread to run every 1 second
        config.put(SidelineSpoutConfig.MONITOR_THREAD_INTERVAL_MS, 1000L);

        // Configure flushing consumer state every 1 second
        config.put(SidelineSpoutConfig.CONSUMER_STATE_FLUSH_INTERVAL_MS, 1000L);

        // For now use the Log Recorder
        config.put(SidelineSpoutConfig.METRICS_RECORDER_CLASS, "com.salesforce.storm.spout.sideline.metrics.LogRecorder");

        // If we have a stream Id we should be configured with
        if (configuredStreamId != null) {
            // Drop it into our configuration.
            config.put(SidelineSpoutConfig.OUTPUT_STREAM_ID, configuredStreamId);
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
     * Class that wraps relevant information about data that was published to kafka.
     * @param <K> - Object type of the Key written to kafka.
     * @param <V> - Object type of the Value written to kafka.
     */
    private static class KafkaRecord<K, V> {
        private final String topic;
        private final int partition;
        private final long offset;
        private final K key;
        private final V value;

        public KafkaRecord(String topic, int partition, long offset, K key, V value) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.key = key;
            this.value = value;
        }

        public static <K,V> KafkaRecord<K,V> newInstance(RecordMetadata recordMetadata, ProducerRecord<K,V> producerRecord) {
            return new KafkaRecord<K,V>(
                recordMetadata.topic(),
                recordMetadata.partition(),
                recordMetadata.offset(),
                producerRecord.key(),
                producerRecord.value()
            );
        }

        public String getTopic() {
            return topic;
        }

        public int getPartition() {
            return partition;
        }

        public long getOffset() {
            return offset;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }
}