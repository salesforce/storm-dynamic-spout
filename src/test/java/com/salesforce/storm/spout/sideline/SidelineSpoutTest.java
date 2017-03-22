package com.salesforce.storm.spout.sideline;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.filter.StaticMessageFilter;
import com.salesforce.storm.spout.sideline.kafka.KafkaTestServer;
import com.salesforce.storm.spout.sideline.kafka.SidelineConsumerTest;
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
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.shade.com.google.common.base.Charsets;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsGetter;
import org.apache.storm.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * End to End testing of Sideline Spout as a whole under various scenarios.
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
     * Validates that we require the ConsumerIdPrefix configuration value.
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
     * 'fire hose' topic.
     *
     * We run this test multiple times using a DataProvider to test using various output
     * stream Ids.
     */
    @Test
    @UseDataProvider("provideStreamIds")
    public void doBasicConsumingTest(final String configuredStreamId, final String expectedStreamId) throws InterruptedException {
        // Define how many tuples we should push into the topic, and then consume back out.
        final int emitTupleCount = 10;

        // Define our ConsumerId prefix
        final String consumerIdPrefix = "SidelineSpout-";

        // Create our config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, null);

        // If we have a stream Id we should be configured with
        if (configuredStreamId != null) {
            // Drop it into our configuration.
            config.put(SidelineSpoutConfig.OUTPUT_STREAM_ID, configuredStreamId);
        }

        // Some mock stuff to get going
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        final SidelineSpout spout = new SidelineSpout(config);
        spout.open(config, topologyContext, spoutOutputCollector);

        // validate our streamId
        assertEquals("Should be using appropriate output stream id", expectedStreamId, spout.getOutputStreamId());

        // Call next tuple, topic is empty, so should get nothing.
        spout.nextTuple();
        assertTrue("SpoutOutputCollector should have no emissions", spoutOutputCollector.getEmissions().isEmpty());

        // Lets produce some data into the topic
        final List<ProducerRecord<byte[], byte[]>> producedRecords = produceRecords(emitTupleCount);

        // Now loop and get our tuples
        for (int x=0; x<emitTupleCount; x++) {
            // Async call spout.nextTuple() because it can take a bit to fill the buffer.
            await()
                    .atMost(5, TimeUnit.SECONDS)
                    .until(() -> {
                        // Ask for next tuple
                        spout.nextTuple();

                        // Return how many tuples have been emitted so far
                        // It should be equal to our loop count + 1
                        return spoutOutputCollector.getEmissions().size();
                    }, equalTo(x+1));

            // Should have some emissions
            assertEquals("SpoutOutputCollector should have emissions", (x + 1), spoutOutputCollector.getEmissions().size());
        }
        // Now lets validate that what we got out of the spout is what we actually expected.
        final List<SpoutEmission> spoutEmissions = spoutOutputCollector.getEmissions();
        logger.info("Emissions: {}", spoutEmissions);

        // Define some expected values for validation
        final String expectedConsumerId = consumerIdPrefix + "firehose";
        int expectedMessageOffset = 0;

        // Loop over what we produced into kafka
        Iterator<SpoutEmission> emissionIterator = spoutEmissions.iterator();
        for (ProducerRecord<byte[], byte[]> producedRecord: producedRecords) {
            // Sanity check
            assertTrue("Should have more emissions", emissionIterator.hasNext());

            // Now find its corresponding tuple
            final SpoutEmission spoutEmission = emissionIterator.next();

            // validate it
            validateEmission(producedRecord, spoutEmission, expectedConsumerId, expectedMessageOffset, expectedStreamId);

            // Increment expected messageoffset for next iteration thru this loop.
            expectedMessageOffset++;
        }

        // Call next tuple a few more times to make sure nothing unexpected shows up.
        for (int x=0; x<3; x++) {
            // This shouldn't get any more tuples
            spout.nextTuple();

            // Should have some emissions
            assertEquals("SpoutOutputCollector should have same number of emissions", emitTupleCount, spoutOutputCollector.getEmissions().size());
        }

        // Cleanup.
        spout.close();
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
     * Our most basic End 2 End test that includes basic sidelining.
     * First we stand up our spout and produce some records into the kafka topic its consuming from.
     * Records 1 and 2 we get out of the spout.
     * Then we enable sidelining, and call nextTuple(), the remaining records should not be emitted.
     * We stop sidelining, this should cause a virtual spout to be started within the spout.
     * Calling nextTuple() should get back the records that were previously skipped.
     * We produce additional records into the topic.
     * Call nextTuple() and we should get them out.
     *
     * We run this test multiple times using a DataProvider to test using various output
     * stream Ids.
     */
    @Test
    @UseDataProvider("provideStreamIds")
    public void doTestWithSidelining(final String configuredStreamId, final String expectedStreamId) throws InterruptedException {
        // Define our ConsumerId prefix
        final String consumerIdPrefix = "SidelineSpout-";

        // Create our Config
        final Map<String, Object> config = getDefaultConfig(consumerIdPrefix, null);

        // If we have a stream Id we should be configured with
        if (configuredStreamId != null) {
            // Drop it into our configuration.
            config.put(SidelineSpoutConfig.OUTPUT_STREAM_ID, configuredStreamId);
        }

        // Configure how long we should wait for internal operations to complete.
        final long waitTime = (long) config.get(SidelineSpoutConfig.CONSUMER_STATE_FLUSH_INTERVAL_MS) * 4;

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

        // Produce 3 records into kafka
        final int expectedOriginalRecordCount = 3;
        List<ProducerRecord<byte[], byte[]>> producedRecords = produceRecords(expectedOriginalRecordCount);

        // Wait for our 'firehose' spout instance should pull these 3 records in when we call nextTuple().
        // Consuming from kafka is an async process de-coupled from the call to nextTuple().  Because of this it could
        // take several calls to nextTuple() before the messages are pulled in from kafka behind the scenes and available
        // to be emitted.
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> {
            spout.nextTuple();
            return spoutOutputCollector.getEmissions().size();
        }, equalTo(expectedOriginalRecordCount));

        // Grab out the emissions so we can validate them.
        List<SpoutEmission> spoutEmissions = spoutOutputCollector.getEmissions();

        // Just a sanity check, this should be 3
        assertEquals(expectedOriginalRecordCount, spoutOutputCollector.getEmissions().size());

        // Define some expected values for validation
        final String expectedFirehoseConsumerId = consumerIdPrefix + "firehose";
        int expectedMessageOffset = 0;

        // Loop over what we produced into kafka
        Iterator<SpoutEmission> emissionIterator = spoutEmissions.iterator();
        for (ProducerRecord<byte[], byte[]> producedRecord: producedRecords) {
            // Sanity check
            assertTrue("Should have more emissions", emissionIterator.hasNext());

            // Now find its corresponding tuple
            final SpoutEmission spoutEmission = emissionIterator.next();

            // validate it
            validateEmission(producedRecord, spoutEmission, expectedFirehoseConsumerId, expectedMessageOffset, expectedStreamId);

            // Increment expected messageoffset for next iteration thru this loop.
            expectedMessageOffset++;
        }

        // Lets ack our tuples, this should commit offsets 0 -> 2. (0,1,2)
        ackTuples(spout, spoutEmissions);

        // Now reset the output collector
        spoutOutputCollector.reset();

        // Sanity test, should be 0 again
        assertEquals(0, spoutOutputCollector.getEmissions().size());

        // Sanity test, we should have a single VirtualSpout instance at this point, the fire hose instance
        assertEquals("Should have a single VirtualSpout instance", 1, spout.getCoordinator().getTotalSpouts());

        // Create a static message filter, this allows us to easily start filtering messages.
        // It should filter ALL messages
        final StaticMessageFilter staticMessageFilter = new StaticMessageFilter(true);

        final SidelineRequest request = new SidelineRequest(staticMessageFilter);

        // Send a new start request with our filter.
        // This means that our starting offset for the sideline'd data should start at offset 3 (we acked offsets 0, 1, 2)
        staticTrigger.sendStartRequest(request);

        // Produce another 3 records into kafka.
        producedRecords = produceRecords(expectedOriginalRecordCount);

        // We basically want the time that would normally pass before we check that there are no new tuples
        // Call next tuple, it should NOT receive any tuples because
        // all tuples are filtered.
        Thread.sleep(waitTime);
        for (int x=0; x<expectedOriginalRecordCount; x++) {
            spout.nextTuple();
        }

        // We should NOT have gotten any tuples emitted, because they were filtered
        assertTrue("Should be empty", spoutOutputCollector.getEmissions().isEmpty());
        assertEquals("Should contain no records", 0, spoutOutputCollector.getEmissions().size());

        // Send a stop sideline request
        staticTrigger.sendStopRequest(request);

        // We need to wait a bit for the sideline spout instance to spin up and start consuming
        // Call next tuple, it should get a tuple from our sidelined spout instance.
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> {
            spout.nextTuple();
            logger.info("Found emissions {}", spoutOutputCollector.getEmissions());
            return spoutOutputCollector.getEmissions().size();
        }, equalTo(expectedOriginalRecordCount));

        // Call next tuple a few more times to be safe nothing else comes in.
        for (int x=0; x<expectedOriginalRecordCount; x++) {
            spout.nextTuple();
        }

        // We should validate these emissions
        spoutEmissions = spoutOutputCollector.getEmissions();
        assertEquals("Should have 3 entries", 3, spoutEmissions.size());

        // Loop over what we produced into kafka
        // These offsets should be 3,4,5 since its the 2nd batch of 3 we produced, and our last
        // completed offset was 2, so we should start at the next one.
        emissionIterator = spoutEmissions.iterator();
        for (ProducerRecord<byte[], byte[]> producedRecord: producedRecords) {
            // Sanity check
            assertTrue("Should have more emissions", emissionIterator.hasNext());

            // Now find its corresponding tuple
            final SpoutEmission spoutEmission = emissionIterator.next();

            // validate it
            // TODO - Need to get the SidelineConsumerId Here.
            validateEmission(producedRecord, spoutEmission, "ThisShouldBeSidelineConsumerIdDunnoHowToGetThatOut", expectedMessageOffset, expectedStreamId);

            // Increment expected messageoffset for next iteration thru this loop.
            expectedMessageOffset++;
        }

        // Validate that virtualsideline spouts are NOT closed out, but still waiting for unacked tuples.
        // We should have 2 instances at this point, the firehose, and 1 sidelining instance.
        assertEquals("We should have 2 virtual spouts running", 2, spout.getCoordinator().getTotalSpouts());

        // Lets ack our messages.
        ackTuples(spout, spoutEmissions);

        // Reset our mock SpoutOutputCollector
        spoutOutputCollector.reset();

        // Validate that virtualsideline spout instance closes out once finished acking all processed tuples.
        // We need to wait for the monitor thread to run to clean it up.
        await()
            .atMost(waitTime, TimeUnit.MILLISECONDS)
            .until(() -> {
                return spout.getCoordinator().getTotalSpouts();
            }, equalTo(1));
        assertEquals("We should have only 1 virtual spouts running", 1, spout.getCoordinator().getTotalSpouts());

        // Produce some more records, verify they come in the firehose.
        producedRecords = produceRecords(expectedOriginalRecordCount);

        // Wait up to 5 seconds, our 'firehose' spout instance should pull these 3 records in when we call nextTuple().
        await().atMost(waitTime, TimeUnit.MILLISECONDS).until(() -> {
            spout.nextTuple();
            return spoutOutputCollector.getEmissions().size();
        }, equalTo(expectedOriginalRecordCount));

        // Grab out the emissions so we can validate them.
        spoutEmissions = spoutOutputCollector.getEmissions();

        // Just a sanity check, this should be 3
        assertEquals(expectedOriginalRecordCount, spoutOutputCollector.getEmissions().size());

        // Loop over what we produced into kafka
        emissionIterator = spoutEmissions.iterator();
        for (ProducerRecord<byte[], byte[]> producedRecord: producedRecords) {
            // Sanity check
            assertTrue("Should have more emissions", emissionIterator.hasNext());

            // Now find its corresponding tuple
            final SpoutEmission spoutEmission = emissionIterator.next();

            // validate it
            validateEmission(producedRecord, spoutEmission, expectedFirehoseConsumerId, expectedMessageOffset, expectedStreamId);

            // Increment expected messageoffset for next iteration thru this loop.
            expectedMessageOffset++;
        }

        // Close out
        spout.close();
    }

    // Helper method
    private void validateEmission(final ProducerRecord<byte[], byte[]> sourceProducerRecord, final SpoutEmission spoutEmission, final String expectedConsumerId, final int expectedMessageOffset, final String expectedOutputStreamId) {
        // These values may change
        final Integer expectedTaskId = null;
        final int expectedPartitionId = 0;

        // Now find its corresponding tuple
        assertNotNull("Not null sanity check", spoutEmission);

        // Validate Message Id
        assertNotNull("Should have non-null messageId", spoutEmission.getMessageId());
        assertTrue("Should be instance of TupleMessageId", spoutEmission.getMessageId() instanceof TupleMessageId);
        final TupleMessageId messageId = (TupleMessageId) spoutEmission.getMessageId();
        assertEquals("Expected Topic Name in MessageId", topicName, messageId.getTopic());
        assertEquals("Expected PartitionId found", expectedPartitionId, messageId.getPartition());
        assertEquals("Expected MessageOffset found", expectedMessageOffset, messageId.getOffset());

        // TODO - how do we get the consumer id for sideline spouts since they're autogenerated?
        //assertEquals("Expected Source Consumer Id", expectedConsumerId, messageId.getSrcVirtualSpoutId());

        // Validate Tuple Contents
        List<Object> tupleValues = spoutEmission.getTuple();
        assertNotNull("Tuple Values should not be null", tupleValues);
        assertFalse("Tuple Values should not be empty", tupleValues.isEmpty());

        // For now the values in the tuple should be 'key' and 'value', this may change.
        assertEquals("Should have 2 values in the tuple", 2, tupleValues.size());
        assertEquals("Found expected 'key' value", new String(sourceProducerRecord.key(), Charsets.UTF_8), tupleValues.get(0));
        assertEquals("Found expected 'value' value", new String(sourceProducerRecord.value(), Charsets.UTF_8), tupleValues.get(1));

        // Validate Emit Parameters
        assertEquals("Got expected streamId", expectedOutputStreamId, spoutEmission.getStreamId());
        assertEquals("Got expected taskId", expectedTaskId, spoutEmission.getTaskId());
    }

    /**
     * Utility method to ack tuples on a spout.  This will wait for the underlying VirtualSpout instance
     * to actually ack them before returning.
     *
     * @param spout - the Spout instance to ack tuples on.
     * @param spoutEmissions - The SpoutEmissions we want to ack.
     */
    private void ackTuples(final SidelineSpout spout, List<SpoutEmission> spoutEmissions) {
        // Ack each one.
        for (SpoutEmission emission: spoutEmissions) {
            spout.ack(emission.getMessageId());
        }
        // Acking tuples is an async process, so we need to make sure they get picked up
        // and processed before continuing.
        await()
            .atMost(3, TimeUnit.SECONDS)
            .until(() -> {
                // Wait for our tuples to get popped off the acked queue.
                Map<String, Queue<TupleMessageId>> queueMap = spout.getCoordinator().getAckedTuplesQueue();
                for (String key : queueMap.keySet()) {
                    // If any queue has entries, return false
                    if (!queueMap.get(key).isEmpty()) {
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
        // Fail each one.
        for (SpoutEmission emission: spoutEmissions) {
            spout.fail(emission.getMessageId());
        }
        // Failing tuples is an async process, so we need to make sure they get picked up
        // and processed before continuing.
        await()
            .atMost(3, TimeUnit.SECONDS)
            .until(() -> {
                // Wait for our tuples to get popped off the acked queue.
                Map<String, Queue<TupleMessageId>> queueMap = spout.getCoordinator().getFailedTuplesQueue();
                for (String key : queueMap.keySet()) {
                    // If any queue has entries, return false
                    if (!queueMap.get(key).isEmpty()) {
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
    public void testDelcareOutputFields_without_stream() {
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

    private List<ProducerRecord<byte[], byte[]>> produceRecords(int numberOfRecords) {
        List<ProducerRecord<byte[], byte[]>> producedRecords = Lists.newArrayList();

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
            producer.send(record);
        }
        // Publish to the topic and close.
        producer.flush();
        logger.info("Produce completed");
        producer.close();

        return producedRecords;
    }

    private Map<String, Object> getDefaultConfig(final String consumerIdPrefix, final String configuredStreamId) {
        final Map<String, Object> config = Maps.newHashMap();
        config.put(SidelineSpoutConfig.DESERIALIZER_CLASS, "com.salesforce.storm.spout.sideline.kafka.deserializer.Utf8StringDeserializer");
        config.put(SidelineSpoutConfig.FAILED_MSG_RETRY_MANAGER_CLASS, "com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers.NoRetryFailedMsgRetryManager");
        config.put(SidelineSpoutConfig.KAFKA_TOPIC, topicName);
        config.put(SidelineSpoutConfig.CONSUMER_ID_PREFIX, consumerIdPrefix);
        config.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort()));
        config.put(SidelineSpoutConfig.PERSISTENCE_ZK_SERVERS, Lists.newArrayList("localhost:" + kafkaTestServer.getZkServer().getPort()));
        config.put(SidelineSpoutConfig.PERSISTENCE_ZK_ROOT, "/sideline-spout-test");
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
}