package com.salesforce.storm.spout.sideline;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.filter.StaticMessageFilter;
import com.salesforce.storm.spout.sideline.kafka.SidelineConsumerTest;
import com.salesforce.storm.spout.sideline.kafka.KafkaTestServer;
import com.salesforce.storm.spout.sideline.mocks.MockTopologyContext;
import com.salesforce.storm.spout.sideline.mocks.output.MockSpoutOutputCollector;
import com.salesforce.storm.spout.sideline.trigger.StartRequest;
import com.salesforce.storm.spout.sideline.trigger.StaticTrigger;
import com.salesforce.storm.spout.sideline.trigger.StopRequest;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.shade.com.google.common.base.Charsets;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.OutputFieldsGetter;
import org.apache.storm.utils.Utils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

/**
 *
 */
public class SidelineSpoutTest {

    private static final Logger logger = LoggerFactory.getLogger(SidelineSpoutTest.class);
    private KafkaTestServer kafkaTestServer;
    private String topicName;

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
        topicName = SidelineConsumerTest.class.getSimpleName() + DateTime.now().getMillis();

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
     * Simple end-2-end test.  Likely to change drastically as we make further progress.
     */
    @Test
    public void doTest() throws InterruptedException {
        // Define how many tuples we should push into the topic, and then consume back out.
        final int emitTupleCount = 10;

        // Mock Config
        final Map<String, Object> config = Maps.newHashMap();
        config.put(SidelineSpoutConfig.KAFKA_TOPIC, topicName);
        config.put(SidelineSpoutConfig.CONSUMER_ID_PREFIX, "SidelineSpout-");
        config.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort()));

        // Some mock stuff to get going
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        // Create spout and call open
        final SidelineSpout spout = new SidelineSpout();
        spout.open(config, topologyContext, spoutOutputCollector);

        // Call next tuple, topic is empty, so should get nothing.
        spout.nextTuple();
        assertTrue("SpoutOutputCollector should have no emissions", spoutOutputCollector.getEmissions().isEmpty());

        // Lets produce some data into the topic
        produceRecords(emitTupleCount);

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
        logger.info("Emissions: {}", spoutOutputCollector.getEmissions());

        // Call next tuple a few more times
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
     * Simple end-2-end test.  Likely to change drastically as we make further progress.
     */
    @Test
    public void doTestWithSidelining() throws InterruptedException {
        final Map<String, Object> config = Maps.newHashMap();
        config.put(SidelineSpoutConfig.KAFKA_TOPIC, topicName);
        config.put(SidelineSpoutConfig.CONSUMER_ID_PREFIX, "SidelineSpout-");
        config.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort()));
        final TopologyContext topologyContext = new MockTopologyContext();
        final MockSpoutOutputCollector spoutOutputCollector = new MockSpoutOutputCollector();

        final StaticTrigger staticTrigger = new StaticTrigger();
        final StaticMessageFilter staticMessageFilter = new StaticMessageFilter();

        final SidelineSpout spout = new SidelineSpout();
        spout.setStartingTrigger(staticTrigger);
        spout.setStoppingTrigger(staticTrigger);
        spout.open(config, topologyContext, spoutOutputCollector);

        produceRecords(1);

        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            spout.nextTuple();
            return spoutOutputCollector.getEmissions().size();
        }, equalTo(1));

        // Start sidelining
        staticMessageFilter.setShouldFilter(true);
        staticTrigger.sendStartRequest(
            new StartRequest(
                Lists.newArrayList(
                    staticMessageFilter
                )
            )
        );

        produceRecords(1);

        // TODO: It would be nice to await here, we basically want the time that would normally pass before we check that there are no new tuples
        Thread.sleep(1000);

        spout.nextTuple();

        // Still the same number of emissions as before
        assertEquals(1, spoutOutputCollector.getEmissions().size());

        // Stop sidelining
        staticMessageFilter.setShouldFilter(false);
        staticTrigger.sendStopRequest(
            new StopRequest(
                staticTrigger.getCurrentSidelineIdentifier()
            )
        );

        Thread.sleep(1000);

        spout.nextTuple();

        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            spout.nextTuple();
            return spoutOutputCollector.getEmissions().size();
        }, equalTo(2));

        spout.close();
    }

    @Test
    public void testDelcareOutputFields_without_stream() {
        final MockSpoutOutputCollector outputCollector = new MockSpoutOutputCollector();
        final TopologyContext context = new MockTopologyContext();
        final Map<String,Object> config = new HashMap<>();
        config.put(SidelineSpoutConfig.KAFKA_TOPIC, topicName);
        config.put(SidelineSpoutConfig.CONSUMER_ID_PREFIX, "SidelineSpout-");
        config.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort()));

        final SidelineSpout spout = new SidelineSpout();
        spout.open(config, context, outputCollector);

        final OutputFieldsGetter declarer = new OutputFieldsGetter();

        spout.declareOutputFields(declarer);

        final Map<String, StreamInfo> fieldsDeclaration = declarer.getFieldsDeclaration();

        assertTrue(fieldsDeclaration.containsKey(Utils.DEFAULT_STREAM_ID));
        assertEquals(
            fieldsDeclaration.get(Utils.DEFAULT_STREAM_ID).get_output_fields(),
            Lists.newArrayList("key", "value")
        );

        spout.close();
    }

    @Test
    public void testDelcareOutputFields_with_stream() {
        final String streamId = "foobar";
        final MockSpoutOutputCollector outputCollector = new MockSpoutOutputCollector();
        final TopologyContext context = new MockTopologyContext();
        final Map<String,Object> config = new HashMap<>();
        config.put(SidelineSpoutConfig.KAFKA_TOPIC, topicName);
        config.put(SidelineSpoutConfig.CONSUMER_ID_PREFIX, "SidelineSpout-");
        config.put(SidelineSpoutConfig.KAFKA_BROKERS, Lists.newArrayList("localhost:" + kafkaTestServer.getKafkaServer().serverConfig().advertisedPort()));
        config.put(SidelineSpoutConfig.OUTPUT_STREAM_ID, streamId);

        final SidelineSpout spout = new SidelineSpout();
        spout.open(config, context, outputCollector);

        final OutputFieldsGetter declarer = new OutputFieldsGetter();

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
     * Noop, just doing coverage!
     */
    @Test
    public void testActivate() {
        final SidelineSpout spout = new SidelineSpout();
        spout.activate();
    }

    /**
     * Noop, just doing coverage!
     */
    @Test
    public void testDeactivate() {
        final SidelineSpout spout = new SidelineSpout();
        spout.deactivate();
    }

    private List<ProducerRecord<byte[], byte[]>> produceRecords(int numberOfRecords) {
        List<ProducerRecord<byte[], byte[]>> producedRecords = Lists.newArrayList();

        KafkaProducer producer = kafkaTestServer.getKafkaProducer("org.apache.kafka.common.serialization.ByteArraySerializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        for (int x=0; x<numberOfRecords; x++) {
            // Construct key and value
            long timeStamp = DateTime.now().getMillis();
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
}