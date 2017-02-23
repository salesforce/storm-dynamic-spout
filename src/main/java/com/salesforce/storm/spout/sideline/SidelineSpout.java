package com.salesforce.storm.spout.sideline;

import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.filter.FilterChainStep;
import com.salesforce.storm.spout.sideline.filter.NegatingFilterChainStep;
import com.salesforce.storm.spout.sideline.kafka.VirtualSidelineSpout;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Utf8StringDeserializer;
import com.salesforce.storm.spout.sideline.request.InMemoryManager;
import com.salesforce.storm.spout.sideline.request.RequestManager;
import com.salesforce.storm.spout.sideline.trigger.SidelineIdentifier;
import com.salesforce.storm.spout.sideline.trigger.StartRequest;
import com.salesforce.storm.spout.sideline.trigger.StartingTrigger;
import com.salesforce.storm.spout.sideline.trigger.StopRequest;
import com.salesforce.storm.spout.sideline.trigger.StoppingTrigger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;

/**
 * Skeleton implementation for now.
 */
public class SidelineSpout extends BaseRichSpout {
    // Logging
    private static final Logger logger = LoggerFactory.getLogger(SidelineSpout.class);

    // Storm Topology Items
    private Map topologyConfig;
    private SpoutOutputCollector outputCollector;
    private TopologyContext topologyContext;
    private transient ConcurrentLinkedDeque<KafkaMessage> queue;
    private VirtualSidelineSpout fireHoseSpout;
    private SpoutCoordinator coordinator;
    private StartingTrigger startingTrigger;
    private StoppingTrigger stoppingTrigger;

    private RequestManager requestManager = new InMemoryManager();

    /**
     * Set a starting trigger on the spout for starting a sideline request
     * @param startingTrigger An impplementation of a starting trigger
     */
    public void setStartingTrigger(StartingTrigger startingTrigger) {
        this.startingTrigger = startingTrigger;
        this.startingTrigger.setSidelineSpout(this);
    }

    /**
     * Set a trigger on the spout for stopping a sideline request
     * @param stoppingTrigger An implementation of a stopping trigger
     */
    public void setStoppingTrigger(StoppingTrigger stoppingTrigger) {
        this.stoppingTrigger = stoppingTrigger;
        this.stoppingTrigger.setSidelineSpout(this);
    }

    /**
     * Starts a sideline request
     * @param startRequest A representation of the request that is being started
     */
    public void startSidelining(StartRequest startRequest) {
        final SidelineIdentifier id = new SidelineIdentifier();

        // Store the offset that this request was made at, when the sideline stops we will begin processing at
        // this offset
        requestManager.set(id, fireHoseSpout.getCurrentState());

        fireHoseSpout.getFilterChain().addSteps(id, startRequest.steps);

        startingTrigger.start(id);
    }

    /**
     * Stops a sideline request
     * @param stopRequest A representation of the request that is being stopped
     */
    public void stopSidelining(StopRequest stopRequest) {
        if (!fireHoseSpout.getFilterChain().hasSteps(stopRequest.id)) {
            logger.error("Received STOP sideline request, but I don't actually have any filter chain steps for it!");
            return;
        }

        List<FilterChainStep> negatedSteps = new ArrayList<>();

        List<FilterChainStep> steps = fireHoseSpout.getFilterChain().removeSteps(stopRequest.id);

        for (FilterChainStep step : steps) {
            negatedSteps.add(new NegatingFilterChainStep(step));
        }

        final VirtualSidelineSpout spout = new VirtualSidelineSpout(
            topologyConfig,
            topologyContext,
            new Utf8StringDeserializer(),
            // Starting offset of the sideline request
            requestManager.get(stopRequest.id),
            // When the sideline request ends
            fireHoseSpout.getCurrentState()
        );
        spout.setConsumerId(fireHoseSpout.getConsumerId() + "_" + stopRequest.id.toString());
        spout.getFilterChain().addSteps(stopRequest.id, negatedSteps);

        coordinator.addSidelineSpout(spout);

        // If any cleanup is necessary it could be handled here
        stoppingTrigger.stop();
    }


    @Override
    public void open(Map toplogyConfig, TopologyContext context, SpoutOutputCollector collector) {
        // Save references.
        this.topologyConfig = Collections.unmodifiableMap(toplogyConfig);
        this.topologyContext = context;
        this.outputCollector = collector;

        // Setup our concurrent queue.
        this.queue = new ConcurrentLinkedDeque<>();

        // @TODO - Parse config, for now use these values
        final String cfgConsumerIdPrefix = (String) getTopologyConfigItem(SidelineSpoutConfig.CONSUMER_ID_PREFIX);

        // Construct an instance of VirtualSidelineSpout that reads from the topic
        // This is the main firehose instance.
        // @TODO this probably needs to be started within some container and run async from the main
        // spout thread.
        logger.info("Starting FireHoseSpout...");
        fireHoseSpout = new VirtualSidelineSpout(getTopologyConfig(), getTopologyContext(), new Utf8StringDeserializer());
        fireHoseSpout.setConsumerId(cfgConsumerIdPrefix + "firehose");
        logger.info("Finished Starting FireHoseSpout...");

        // Setting up thread to call nextTuple

        // Fire up thread/instance/class that watches for any sideline consumers that should be running
            // This thread/class will then spawn any additional VirtualSidelineSpout instances that should be running
            // This thread could also maybe manage when it should kill off finished VirtualSideLineSpout instanes?

        // Fire up thread that manages which tuples should get emitted next
            // This thing cycles thru the firehose instance, and any sideline consumer instances
            // and fills up a buffer of what messages should go out next
            // Maybe this instance is a wrapper/container around all of the VirtualSideLineSpout instances?

        coordinator = new SpoutCoordinator(
            fireHoseSpout
        );

        // TODO: Look for any existing sideline requests that haven't finished and add them to the
        //  coordinator
        final int totalSpouts = 1; // Will be updated based upon ^^

        final CountDownLatch openSignal = new CountDownLatch(totalSpouts);

        coordinator.start(openSignal, (KafkaMessage message) -> {
            queue.add(message);
        });

        try {
            openSignal.await();
        } catch (InterruptedException ex) {
            logger.error("Exception while waiting for the coordinator to open it's spouts {}", ex);
        }
    }

    @Override
    public void nextTuple() {
        // Talk to thread that manages what tuples should be emitted next to get the next tuple
        // Ensure that the tuple's Id identifies which spout instance it came from
        // so we can trace the tuple id back to the spout later.
        // Emit tuple.
        if (!queue.isEmpty()) {
            final KafkaMessage kafkaMessage = queue.removeFirst();

            // Debug logging
            logger.info("Emitting MsgId[{}] - {}", kafkaMessage.getTupleMessageId(), kafkaMessage.getValues());

            // Dump to output collector.
            outputCollector.emit(kafkaMessage.getValues(), kafkaMessage.getTupleMessageId());
        }
    }

    /**
     * @TODO - this is how KafkaStorm does it:
     * https://github.com/apache/storm/blob/master/external/storm-kafka/src/jvm/org/apache/storm/kafka/KafkaSpout.java#L183
     * 
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Need to declare output fields somehow.
    }

    @Override
    public void close() {
        // Shutdown shit
        logger.info("Calling close on FireHoseSpout");
        coordinator.stop();
    }

    @Override
    public void activate() {
        // Unpause stuff
        logger.info("Calling activate on FireHoseSpout");
    }

    @Override
    public void deactivate() {
        // Pause stuff
        logger.info("Calling deactivate on FireHoseSpout");
    }

    @Override
    public void ack(Object id) {
        // Cast to appropriate object type
        final TupleMessageId tupleMessageId = (TupleMessageId) id;

        coordinator.ack(tupleMessageId);
    }

    @Override
    public void fail(Object id) {
        // Cast to appropriate object type
        final TupleMessageId tupleMessageId = (TupleMessageId) id;

        coordinator.fail(tupleMessageId);
    }

    public Map getTopologyConfig() {
        return topologyConfig;
    }

    public Object getTopologyConfigItem(final String key) {
        return getTopologyConfig().get(key);
    }

    public SpoutOutputCollector getOutputCollector() {
        return outputCollector;
    }

    public TopologyContext getTopologyContext() {
        return topologyContext;
    }
}
