package com.salesforce.storm.spout.sideline;

import com.google.common.base.Strings;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.filter.FilterChainStep;
import com.salesforce.storm.spout.sideline.filter.NegatingFilterChainStep;
import com.salesforce.storm.spout.sideline.kafka.VirtualSidelineSpout;
import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers.FailedMsgRetryManager;
import com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers.NoRetryFailedMsgRetryManager;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.persistence.PersistenceManager;
import com.salesforce.storm.spout.sideline.trigger.SidelineIdentifier;
import com.salesforce.storm.spout.sideline.trigger.StartRequest;
import com.salesforce.storm.spout.sideline.trigger.StartingTrigger;
import com.salesforce.storm.spout.sideline.trigger.StopRequest;
import com.salesforce.storm.spout.sideline.trigger.StoppingTrigger;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

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

    /**
     * Manages creating implementation instances.
     */
    private FactoryManager factoryManager;

    /**
     * Stores state about starting/stopping sideline requests.
     */
    private PersistenceManager persistenceManager;

    /**
     * For collecting metrics.
     */
    private transient MetricsRecorder metricsRecorder;

    /**
     * Determines which output stream to emit tuples out.
     * Gets set during open().
     */
    private String outputStreamId = null;

    /**
     * Constructor to create our SidelineSpout.
     * @TODO this method arguments may change to an actual SidelineSpoutConfig object instead of a generic map?
     *
     * @param topologyConfig - Our configuration.
     */
    public SidelineSpout(Map topologyConfig) {
        // Save off config.
        this.topologyConfig = topologyConfig;

        // Create our factory manager, which must be serializable.
        factoryManager = new FactoryManager(this.topologyConfig);
    }

    /**
     * Set a starting trigger on the spout for starting a sideline request
     * @param startingTrigger An impplementation of a starting trigger
     */
    public void setStartingTrigger(StartingTrigger startingTrigger) {
        this.startingTrigger = startingTrigger;
    }

    /**
     * Set a trigger on the spout for stopping a sideline request
     * @param stoppingTrigger An implementation of a stopping trigger
     */
    public void setStoppingTrigger(StoppingTrigger stoppingTrigger) {
        this.stoppingTrigger = stoppingTrigger;
    }

    /**
     * Starts a sideline request
     * @param startRequest A representation of the request that is being started
     */
    public SidelineIdentifier startSidelining(StartRequest startRequest) {
        logger.info("Received START sideline request");

        final SidelineIdentifier id = new SidelineIdentifier();

        // Store the offset that this request was made at, when the sideline stops we will begin processing at
        // this offset
        final ConsumerState startingState = fireHoseSpout.getCurrentState();

        // TODO - Talk to slemon about this.
        // These are the committed offsets, meaning we have successfully processed them
        // So we want to start at the next message in each partition
        for (TopicPartition topicPartition: startingState.getTopicPartitions()) {
            // Increment by 1?  This seems like a hack.
            startingState.setOffset(topicPartition, startingState.getOffsetForTopicAndPartition(topicPartition) + 1);
        }

        // Store in request manager
        persistenceManager.persistSidelineRequestState(id, startingState);

        // Add our new filter steps
        fireHoseSpout.getFilterChain().addSteps(id, startRequest.steps);

        // Call back to the trigger after starting
        // TODO: Revisit this, specifically the payload
        startingTrigger.start(id);

        // Update start count metric
        metricsRecorder.count(getClass(), "start-sideline", 1L);

        return id;
    }

    /**
     * Stops a sideline request.
     * @param stopRequest A representation of the request that is being stopped
     */
    public void stopSidelining(StopRequest stopRequest) {
        if (!fireHoseSpout.getFilterChain().hasSteps(stopRequest.id)) {
            logger.error("Received STOP sideline request, but I don't actually have any filter chain steps for it!");
            return;
        }

        logger.info("Received STOP sideline request");

        List<FilterChainStep> negatedSteps = new ArrayList<>();

        List<FilterChainStep> steps = fireHoseSpout.getFilterChain().removeSteps(stopRequest.id);

        for (FilterChainStep step : steps) {
            negatedSteps.add(new NegatingFilterChainStep(step));
        }

        final FailedMsgRetryManager failedMsgRetryManager = factoryManager.createNewFailedMsgRetryManagerInstance();

        // This is the state that the VirtualSidelineSpout should start with
        final ConsumerState startingState = persistenceManager.retrieveSidelineRequestState(stopRequest.id);

        // This is the state that the VirtualSidelineSpout should end with
        final ConsumerState endingState = fireHoseSpout.getCurrentState();

        logger.info("Starting VirtualSidelineSpout with starting state {}", startingState);
        logger.info("Starting VirtualSidelineSpout with ending state {}", endingState);

        final VirtualSidelineSpout spout = new VirtualSidelineSpout(
            topologyConfig,
            topologyContext,
            factoryManager.createNewDeserializerInstance(),
            failedMsgRetryManager,
            metricsRecorder,
            // Starting offset of the sideline request
            startingState,
            // When the sideline request ends
            endingState
        );
        spout.setConsumerId(fireHoseSpout.getConsumerId() + "_" + stopRequest.id.toString());
        spout.getFilterChain().addSteps(stopRequest.id, negatedSteps);

        coordinator.addSidelineSpout(spout);

        // Callback to teh trigger after stopping
        // TODO: Revisit this, specifically the payload
        stoppingTrigger.stop();

        // Update stop count metric
        metricsRecorder.count(getClass(), "stop-sideline", 1L);
    }

    @Override
    public void open(Map toplogyConfig, TopologyContext context, SpoutOutputCollector collector) {
        // Save references.
        this.topologyConfig = Collections.unmodifiableMap(toplogyConfig);
        this.topologyContext = context;
        this.outputCollector = collector;

        // Initialize Metrics Collection
        this.metricsRecorder = factoryManager.createNewMetricsRecorder();
        metricsRecorder.open(this.topologyConfig, this.topologyContext);

        // Setup our concurrent queue.
        this.queue = new ConcurrentLinkedDeque<>();

        if (startingTrigger != null) {
            startingTrigger.setSidelineSpout(this);
        }

        if (stoppingTrigger != null) {
            stoppingTrigger.setSidelineSpout(this);
        }

        // Grab our ConsumerId prefix from the config
        final String cfgConsumerIdPrefix = (String) getTopologyConfigItem(SidelineSpoutConfig.CONSUMER_ID_PREFIX);
        if (Strings.isNullOrEmpty(cfgConsumerIdPrefix)) {
            throw new IllegalStateException("Missing required configuration: " + SidelineSpoutConfig.CONSUMER_ID_PREFIX);
        }

        // Create and open() persistence manager passing appropriate configuration.
        persistenceManager = factoryManager.createNewPersistenceManagerInstance();
        persistenceManager.open(getTopologyConfig());

        // Create the main spout for the topic, we'll dub it the 'firehose'
        fireHoseSpout = new VirtualSidelineSpout(getTopologyConfig(), getTopologyContext(), factoryManager.createNewDeserializerInstance(), factoryManager.createNewFailedMsgRetryManagerInstance(), metricsRecorder);
        fireHoseSpout.setConsumerId(cfgConsumerIdPrefix);

        // Setting up thread to call nextTuple

        // Fire up thread/instance/class that watches for any sideline consumers that should be running
            // This thread/class will then spawn any additional VirtualSidelineSpout instances that should be running
            // This thread could also maybe manage when it should kill off finished VirtualSideLineSpout instanes?

        // Fire up thread that manages which tuples should get emitted next
            // This thing cycles thru the firehose instance, and any sideline consumer instances
            // and fills up a buffer of what messages should go out next
            // Maybe this instance is a wrapper/container around all of the VirtualSideLineSpout instances?

        coordinator = new SpoutCoordinator(
                // Our main firehose spout instance.
                fireHoseSpout,

                // Our metrics recorder.
                metricsRecorder
        );

        // TODO: Look for any existing sideline requests that haven't finished and add them to the
        //  coordinator

        coordinator.open((KafkaMessage message) -> {
            queue.add(message);
        });

        if (startingTrigger != null) {
            startingTrigger.open(toplogyConfig);
        }

        if (stoppingTrigger != null) {
            stoppingTrigger.open(toplogyConfig);
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
            outputCollector.emit(getOutputStreamId(), kafkaMessage.getValues(), kafkaMessage.getTupleMessageId());

            // Update emit count metric for SidelineSpout
            metricsRecorder.count(getClass(), "emit", 1L);

            // Update emit count metric for VirtualSidelineSpout this tuple originated from
            metricsRecorder.count(VirtualSidelineSpout.class, kafkaMessage.getTupleMessageId().getSrcConsumerId() + ".emit", 1);
        }
    }

    /**
     * Declare the output fields.

     * @param declarer The output field declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // I'm unsure where declareOutputFields() gets called in the spout lifecycle, it may get called
        // prior to open, in which case we need to shuffle some logic around.

        // Handles both explicitly defined and default stream definitions.
        declarer.declareStream(getOutputStreamId(), factoryManager.createNewDeserializerInstance().getOutputFields());
    }

    @Override
    public void close() {
        logger.info("Stopping the coordinator and closing all spouts");

        if (coordinator != null) {
            coordinator.close();
            coordinator = null;
        }

        if (startingTrigger != null) {
            startingTrigger.close();
        }

        if (stoppingTrigger != null) {
            stoppingTrigger.close();
        }
    }

    @Override
    public void activate() {
        logger.info("Activating spout");
    }

    @Override
    public void deactivate() {
        logger.info("Deactivate spout");
    }

    @Override
    public void ack(Object id) {
        // Cast to appropriate object type
        final TupleMessageId tupleMessageId = (TupleMessageId) id;

        // Ack the tuple
        coordinator.ack(tupleMessageId);

        // Update ack count metric
        metricsRecorder.count(getClass(), "ack", 1L);

        // Update ack count metric for VirtualSidelineSpout this tuple originated from
        metricsRecorder.count(VirtualSidelineSpout.class, tupleMessageId.getSrcConsumerId() + ".ack", 1);
    }

    @Override
    public void fail(Object id) {
        // Cast to appropriate object type
        final TupleMessageId tupleMessageId = (TupleMessageId) id;

        // Fail the tuple
        coordinator.fail(tupleMessageId);

        // Update fail count metric
        metricsRecorder.count(getClass(), "fail", 1L);

        // Update ack count metric for VirtualSidelineSpout this tuple originated from
        metricsRecorder.count(VirtualSidelineSpout.class, tupleMessageId.getSrcConsumerId() + ".fail", 1);
    }

    public Map getTopologyConfig() {
        return topologyConfig;
    }

    public Object getTopologyConfigItem(final String key) {
        return getTopologyConfig().get(key);
    }

    public TopologyContext getTopologyContext() {
        return topologyContext;
    }

    /**
     * @return - returns the stream that tuples will be emitted out.
     */
    protected String getOutputStreamId() {
        if (outputStreamId == null) {
            if (topologyConfig == null) {
                throw new IllegalStateException("Missing required configuration!  SidelineSpoutConfig not defined!");
            }
            outputStreamId = (String) getTopologyConfigItem(SidelineSpoutConfig.OUTPUT_STREAM_ID);
            if (Strings.isNullOrEmpty(outputStreamId)) {
                outputStreamId = Utils.DEFAULT_STREAM_ID;
            }
        }
        return outputStreamId;
    }
}
