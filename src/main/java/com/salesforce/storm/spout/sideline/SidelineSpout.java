package com.salesforce.storm.spout.sideline;

import com.google.common.base.Strings;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.filter.FilterChainStep;
import com.salesforce.storm.spout.sideline.filter.NegatingFilterChainStep;
import com.salesforce.storm.spout.sideline.kafka.VirtualSidelineSpout;
import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers.FailedMsgRetryManager;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.persistence.PersistenceManager;
import com.salesforce.storm.spout.sideline.persistence.SidelinePayload;
import com.salesforce.storm.spout.sideline.trigger.SidelineIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import com.salesforce.storm.spout.sideline.trigger.StartingTrigger;
import com.salesforce.storm.spout.sideline.trigger.StoppingTrigger;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Skeleton implementation for now.
 */
public class SidelineSpout extends BaseRichSpout {
    // Logging
    private static final Logger logger = LoggerFactory.getLogger(SidelineSpout.class);

    private static final int QUEUE_MAX_SIZE = 10000;

    // Storm Topology Items
    private Map topologyConfig;
    private SpoutOutputCollector outputCollector;
    private TopologyContext topologyContext;
    private transient BlockingQueue<KafkaMessage> queue;
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
     * @param sidelineRequest A representation of the request that is being started
     */
    public SidelineIdentifier startSidelining(SidelineRequest sidelineRequest) {
        logger.info("Received START sideline request");

        final SidelineIdentifier id = new SidelineIdentifier();

        // Store the offset that this request was made at, when the sideline stops we will begin processing at
        // this offset
        final ConsumerState startingState = fireHoseSpout.getCurrentState();

        // Store in request manager
        persistenceManager.persistSidelineRequestState(
            SidelineType.START,
            id,
            sidelineRequest,
            startingState,
            null
        );

        // Add our new filter steps
        fireHoseSpout.getFilterChain().addSteps(id, sidelineRequest.steps);

        // Update start count metric
        metricsRecorder.count(getClass(), "start-sideline", 1L);

        return id;
    }

    /**
     * Stops a sideline request.
     * @param sidelineRequest A representation of the request that is being stopped
     */
    public void stopSidelining(SidelineRequest sidelineRequest) {
        final SidelineIdentifier id = fireHoseSpout.getFilterChain().findSteps(sidelineRequest.steps);

        if (id == null) {
            logger.error(
                "Received STOP sideline request, but I don't actually have any filter chain steps for it! Make sure you check that your filter implements an equals() method. {} {}",
                sidelineRequest.steps,
                fireHoseSpout.getFilterChain().getSteps()
            );
            return;
        }

        logger.info("Received STOP sideline request");

        List<FilterChainStep> negatedSteps = new ArrayList<>();

        List<FilterChainStep> steps = fireHoseSpout.getFilterChain().removeSteps(id);

        for (FilterChainStep step : steps) {
            negatedSteps.add(new NegatingFilterChainStep(step));
        }

        final FailedMsgRetryManager failedMsgRetryManager = factoryManager.createNewFailedMsgRetryManagerInstance();

        // This is the state that the VirtualSidelineSpout should start with
        final ConsumerState startingState = persistenceManager.retrieveSidelineRequest(id).startingState;

        // This is the state that the VirtualSidelineSpout should end with
        final ConsumerState endingState = fireHoseSpout.getCurrentState();

        persistenceManager.persistSidelineRequestState(
            SidelineType.STOP,
            id,
            new SidelineRequest(steps), // Persist a new request with the negated steps
            startingState,
            endingState
        );

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
        spout.setConsumerId(fireHoseSpout.getConsumerId() + "_" + id.toString());
        spout.getFilterChain().addSteps(id, negatedSteps);

        coordinator.addSidelineSpout(spout);

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
        this.queue = new LinkedBlockingQueue<>(QUEUE_MAX_SIZE);

        if (startingTrigger != null) {
            startingTrigger.setSidelineSpout(new SpoutTriggerProxy(this));
        }

        if (stoppingTrigger != null) {
            stoppingTrigger.setSidelineSpout(new SpoutTriggerProxy(this));
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

        coordinator.open(queue);

        // TODO: We should build the full payload here rather than individual requests later on
        final List<SidelineIdentifier> existingRequestIds = persistenceManager.listSidelineRequests();

        for (SidelineIdentifier id : existingRequestIds) {
            final SidelinePayload payload = persistenceManager.retrieveSidelineRequest(id);

            // Resuming a start request means we apply the previous filter chain to the fire hose
            if (payload.type.equals(SidelineType.START)) {
                logger.info("Resuming START sideline {} {}", payload.id, payload.request.steps);

                fireHoseSpout.getFilterChain().addSteps(
                    payload.id,
                    payload.request.steps
                );
            }

            // Resuming a stopped request means we spin up a new sideline spout
            if (payload.type.equals(SidelineType.STOP)) {
                logger.info("Resuming STOP sideline {} {}", payload.id, payload.request.steps);

                final VirtualSidelineSpout spout = new VirtualSidelineSpout(
                    topologyConfig,
                    topologyContext,
                    factoryManager.createNewDeserializerInstance(),
                    factoryManager.createNewFailedMsgRetryManagerInstance(),
                    metricsRecorder,
                    payload.startingState,
                    payload.endingState
                );

                // Add the request's filter steps
                spout.getFilterChain().addSteps(payload.id, payload.request.steps);

                // Now pass the new "resumed" spout over to the coordinator to open and run
                coordinator.addSidelineSpout(spout);
            }
        }

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
        final KafkaMessage kafkaMessage = queue.poll();

        if (kafkaMessage == null) {
            return;
        }

        // Debug logging
        logger.info("Emitting MsgId[{}] - {}", kafkaMessage.getTupleMessageId(), kafkaMessage.getValues());

        // Dump to output collector.
        outputCollector.emit(getOutputStreamId(), kafkaMessage.getValues(), kafkaMessage.getTupleMessageId());

        // Update emit count metric for SidelineSpout
        metricsRecorder.count(getClass(), "emit", 1L);

        // Update emit count metric for VirtualSidelineSpout this tuple originated from
        metricsRecorder.count(VirtualSidelineSpout.class, kafkaMessage.getTupleMessageId().getSrcConsumerId() + ".emit", 1);
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
