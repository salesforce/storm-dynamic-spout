package com.salesforce.storm.spout.sideline;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.filter.FilterChainStep;
import com.salesforce.storm.spout.sideline.filter.NegatingFilterChainStep;
import com.salesforce.storm.spout.sideline.kafka.ConsumerState;
import com.salesforce.storm.spout.sideline.kafka.VirtualSpout;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.persistence.SidelinePayload;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import com.salesforce.storm.spout.sideline.trigger.StartingTrigger;
import com.salesforce.storm.spout.sideline.trigger.StoppingTrigger;
import com.salesforce.storm.spout.sideline.tupleBuffer.TupleBuffer;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Spout instance.
 */
public class SidelineSpout extends BaseRichSpout {
    // Logging
    private static final Logger logger = LoggerFactory.getLogger(SidelineSpout.class);

    /**
     * The Topology configuration map.
     */
    private Map topologyConfig;

    /**
     * The Spout configuration map
     */
    private Map spoutConfig;

    /**
     * Spout's output collector, for emitting tuples out into the topology.
     */
    private SpoutOutputCollector outputCollector;

    /**
     * The Topology Context object.
     */
    private TopologyContext topologyContext;

    /**
     * Our internal Coordinator.  This manages all Virtual Spouts as well
     * as handles routing emitted, acked, and failed tuples between this SidelineSpout instance
     * and the appropriate Virtual Spouts.
     */
    private SpoutCoordinator coordinator;

    /**
     * Starting Trigger
     *
     * This is an instance that is responsible for telling the sideline spout when to begin sidelining.
     */
    private StartingTrigger startingTrigger;

    /**
     * Stopping Trigger
     *
     * This is an instance is responsible for telling the sideline spout when to stop sidelining
     */
    private StoppingTrigger stoppingTrigger;

    /**
     * This is our main Virtual Spout instance which consumes from the configured topic.
     * TODO: Do we need access to this here?  Could this be moved into the Coordinator?
     */
    private VirtualSpout fireHoseSpout;

    /**
     * Manages creating implementation instances.
     */
    private final FactoryManager factoryManager;

    /**
     * Stores state about starting/stopping sideline requests.
     */
    private PersistenceAdapter persistenceAdapter;

    /**
     * For collecting metrics.
     */
    private transient MetricsRecorder metricsRecorder;
    private transient Map<String, Long> emitCountMetrics;
    private long emitCounter = 0L;

    /**
     * Determines which output stream to emit tuples out.
     * Gets set during open().
     */
    private String outputStreamId = null;

    /**
     * Constructor to create our SidelineSpout.
     * @TODO this method arguments may change to an actual SidelineSpoutConfig object instead of a generic map?
     *
     * @param spoutConfig - Our configuration.
     */
    public SidelineSpout(Map spoutConfig) {
        // Save off config, injecting appropriate default values for anything not explicitly configured.
        this.spoutConfig = Collections.unmodifiableMap(SidelineSpoutConfig.setDefaults(spoutConfig));

        // Create our factory manager, which must be serializable.
        factoryManager = new FactoryManager(getSpoutConfig());
    }

    /**
     * Set a starting trigger on the spout for starting a sideline request.
     * @param startingTrigger An implementation of a starting trigger
     */
    public void setStartingTrigger(StartingTrigger startingTrigger) {
        this.startingTrigger = startingTrigger;
    }

    /**
     * Set a trigger on the spout for stopping a sideline request.
     * @param stoppingTrigger An implementation of a stopping trigger
     */
    public void setStoppingTrigger(StoppingTrigger stoppingTrigger) {
        this.stoppingTrigger = stoppingTrigger;
    }

    /**
     * Starts a sideline request.
     * @param sidelineRequest A representation of the request that is being started
     */
    public SidelineRequestIdentifier startSidelining(SidelineRequest sidelineRequest) {
        logger.info("Received START sideline request");

        // Store the offset that this request was made at, when the sideline stops we will begin processing at
        // this offset
        final ConsumerState startingState = fireHoseSpout.getCurrentState();

        for (final TopicPartition topicPartition : startingState.getTopicPartitions()) {
            // Store in request manager
            getPersistenceAdapter().persistSidelineRequestState(
                SidelineType.START,
                sidelineRequest.id, // TODO: Now that this is in the request, we should change the persistence adapter
                sidelineRequest,
                topicPartition.partition(),
                startingState.getOffsetForTopicAndPartition(topicPartition),
                null
            );
        }

        // Add our new filter steps
        fireHoseSpout.getFilterChain().addStep(sidelineRequest.id, sidelineRequest.step);

        // Update start count metric
        getMetricsRecorder().count(getClass(), "start-sideline", 1L);

        return sidelineRequest.id;
    }

    /**
     * Stops a sideline request.
     * @param sidelineRequest A representation of the request that is being stopped
     */
    public void stopSidelining(SidelineRequest sidelineRequest) {
        final SidelineRequestIdentifier id = fireHoseSpout.getFilterChain().findStep(sidelineRequest.step);

        if (id == null) {
            logger.error(
                "Received STOP sideline request, but I don't actually have any filter chain steps for it! Make sure you check that your filter implements an equals() method. {} {}",
                sidelineRequest.step,
                fireHoseSpout.getFilterChain().getSteps()
            );
            return;
        }

        logger.info("Received STOP sideline request");

        // Remove the steps associated with this sideline request
        final FilterChainStep step = fireHoseSpout.getFilterChain().removeSteps(id);

        // This is the state that the VirtualSidelineSpout should end with
        final ConsumerState endingState = fireHoseSpout.getCurrentState();

        // We'll construct a consumer state from the various partition data stored for this sideline request
        final ConsumerState.ConsumerStateBuilder startingStateBuilder = ConsumerState.builder();

        // We are looping over the current partitions for the firehose, functionally this is the collection of partitions
        // assigned to this particular sideline spout instance
        for (final TopicPartition topicPartition : endingState.getTopicPartitions()) {
            // This is the state that the VirtualSidelineSpout should start with
            final SidelinePayload sidelinePayload = getPersistenceAdapter().retrieveSidelineRequest(id, topicPartition.partition());

            // Add this partition to the starting consumer state
            startingStateBuilder.withPartition(topicPartition, sidelinePayload.startingOffset);

            // Persist the side line request state with the new negated version of the steps.
            persistenceAdapter.persistSidelineRequestState(
                SidelineType.STOP,
                id,
                new SidelineRequest(step), // Persist the non-negated steps, we'll always apply negation at runtime
                topicPartition.partition(),
                sidelinePayload.startingOffset,
                endingState.getOffsetForTopicAndPartition(topicPartition)
            );
        }

        // Build our starting state, this is a map of partition and offset
        final ConsumerState startingState = startingStateBuilder.build();

        openVirtualSpout(
            id,
            step,
            startingState,
            endingState
        );

        // Update stop count metric
        getMetricsRecorder().count(getClass(), "stop-sideline", 1L);
    }

    /**
     * Open is called once the SidelineSpout instance has been deployed to the Storm cluster
     * and is ready to get to work.
     *
     * @param topologyConfig - The Storm Topology configuration.
     * @param topologyContext - The Storm Topology context.
     * @param spoutOutputCollector - The output collector to emit tuples via.
     */
    @Override
    public void open(Map topologyConfig, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        // Save references.
        this.topologyConfig = topologyConfig;
        this.topologyContext = topologyContext;
        this.outputCollector = spoutOutputCollector;

        // Ensure a consumer id prefix has been correctly set.
        if (Strings.isNullOrEmpty((String) getSpoutConfigItem(SidelineSpoutConfig.CONSUMER_ID_PREFIX))) {
            throw new IllegalStateException("Missing required configuration: " + SidelineSpoutConfig.CONSUMER_ID_PREFIX);
        }

        // Initialize Metrics Collection
        metricsRecorder = getFactoryManager().createNewMetricsRecorder();
        getMetricsRecorder().open(getSpoutConfig(), getTopologyContext());

        // If we have a starting trigger (technically they're optional but if you don't have one why are you using this spout), set the spout proxy on it
        if (startingTrigger != null) {
            startingTrigger.setSidelineSpout(new SpoutTriggerProxy(this));
        }

        // If we have a stopping trigger (technically they're optional but if you don't have one why are you using this spout), set the spout proxy on it
       if (stoppingTrigger != null) {
            stoppingTrigger.setSidelineSpout(new SpoutTriggerProxy(this));
        }

        // Create and open() persistence manager passing appropriate configuration.
        persistenceAdapter = getFactoryManager().createNewPersistenceAdapterInstance();
        getPersistenceAdapter().open(getSpoutConfig());

        // Create the main spout for the topic, we'll dub it the 'firehose'
        fireHoseSpout = new VirtualSpout(
            getSpoutConfig(),
            getTopologyContext(),
            getFactoryManager(),
            getMetricsRecorder()
        );
        fireHoseSpout.setVirtualSpoutId(generateVirtualSpoutId("main"));

        // Create TupleBuffer
        final TupleBuffer tupleBuffer = getFactoryManager().createNewTupleBufferInstance();
        tupleBuffer.open(getSpoutConfig());

        // Create Spout Coordinator.
        coordinator = new SpoutCoordinator(
            // Our main firehose spout instance.
            fireHoseSpout,

            // Our metrics recorder.
            getMetricsRecorder(),

            // Our TupleBuffer/Queue Implementation.
            tupleBuffer
        );

        // Call open on coordinator.
        getCoordinator().open(getSpoutConfig());

        final ConsumerState currentState = fireHoseSpout.getCurrentState();

        final List<SidelineRequestIdentifier> existingRequestIds = getPersistenceAdapter().listSidelineRequests();
        logger.info("Found {} existing sideline requests that need to be resumed", existingRequestIds.size());

        for (SidelineRequestIdentifier id : existingRequestIds) {
            final ConsumerState.ConsumerStateBuilder startingStateBuilder = ConsumerState.builder();
            final ConsumerState.ConsumerStateBuilder endingStateStateBuilder = ConsumerState.builder();

            SidelinePayload payload = null;

            for (final TopicPartition topicPartition : currentState.getTopicPartitions()) {
                payload = getPersistenceAdapter().retrieveSidelineRequest(id, topicPartition.partition());

                if (payload == null) {
                    continue;
                }

                startingStateBuilder.withPartition(topicPartition, payload.startingOffset);

                // We only have an ending offset on STOP requests
                if (payload.endingOffset != null) {
                    endingStateStateBuilder.withPartition(topicPartition, payload.endingOffset);
                }
            }

            if (payload == null) {
                logger.warn("Sideline request {} did not have any partitions persisted", id);
                continue;
            }

            // Resuming a start request means we apply the previous filter chain to the fire hose
            if (payload.type.equals(SidelineType.START)) {
                logger.info("Resuming START sideline {} {}", payload.id, payload.request.step);

                fireHoseSpout.getFilterChain().addStep(
                    payload.id,
                    payload.request.step
                );
            }

            // Resuming a stopped request means we spin up a new sideline spout
            if (payload.type.equals(SidelineType.STOP)) {
                openVirtualSpout(
                    payload.id,
                    payload.request.step,
                    startingStateBuilder.build(),
                    endingStateStateBuilder.build()
                );
            }
        }

        // If we have a starting trigger (technically they're optional but if you don't have one why are you using this spout), open it
        if (startingTrigger != null) {
            startingTrigger.open(getSpoutConfig());
        } else {
            logger.warn("Sideline spout is configured without a starting trigger");
        }

        // If we have a stopping trigger (technically they're optional but if you don't have one why are you using this spout), open it
        if (stoppingTrigger != null) {
            stoppingTrigger.open(getSpoutConfig());
        } else {
            logger.warn("Sideline spout is configured without a stopping trigger");
        }

        // For emit metrics
        emitCountMetrics = Maps.newHashMap();
    }

    /**
     * Open a virtual spout (like when a sideline stop request is made)
     * @param id Id of the sideline request
     * @param step Filter chain step (it will be negate)
     * @param startingState Starting consumer state
     * @param endingState Ending consumer state
     */
    private void openVirtualSpout(
        final SidelineRequestIdentifier id,
        final FilterChainStep step,
        final ConsumerState startingState,
        final ConsumerState endingState
    ) {
        // Generate our virtualSpoutId using the payload id.
        final String virtualSpoutId = generateVirtualSpoutId(id.toString());

        // This info is repeated in VirtualSidelineSpout.open(), not needed here.
        logger.debug("Starting VirtualSidelineSpout {} with starting state {} and ending state", virtualSpoutId, startingState, endingState);

        // Create spout instance.
        final VirtualSpout spout = new VirtualSpout(
            getSpoutConfig(),
            getTopologyContext(),
            getFactoryManager(),
            getMetricsRecorder(),
            startingState,
            endingState
        );
        spout.setVirtualSpoutId(virtualSpoutId);
        spout.setSidelineRequestIdentifier(id);

        // Add and negate the request's filter step, we leave them stored in their original form and negate them at runtime
        spout.getFilterChain().addStep(id, new NegatingFilterChainStep(step));

        // Now pass the new "resumed" spout over to the coordinator to open and run
        getCoordinator().addSidelineSpout(spout);
    }

    @Override
    public void nextTuple() {
        /**
         * Ask the SpoutCoordinator for the next message that should be emitted.
         * If it returns null, then there's nothing new to emit!
         * If a KafkaMessage object is returned, it contains the appropriately
         * mapped MessageId and Values for the tuple that should be emitted.
         */
        final KafkaMessage kafkaMessage = getCoordinator().nextMessage();
        if (kafkaMessage == null) {
            // Nothing new to emit!
            return;
        }

        // Emit tuple via the output collector.
        getOutputCollector().emit(getOutputStreamId(), kafkaMessage.getValues(), kafkaMessage.getTupleMessageId());

        // Update emit count metric for VirtualSidelineSpout this tuple originated from
        getMetricsRecorder().count(VirtualSpout.class, kafkaMessage.getTupleMessageId().getSrcVirtualSpoutId() + ".emit", 1);

        // Everything below is temporary emit metrics for debugging.

        // Update / Display emit metrics
        final String srcId = kafkaMessage.getTupleMessageId().getSrcVirtualSpoutId();
        if (!emitCountMetrics.containsKey(srcId)) {
            emitCountMetrics.put(srcId, 1L);
        } else {
            emitCountMetrics.put(srcId, emitCountMetrics.get(srcId) + 1L);
        }
        emitCounter++;
        if (emitCounter >= 5_000_000L) {
            for (Map.Entry<String, Long> entry : emitCountMetrics.entrySet()) {
                logger.info("Emit Count on {} => {}", entry.getKey(), entry.getValue());
            }
            emitCountMetrics.clear();
            emitCounter = 0;
        }

        // End temp debugging logs
    }

    /**
     * Declare the output fields and stream id.
     * @param declarer The output field declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Handles both explicitly defined and default stream definitions.
        declarer.declareStream(getOutputStreamId(), factoryManager.createNewDeserializerInstance().getOutputFields());
    }

    /**
     * Called to close up shop and end this instance.
     */
    @Override
    public void close() {
        logger.info("Stopping the coordinator and closing all spouts");

        // Close coordinator
        if (getCoordinator() != null) {
            getCoordinator().close();
            coordinator = null;
        }

        // Close persistence manager
        if (getPersistenceAdapter() != null) {
            getPersistenceAdapter().close();
            persistenceAdapter = null;
        }

        // If we have a starting trigger (technically they're optional but if you don't have one why are you using this spout), close it
        if (startingTrigger != null) {
            startingTrigger.close();
        }

        // If we have a stopping trigger (technically they're optional but if you don't have one why are you using this spout), close it
        if (stoppingTrigger != null) {
            stoppingTrigger.close();
        }

        // Close metrics recorder.
        if (getMetricsRecorder() != null) {
            getMetricsRecorder().close();
        }
    }

    /**
     * Currently a no-op.  We could make this pause things in the coordinator.
     */
    @Override
    public void activate() {
        logger.debug("Activating spout");
    }

    /**
     * Currently a no-op.  We could make this un-pause things in the coordinator.
     */
    @Override
    public void deactivate() {
        logger.debug("Deactivate spout");
    }

    /**
     * Called for a Tuple MessageId when the tuple has been fully processed.
     * @param id - the tuple's message id.
     */
    @Override
    public void ack(Object id) {
        // Cast to appropriate object type
        final TupleMessageId tupleMessageId = (TupleMessageId) id;

        // Ack the tuple via the coordinator
        getCoordinator().ack(tupleMessageId);

        // Update ack count metric for VirtualSidelineSpout this tuple originated from
        getMetricsRecorder().count(VirtualSpout.class, tupleMessageId.getSrcVirtualSpoutId() + ".ack", 1);
    }

    /**
     * Called for a Tuple MessageId when the tuple has failed during processing.
     * @param id - The failed tuple's message id.
     */
    @Override
    public void fail(Object id) {
        // Cast to appropriate object type
        final TupleMessageId tupleMessageId = (TupleMessageId) id;

        logger.warn("Failed {}", tupleMessageId);

        // Fail the tuple via the coordinator
        getCoordinator().fail(tupleMessageId);
    }

    /**
     * @return - the Storm topology config map.
     */
    private Map getSpoutConfig() {
        return spoutConfig;
    }

    /**
     * Utility method to get a specific entry in the Storm topology config map.
     * @param key - the configuration item to retrieve
     * @return - the configuration item's value.
     */
    private Object getSpoutConfigItem(final String key) {
        return getSpoutConfig().get(key);
    }

    /**
     * @return - The Storm topology context.
     */
    private TopologyContext getTopologyContext() {
        return topologyContext;
    }

    /**
     * @return - The factory manager instance.
     */
    private FactoryManager getFactoryManager() {
        return factoryManager;
    }

    /**
     * @return - The spout's output collector.
     */
    private SpoutOutputCollector getOutputCollector() {
        return outputCollector;
    }

    /**
     * @return The spout's metrics recorder implementation.
     */
    private MetricsRecorder getMetricsRecorder() {
        return metricsRecorder;
    }

    /**
     * @return - The virtual spout coordinator.
     */
    SpoutCoordinator getCoordinator() {
        return coordinator;
    }

    /**
     * @return - The persistence manager.
     */
    PersistenceAdapter getPersistenceAdapter() {
        return persistenceAdapter;
    }

    /**
     * @return - returns the stream that tuples will be emitted out.
     */
    String getOutputStreamId() {
        if (outputStreamId == null) {
            if (spoutConfig == null) {
                throw new IllegalStateException("Missing required configuration!  SidelineSpoutConfig not defined!");
            }
            outputStreamId = (String) getSpoutConfigItem(SidelineSpoutConfig.OUTPUT_STREAM_ID);
            if (Strings.isNullOrEmpty(outputStreamId)) {
                outputStreamId = Utils.DEFAULT_STREAM_ID;
            }
        }
        return outputStreamId;
    }

    /**
     * Generates a VirtualSpoutId using an optional postfix.  It also appends
     * the Task index id.  This will probably cause problems if you decrease the number of instances of the spout.
     *
     * @param id - Id to add after the prefix
     * @return - Generates VirtualSpoutId.
     */
    String generateVirtualSpoutId(final String id) {
        if (Strings.isNullOrEmpty(id)) {
            throw new IllegalArgumentException("Id cannot be null or empty!");
        }

        // Also prefixed with our configured prefix
        String newId = (String) getSpoutConfigItem(SidelineSpoutConfig.CONSUMER_ID_PREFIX);
        // append it
        newId += ":" + id;

        // return it
        return newId;
    }
}
