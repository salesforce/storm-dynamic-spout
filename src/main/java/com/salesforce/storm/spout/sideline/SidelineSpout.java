package com.salesforce.storm.spout.sideline;

import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.filter.FilterChainStep;
import com.salesforce.storm.spout.sideline.filter.NegatingFilterChainStep;
import com.salesforce.storm.spout.sideline.consumer.ConsumerState;
import com.salesforce.storm.spout.sideline.persistence.SidelinePayload;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import com.salesforce.storm.spout.sideline.trigger.StartingTrigger;
import com.salesforce.storm.spout.sideline.trigger.StoppingTrigger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Spout instance.
 */
public class SidelineSpout extends DynamicSpout {

    private static final Logger logger = LoggerFactory.getLogger(SidelineSpout.class);

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
     * This is our main Virtual Spout instance which consumes from the configured namespace.
     * TODO: Do we need access to this here?  Could this be moved into the Coordinator?
     */
    private VirtualSpout fireHoseSpout;

    public SidelineSpout(Map config) {
        super(config);
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

        for (final ConsumerPartition consumerPartition : startingState.getConsumerPartitions()) {
            // Store in request manager
            getPersistenceAdapter().persistSidelineRequestState(
                SidelineType.START,
                sidelineRequest.id, // TODO: Now that this is in the request, we should change the persistence adapter
                sidelineRequest,
                consumerPartition.partition(),
                startingState.getOffsetForNamespaceAndPartition(consumerPartition),
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
        // Create a negated version of the step we just pulled from the firehose
        final FilterChainStep negatedStep = new NegatingFilterChainStep(step);

        // This is the state that the VirtualSidelineSpout should end with
        final ConsumerState endingState = fireHoseSpout.getCurrentState();

        // We'll construct a consumer state from the various partition data stored for this sideline request
        final ConsumerState.ConsumerStateBuilder startingStateBuilder = ConsumerState.builder();

        // We are looping over the current partitions for the firehose, functionally this is the collection of partitions
        // assigned to this particular sideline spout instance
        for (final ConsumerPartition consumerPartition : endingState.getConsumerPartitions()) {
            // This is the state that the VirtualSidelineSpout should start with
            final SidelinePayload sidelinePayload = getPersistenceAdapter().retrieveSidelineRequest(id, consumerPartition.partition());

            // Add this partition to the starting consumer state
            startingStateBuilder.withPartition(consumerPartition, sidelinePayload.startingOffset);

            // Persist the side line request state with the new negated version of the steps.
            getPersistenceAdapter().persistSidelineRequestState(
                SidelineType.STOP,
                id,
                new SidelineRequest(id, negatedStep), // Persist the negated steps, so they load properly on resume
                consumerPartition.partition(),
                sidelinePayload.startingOffset,
                endingState.getOffsetForNamespaceAndPartition(consumerPartition)
            );
        }

        // Build our starting state, this is a map of partition and offset
        final ConsumerState startingState = startingStateBuilder.build();

        openVirtualSpout(
            id,
            negatedStep,
            startingState,
            endingState
        );

        // Update stop count metric
        getMetricsRecorder().count(getClass(), "stop-sideline", 1L);
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

        // TODO: Sort this out so that we can track the sideline request identifier inside of the virtual spout identifier
        spout.setVirtualSpoutId(virtualSpoutId);
        spout.setSidelineRequestIdentifier(id);

        // Add the supplied filter chain step to the new virtual spout's filter chain
        spout.getFilterChain().addStep(id, step);

        // Now pass the new "resumed" spout over to the coordinator to open and run
        addVirtualSpout(spout);
    }

    void onOpen(Map topologyConfig, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        // If we have a starting trigger (technically they're optional but if you don't have one why are you using this spout), set the spout proxy on it
        if (startingTrigger != null) {
            startingTrigger.setSidelineSpout(new SpoutTriggerProxy(this));
        }

        // If we have a stopping trigger (technically they're optional but if you don't have one why are you using this spout), set the spout proxy on it
        if (stoppingTrigger != null) {
            stoppingTrigger.setSidelineSpout(new SpoutTriggerProxy(this));
        }

        // Create the main spout for the namespace, we'll dub it the 'firehose'
        fireHoseSpout = new VirtualSpout(
            getSpoutConfig(),
            getTopologyContext(),
            getFactoryManager(),
            getMetricsRecorder()
        );
        // TODO: Add callbacks fro onOpen and onClose
        /**
        fireHoseSpout.onOpen((Consumer consumer, String identifier) -> {

        })
         **/
        fireHoseSpout.setVirtualSpoutId(generateVirtualSpoutId("main"));

        // Our main firehose spout instance.
        addVirtualSpout(fireHoseSpout);

        final String topic = (String) getSpoutConfigItem(SidelineSpoutConfig.KAFKA_TOPIC);

        final List<SidelineRequestIdentifier> existingRequestIds = getPersistenceAdapter().listSidelineRequests();
        logger.info("Found {} existing sideline requests that need to be resumed", existingRequestIds.size());

        for (SidelineRequestIdentifier id : existingRequestIds) {
            final ConsumerState.ConsumerStateBuilder startingStateBuilder = ConsumerState.builder();
            final ConsumerState.ConsumerStateBuilder endingStateStateBuilder = ConsumerState.builder();

            SidelinePayload payload = null;

            final Set<Integer> partitions = getPersistenceAdapter().listSidelineRequestPartitions(id);

            for (final Integer partition : partitions) {
                payload = getPersistenceAdapter().retrieveSidelineRequest(id, partition);

                if (payload == null) {
                    continue;
                }
                startingStateBuilder.withPartition(topic, partition, payload.startingOffset);

                // We only have an ending offset on STOP requests
                if (payload.endingOffset != null) {
                    endingStateStateBuilder.withPartition(topic, partition, payload.endingOffset);
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
    }

    void onClose() {
        // If we have a starting trigger (technically they're optional but if you don't have one why are you using this spout), close it
        if (startingTrigger != null) {
            startingTrigger.close();
        }

        // If we have a stopping trigger (technically they're optional but if you don't have one why are you using this spout), close it
        if (stoppingTrigger != null) {
            stoppingTrigger.close();
        }
    }

    void onActivate() {}

    void onDeactivate() {}
}
