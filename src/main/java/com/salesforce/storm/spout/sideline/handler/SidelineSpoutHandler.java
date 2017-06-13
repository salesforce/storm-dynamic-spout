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
package com.salesforce.storm.spout.sideline.handler;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.salesforce.storm.spout.sideline.ConsumerPartition;
import com.salesforce.storm.spout.sideline.DynamicSpout;
import com.salesforce.storm.spout.sideline.SidelineVirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.SpoutTriggerProxy;
import com.salesforce.storm.spout.sideline.VirtualSpout;
import com.salesforce.storm.spout.sideline.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.config.SpoutConfig;
import com.salesforce.storm.spout.sideline.consumer.ConsumerState;
import com.salesforce.storm.spout.sideline.filter.FilterChainStep;
import com.salesforce.storm.spout.sideline.filter.NegatingFilterChainStep;
import com.salesforce.storm.spout.sideline.persistence.SidelinePayload;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import com.salesforce.storm.spout.sideline.trigger.StartingTrigger;
import com.salesforce.storm.spout.sideline.trigger.StoppingTrigger;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handler for managing sidelines on a DynamicSpout.
 */
public class SidelineSpoutHandler implements SpoutHandler {

    // Logger
    private static final Logger logger = LoggerFactory.getLogger(SidelineSpoutHandler.class);

    /**
     * Identifier for the firehose, or 'main' VirtualSpout instance.
     */
    private static final String MAIN_ID = "main";

    /**
     * The Spout configuration map.
     */
    private Map<String, Object> spoutConfig;

    /**
     * The Topology Context object.
     */
    private TopologyContext topologyContext;

    /**
     * Starting Trigger.
     *
     * This is an instance that is responsible for telling the sideline spout when to begin sidelining.
     */
    private StartingTrigger startingTrigger;

    /**
     * Stopping Trigger.
     *
     * This is an instance is responsible for telling the sideline spout when to stop sidelining
     */
    private StoppingTrigger stoppingTrigger;

    private DynamicSpout spout;

    /**
     * This is our main Virtual Spout instance which consumes from the configured namespace.
     */
    private VirtualSpout fireHoseSpout;

    /**
     * When this handler is opened this method stores spout config for use by the instance.
     * @param spoutConfig Spout configuration.
     */
    @Override
    public void open(Map<String, Object> spoutConfig) {
        this.spoutConfig = spoutConfig;
    }

    /**
     * Handler called when the dynamic spout opens, this method is responsible for creating and setting triggers for
     * handling the spinning up and down of sidelines.
     * @param spout Dynamic spout instance.
     * @param topologyConfig Topology configuration.
     * @param topologyContext Topology context.
     */
    @Override
    public void onSpoutOpen(DynamicSpout spout, Map topologyConfig, TopologyContext topologyContext) {
        this.spout = spout;
        this.topologyContext = topologyContext;
        this.startingTrigger = createStartingTrigger();
        this.stoppingTrigger = createStoppingTrigger();

        // If we have a starting trigger (technically they're optional but if you don't have one why are you using this
        // spout), set the spout proxy on it
        if (startingTrigger != null) {
            startingTrigger.setSidelineSpout(new SpoutTriggerProxy(this));
        }

        // If we have a stopping trigger (technically they're optional but if you don't have one why are you using this
        // spout), set the spout proxy on it
        if (stoppingTrigger != null) {
            stoppingTrigger.setSidelineSpout(new SpoutTriggerProxy(this));
        }

        // Create the main spout for the namespace, we'll dub it the 'firehose'
        fireHoseSpout = new VirtualSpout(
            generateVirtualSpoutId(new SidelineRequestIdentifier(MAIN_ID)),
            getSpoutConfig(),
            topologyContext,
            spout.getFactoryManager(),
            null,
            null
        );

        // Our main firehose spout instance.
        spout.addVirtualSpout(fireHoseSpout);

        final String topic = (String) getSpoutConfig().get(SpoutConfig.KAFKA_TOPIC);

        final List<SidelineRequestIdentifier> existingRequestIds = spout.getPersistenceAdapter().listSidelineRequests();
        logger.info("Found {} existing sideline requests that need to be resumed", existingRequestIds.size());

        for (SidelineRequestIdentifier id : existingRequestIds) {
            final ConsumerState.ConsumerStateBuilder startingStateBuilder = ConsumerState.builder();
            final ConsumerState.ConsumerStateBuilder endingStateStateBuilder = ConsumerState.builder();

            SidelinePayload payload = null;

            final Set<Integer> partitions = spout.getPersistenceAdapter().listSidelineRequestPartitions(id);

            for (final Integer partition : partitions) {
                payload = spout.getPersistenceAdapter().retrieveSidelineRequest(id, partition);

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

    /**
     * Handler called when the dynamic spout closes, this method is responsible for tearing down triggers  sidelining.
     */
    @Override
    public void onSpoutClose(DynamicSpout spout) {
        // If we have a starting trigger (technically they're optional but if you don't have one why are you using this spout), close it
        if (startingTrigger != null) {
            startingTrigger.close();
        }

        // If we have a stopping trigger (technically they're optional but if you don't have one why are you using this spout), close it
        if (stoppingTrigger != null) {
            stoppingTrigger.close();
        }

        startingTrigger = null;
        stoppingTrigger = null;
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
            spout.getPersistenceAdapter().persistSidelineRequestState(
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
        spout.getMetricsRecorder().count(getClass(), "start-sideline", 1L);

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
                "Received STOP sideline request, but I don't actually have any filter chain steps for it! Make sure "
                + "you check that your filter implements an equals() method. {} {}",
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
            final SidelinePayload sidelinePayload = spout.getPersistenceAdapter().retrieveSidelineRequest(id, consumerPartition.partition());

            // Add this partition to the starting consumer state
            startingStateBuilder.withPartition(consumerPartition, sidelinePayload.startingOffset);

            // Persist the side line request state with the new negated version of the steps.
            spout.getPersistenceAdapter().persistSidelineRequestState(
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
        spout.getMetricsRecorder().count(getClass(), "stop-sideline", 1L);
    }


    /**
     * Open a virtual spout (like when a sideline stop request is made).
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
        final VirtualSpoutIdentifier virtualSpoutId = generateVirtualSpoutId(id);

        // This info is repeated in VirtualSidelineSpout.open(), not needed here.
        logger.debug("Starting VirtualSidelineSpout {} with starting state {} and ending state", virtualSpoutId, startingState, endingState);

        // Create spout instance.
        final VirtualSpout virtualSpout = new VirtualSpout(
            virtualSpoutId,
            getSpoutConfig(),
            topologyContext,
            spout.getFactoryManager(),
            startingState,
            endingState
        );

        // Add the supplied filter chain step to the new virtual spout's filter chain
        virtualSpout.getFilterChain().addStep(id, step);

        // Now pass the new "resumed" spout over to the coordinator to open and run
        spout.addVirtualSpout(virtualSpout);
    }

    /**
     * Generates a VirtualSpoutId from a sideline request id.
     *
     * @param sidelineRequestIdentifier Sideline request to use for constructing the id
     * @return Generated VirtualSpoutId.
     */
    VirtualSpoutIdentifier generateVirtualSpoutId(final SidelineRequestIdentifier sidelineRequestIdentifier) {
        Preconditions.checkArgument(
            !Strings.isNullOrEmpty(sidelineRequestIdentifier.toString()),
            "SidelineRequestIdentifier cannot be null or empty!"
        );

        // Also prefixed with our configured prefix
        final String prefix = (String) getSpoutConfig().get(SpoutConfig.CONSUMER_ID_PREFIX);

        // return it
        return new SidelineVirtualSpoutIdentifier(prefix, sidelineRequestIdentifier);
    }


    /**
     * Create an instance of the configured StartingTrigger.
     * @return Instance of a StartingTrigger
     */
    @SuppressWarnings("unchecked")
    public synchronized StartingTrigger createStartingTrigger() {
        String classStr = (String) getSpoutConfig().get(SpoutConfig.STARTING_TRIGGER_CLASS);
        // Empty class is allowed, this is not required to be configured
        if (Strings.isNullOrEmpty(classStr)) {
            return null;
        }

        try {
            return ((Class<? extends StartingTrigger>) Class.forName(classStr)).newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create an instance of the configured StoppingTrigger.
     * @return Instance of a StoppingTrigger
     */
    @SuppressWarnings("unchecked")
    public synchronized StoppingTrigger createStoppingTrigger() {
        String classStr = (String) getSpoutConfig().get(SpoutConfig.STOPPING_TRIGGER_CLASS);
        // Empty class is allowed, this is not required to be configured
        if (Strings.isNullOrEmpty(classStr)) {
            return null;
        }

        try {
            return ((Class<? extends StoppingTrigger>) Class.forName(classStr)).newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the firehose virtual spout
     * @return Firehose virtual spout.
     */
    VirtualSpout getFireHoseSpout() {
        return fireHoseSpout;
    }

    /**
     * Get the spout config.
     * @return Spout config.
     */
    Map<String, Object> getSpoutConfig() {
        return spoutConfig;
    }

    /**
     * Get the stopping trigger.
     * @return Stopping trigger.
     */
    StartingTrigger getStartingTrigger() {
        return startingTrigger;
    }

    /**
     * Get the starting trigger.
     * @return Starting trigger.
     */
    StoppingTrigger getStoppingTrigger() {
        return stoppingTrigger;
    }
}
