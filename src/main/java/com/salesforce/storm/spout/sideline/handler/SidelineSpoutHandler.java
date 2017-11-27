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
import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.DelegateSpout;
import com.salesforce.storm.spout.dynamic.DelegateSpoutFactory;
import com.salesforce.storm.spout.dynamic.DynamicSpout;
import com.salesforce.storm.spout.dynamic.FactoryManager;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.handler.SpoutHandler;
import com.salesforce.storm.spout.sideline.SidelineVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.DynamicSpoutConfig;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerState;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;
import com.salesforce.storm.spout.dynamic.filter.FilterChainStep;
import com.salesforce.storm.spout.dynamic.filter.InvalidFilterChainStepException;
import com.salesforce.storm.spout.dynamic.filter.NegatingFilterChainStep;
import com.salesforce.storm.spout.sideline.metrics.SidelineMetrics;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.persistence.SidelinePayload;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineTrigger;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * Handler for managing sidelines on a DynamicSpout.
 */
public class SidelineSpoutHandler implements SpoutHandler, SidelineController {

    // Logger
    private static final Logger logger = LoggerFactory.getLogger(SidelineSpoutHandler.class);

    /**
     * Identifier for the firehose, or 'main' VirtualSpout instance.
     */
    private static final String MAIN_ID = "main";

    private boolean isOpen = false;

    /**
     * The Spout configuration map.
     */
    private SpoutConfig spoutConfig;

    /**
     * The Topology Context object.
     */
    private TopologyContext topologyContext;

    /**
     * Timer for periodically checking the state of the VirtualSpouts being managed.
     */
    private Timer timer;

    /**
     * Collection of sideline triggers to manage.
     */
    private final List<SidelineTrigger> sidelineTriggers = new ArrayList<>();

    /**
     * {@link DynamicSpout} instance that has this handler attached to it.
     */
    private DynamicSpout spout;

    /**
     * Persistence layer for storing sideline state.
     */
    private PersistenceAdapter persistenceAdapter;

    /**
     * Factory for creating {@link com.salesforce.storm.spout.dynamic.DelegateSpout} instac.es
     */
    private DelegateSpoutFactory delegateSpoutFactory;

    /**
     * This is our main Virtual Spout instance which consumes from the configured namespace.
     */
    private DelegateSpout fireHoseSpout;

    /**
     * When this handler is opened this method stores spout config for use by the instance.
     * @param spoutConfig Spout configuration.
     * @param delegateSpoutFactory Factory for creating {@link DelegateSpout} instances.
     */
    @Override
    public void open(final SpoutConfig spoutConfig, final DelegateSpoutFactory delegateSpoutFactory) {
        if (isOpen) {
            throw new RuntimeException("SidelineSpoutHandler is already opened!");
        }

        isOpen = true;

        this.spoutConfig = spoutConfig;

        final String persistenceAdapterClass = (String) spoutConfig.get(SidelineConfig.PERSISTENCE_ADAPTER_CLASS);

        Preconditions.checkArgument(
            persistenceAdapterClass != null && !persistenceAdapterClass.isEmpty(),
            "Sideline persistence adapter class is required"
        );

        this.persistenceAdapter = FactoryManager.createNewInstance(
            persistenceAdapterClass
        );
        this.persistenceAdapter.open(spoutConfig);

        this.delegateSpoutFactory = delegateSpoutFactory;
    }

    /**
     * When this handler is closed this method is called.
     */
    @Override
    public void close() {
        cancelTimer();

        if (persistenceAdapter != null) {
            persistenceAdapter.close();
            persistenceAdapter = null;
        }

        isOpen = false;
    }

    private void cancelTimer() {
        if (timer != null) {
            // Cancel timer background thread.
            timer.cancel();
            timer = null;
        }
    }

    /**
     * Handler called when the dynamic spout opens, this method is responsible for creating and setting triggers for
     * handling the spinning up and down of sidelines.
     * @param spout Dynamic spout instance.
     * @param topologyConfig Topology configuration.
     * @param topologyContext Topology context.
     */
    @Override
    public void onSpoutOpen(
        final DynamicSpout spout,
        final Map topologyConfig,
        final TopologyContext topologyContext
    ) {
        this.spout = spout;
        this.topologyContext = topologyContext;

        createSidelineTriggers();

        Preconditions.checkArgument(
            spoutConfig.hasNonNullValue(SidelineConfig.REFRESH_INTERVAL_SECONDS),
            "Configuration value for " + SidelineConfig.REFRESH_INTERVAL_SECONDS + " is required."
        );

        final long refreshIntervalSeconds = spoutConfig.getLong(SidelineConfig.REFRESH_INTERVAL_SECONDS);

        final long refreshIntervalMillis = TimeUnit.SECONDS.toMillis(refreshIntervalSeconds);

        // Why not just start the timer at 0? Because we want to block onSpoutOpen() until the first run of loadSidelines()
        loadSidelines();

        // Repeat our sidelines check periodically
        final String threadName = "[" + DynamicSpout.class.getSimpleName() + ":" + getClass().getSimpleName() + "] Timer on "
            + topologyContext.getThisComponentId() + ":" + topologyContext.getThisTaskIndex();

        timer = new Timer(threadName);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                // Catch this so that it doesn't kill the recurring task
                try {
                    loadSidelines();
                } catch (Exception ex) {
                    logger.error("Attempting to loadSidelines() failed {}", ex);
                }
            }
        }, refreshIntervalMillis, refreshIntervalMillis);

        for (final SidelineTrigger sidelineTrigger : sidelineTriggers) {
            sidelineTrigger.open(getSpoutConfig());
        }
    }

    /**
     * Loads existing sideline requests that have been previously persisted and checks to make sure that they are running.
     *
     * This method is synchronized to avoid having overlap runs, which could cause odd state issues as the same operation
     * is essentially being performed X times.
     */
    synchronized void loadSidelines() {
        final VirtualSpoutIdentifier fireHoseIdentifier = getFireHoseSpoutIdentifier();

        // Presumably this happens only if the VirtualSpout has crashed inside of the coordinator.  What this means is that the thread
        // died, but we have a left over instance.  This is very unlikely to happen
        if (!spout.hasVirtualSpout(fireHoseIdentifier) && fireHoseSpout != null) {
            // We do not call close() here because it's NOT safe.
            fireHoseSpout = null;
        }

        // If we haven't spun up a VirtualSpout yet, we create it here.
        if (fireHoseSpout == null) {
            // Create the main spout for the namespace, we'll dub it the 'firehose'
            fireHoseSpout = delegateSpoutFactory.create(fireHoseIdentifier);
        }

        final List<SidelineRequestIdentifier> existingRequestIds = getPersistenceAdapter().listSidelineRequests();
        logger.info("Found {} existing sideline requests that need to be resumed", existingRequestIds.size());

        for (SidelineRequestIdentifier id : existingRequestIds) {
            final ConsumerState.ConsumerStateBuilder startingStateBuilder = ConsumerState.builder();
            final ConsumerState.ConsumerStateBuilder endingStateStateBuilder = ConsumerState.builder();

            SidelinePayload payload = null;

            final Set<ConsumerPartition> partitions = getPersistenceAdapter().listSidelineRequestPartitions(id);

            for (final ConsumerPartition partition : partitions) {
                payload = retrieveSidelinePayload(id, partition);

                if (payload == null) {
                    logger.warn("Sideline {} on partition {} payload was null, this is probably a serialization problem.");
                    continue;
                }

                startingStateBuilder.withPartition(partition, payload.startingOffset);

                // We only have an ending offset on STOP requests
                if (payload.endingOffset != null) {
                    endingStateStateBuilder.withPartition(partition, payload.endingOffset);
                }
            }

            if (payload == null) {
                logger.warn("Sideline request {} did not have any persisted partitions", id);
                continue;
            }

            // Resuming a start request means we apply the previous filter chain to the fire hose
            if (payload.type.equals(SidelineType.START)) {
                logger.info("Resuming START sideline {} {}", payload.id, payload.request.step);

                // Only add the step if the id isn't already in the chain.  Note that we do NOT check for the step here, it's possible
                // though very unusual to have the exact same step used in different requests. While that's silly, we don't want to choke
                // on it here and put the filter chain in a weird state.
                if (!fireHoseSpout.getFilterChain().hasStep(payload.id)) {
                    fireHoseSpout.getFilterChain().addStep(
                        payload.id,
                        payload.request.step
                    );
                }
            }

            // Resuming a stopped request means we spin up a new sideline spout
            if (payload.type.equals(SidelineType.STOP)) {
                // This method will check to see that the VirtualSpout isn't already in the SpoutCoordinator before adding it
                addSidelineVirtualSpout(
                    payload.id,
                    payload.request.step,
                    startingStateBuilder.build(),
                    endingStateStateBuilder.build()
                );
            }
        }

        // After altering the filter chain is complete, lets NOW start the fire hose
        // This keeps a race condition where the fire hose could start consuming before filter chain
        // steps get added.
        if (!spout.hasVirtualSpout(fireHoseIdentifier)) {
            spout.addVirtualSpout(fireHoseSpout);
        }
    }

    /**
     * Handler called when the dynamic spout closes, this method is responsible for tearing down triggers  sidelining.
     */
    @Override
    public void onSpoutClose(final DynamicSpout spout) {
        cancelTimer();

        final ListIterator<SidelineTrigger> iter = sidelineTriggers.listIterator();

        while (iter.hasNext()) {
            // Get the trigger
            final SidelineTrigger sidelineTrigger = iter.next();
            // Close the trigger
            sidelineTrigger.close();
            // Remove it from our list of triggers
            iter.remove();
        }
    }

    /**
     * Does a sideline exist in the started state?
     * @param sidelineRequest sideline request.
     * @return true it does, false it does not.
     */
    public boolean isSidelineStarted(SidelineRequest sidelineRequest) {
        return fireHoseSpout.getFilterChain().hasStep(sidelineRequest.id);
    }

    /**
     * Starts a sideline request.
     * @param sidelineRequest representation of the request that is being started.
     */
    public void startSidelining(SidelineRequest sidelineRequest) {
        logger.info("Received START sideline request");

        // Store the offset that this request was made at, when the sideline stops we will begin processing at
        // this offset
        final ConsumerState startingState = getFireHoseCurrentState();

        for (final ConsumerPartition consumerPartition : startingState.getConsumerPartitions()) {
            // Store in request manager
            getPersistenceAdapter().persistSidelineRequestState(
                SidelineType.START,
                sidelineRequest.id, // TODO: Now that this is in the request, we should change the persistence adapter
                sidelineRequest,
                consumerPartition,
                startingState.getOffsetForNamespaceAndPartition(consumerPartition),
                null
            );
        }

        // Add our new filter steps
        fireHoseSpout.getFilterChain().addStep(sidelineRequest.id, sidelineRequest.step);

        // Update start countBy metric
        spout.getMetricsRecorder().count(SidelineMetrics.START);
    }

    /**
     * Does a sideline exist in the stopped state?
     * @param sidelineRequest sideline request.
     * @return true it has, false it has not.
     */
    public boolean isSidelineStopped(SidelineRequest sidelineRequest) {
        return spout.hasVirtualSpout(
            generateSidelineVirtualSpoutId(sidelineRequest.id)
        );
    }

    /**
     * Stops a sideline request.
     * @param sidelineRequest A representation of the request that is being stopped
     */
    public void stopSidelining(SidelineRequest sidelineRequest) {
        final SidelineRequestIdentifier id = (SidelineRequestIdentifier) fireHoseSpout.getFilterChain().findStep(sidelineRequest.step);

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
        final FilterChainStep step = fireHoseSpout.getFilterChain().removeStep(id);
        // Create a negated version of the step we just pulled from the firehose
        final FilterChainStep negatedStep = new NegatingFilterChainStep(step);

        // This is the state that the VirtualSpout should end with
        final ConsumerState endingState = getFireHoseCurrentState();

        // We'll construct a consumer state from the various partition data stored for this sideline request
        final ConsumerState.ConsumerStateBuilder startingStateBuilder = ConsumerState.builder();

        // We are looping over the current partitions for the firehose, functionally this is the collection of partitions
        // assigned to this particular sideline spout instance
        for (final ConsumerPartition consumerPartition : endingState.getConsumerPartitions()) {
            // This is the state that the VirtualSpout should start with
            final SidelinePayload sidelinePayload = retrieveSidelinePayload(
                id,
                consumerPartition
            );

            if (sidelinePayload == null) {
                logger.warn("Sideline {} on partition {} payload was null, this is probably a serialization problem.");
                continue;
            }

            logger.info("Loaded sideline payload for {} = {}", consumerPartition, sidelinePayload);

            // Add this partition to the starting consumer state
            startingStateBuilder.withPartition(consumerPartition, sidelinePayload.startingOffset);

            // Persist the side line request state with the new negated version of the steps.
            getPersistenceAdapter().persistSidelineRequestState(
                SidelineType.STOP,
                id,
                new SidelineRequest(id, negatedStep), // Persist the negated steps, so they load properly on resume
                consumerPartition,
                sidelinePayload.startingOffset,
                endingState.getOffsetForNamespaceAndPartition(consumerPartition)
            );
        }

        // Build our starting state, this is a map of partition and offset
        final ConsumerState startingState = startingStateBuilder.build();

        addSidelineVirtualSpout(
            id,
            negatedStep,
            startingState,
            endingState
        );

        // Update stop countBy metric
        spout.getMetricsRecorder().count(SidelineMetrics.STOP);
    }

    /**
     * Retrieve the current state from the fire hose, try a few times if the fire hose consumer hasn't finished doing
     * its thing.  This method is intended to block until the virtual spout gives back state or we've waited too long.
     * @return current consumer state for the fire hose, or null if something is messed up.
     */
    private ConsumerState getFireHoseCurrentState() {
        // Track how many times we've attempted to get the fire hoses current state
        int trips = 0;
        ConsumerState currentState = null;

        do {
            try {
                trips++;

                logger.info("Attempting to pull current state from the fire hose.");

                // This could come back null is the consumer is null, which happens when we try calling getCurrentState()
                // before the consumer and the virtual spout has opened
                currentState = fireHoseSpout.getCurrentState();

                // We got current state back, so we can return it now
                if (currentState != null) {
                    logger.info("Received current state from the fire hose on trip {}! {}", trips, currentState);
                    return currentState;
                }

                // Wait half a second before we try this again
                Thread.sleep(500L);
            } catch (InterruptedException ex) {
                // Log the error, but we're going to take another attempt at this before we give up
                logger.error("Trying to get the current state from the firehose and I got interrupted {}", ex);
            }
        }
        while (currentState == null && trips < 10);

        logger.error("We've tried 10 times to pull the current state from the fire hose consumer and are now giving up.");

        throw new IllegalStateException("Unable to pull current state from the fire hose after a few attempts!");
    }

    /**
     * Open a virtual spout for a sideline (do not use this for the firehose).
     * @param id Id of the sideline request
     * @param step Filter chain step (it will be negate)
     * @param startingState Starting consumer state
     * @param endingState Ending consumer state
     */
    private void addSidelineVirtualSpout(
        final SidelineRequestIdentifier id,
        final FilterChainStep step,
        final ConsumerState startingState,
        final ConsumerState endingState
    ) {
        // Generate our virtualSpoutId using the payload id.
        final VirtualSpoutIdentifier virtualSpoutId = generateSidelineVirtualSpoutId(id);

        if (spout.hasVirtualSpout(virtualSpoutId)) {
            logger.info("VirtualSpout {} is already running", virtualSpoutId);
            return;
        }

        // This info is repeated in VirtualSpout.open(), not needed here.
        logger.debug("Starting VirtualSpout {} with starting state {} and ending state", virtualSpoutId, startingState, endingState);

        // Create spout instance.
        final DelegateSpout virtualSpout = delegateSpoutFactory.create(virtualSpoutId, startingState, endingState);

        // Add the supplied filter chain step to the new virtual spout's filter chain
        virtualSpout.getFilterChain().addStep(id, step);

        // Now pass the new "resumed" spout over to the coordinator to open and run
        spout.addVirtualSpout(virtualSpout);
    }

    /**
     * Get the virtual spout id prefix from the config.
     * @return virtual spout id prefix.
     */
    String getVirtualSpoutIdPrefix() {
        return (String) getSpoutConfig().get(DynamicSpoutConfig.VIRTUAL_SPOUT_ID_PREFIX);
    }

    /**
     * Generates a VirtualSpoutId from a sideline request id.
     *
     * @param sidelineRequestIdentifier Sideline request to use for constructing the id
     * @return Generated VirtualSpoutId.
     */
    VirtualSpoutIdentifier generateSidelineVirtualSpoutId(final SidelineRequestIdentifier sidelineRequestIdentifier) {
        Preconditions.checkArgument(
            !Strings.isNullOrEmpty(sidelineRequestIdentifier.toString()),
            "SidelineRequestIdentifier cannot be null or empty!"
        );

        // Prefix with our configured virtual spout id, usually the consumer, then use the sideline identifier
        return new SidelineVirtualSpoutIdentifier(getVirtualSpoutIdPrefix(), sidelineRequestIdentifier);
    }

    private SidelinePayload retrieveSidelinePayload(final SidelineRequestIdentifier id, final ConsumerPartition consumerPartition) {
        try {
            return getPersistenceAdapter().retrieveSidelineRequest(id, consumerPartition);
        } catch (InvalidFilterChainStepException ex) {
            logger.error("Unable to load sideline payload {}", ex);
            // Basically if we can't deserialize the step we're not sending back any part of the payload.
            return null;
        }
    }

    /**
     * Create an instance of the configured SidelineTrigger.
     */
    @SuppressWarnings("unchecked")
    synchronized void createSidelineTriggers() {
        final Object triggerClass = getSpoutConfig().get(SidelineConfig.TRIGGER_CLASS);

        // No triggers configured, nothing to setup!
        if (triggerClass == null) {
            return;
        }

        final List<String> sidelineTriggersClasses = (triggerClass instanceof String)
            ? Collections.singletonList((String) triggerClass)
            : (List<String>) triggerClass;

        for (final String sidelineTriggerClass : sidelineTriggersClasses) {
            // Will throw a RuntimeException if this is not configured correctly
            final SidelineTrigger sidelineTrigger = FactoryManager.createNewInstance(sidelineTriggerClass);

            // This is the new preferred approach for triggers
            sidelineTrigger.setSidelineController(this);

            // Add it to our collection so we can iterate it later.
            sidelineTriggers.add(sidelineTrigger);
        }
    }

    /**
     * Get the firehose virtual spout
     * @return Firehose virtual spout.
     */
    DelegateSpout getFireHoseSpout() {
        return fireHoseSpout;
    }

    /**
     * Get the firehose virtual spout identifier.
     * @return firehose virtual spout identifier.
     */
    VirtualSpoutIdentifier getFireHoseSpoutIdentifier() {
        return new DefaultVirtualSpoutIdentifier(getVirtualSpoutIdPrefix() + ":" + MAIN_ID);
    }

    /**
     * Get the spout config.
     * @return Spout config.
     */
    SpoutConfig getSpoutConfig() {
        return spoutConfig;
    }

    /**
     * Get the sideline triggers created for use by the spout handler.
     * @return list of sideline triggers.
     */
    List<SidelineTrigger> getSidelineTriggers() {
        return sidelineTriggers;
    }

    PersistenceAdapter getPersistenceAdapter() {
        return persistenceAdapter;
    }
}
