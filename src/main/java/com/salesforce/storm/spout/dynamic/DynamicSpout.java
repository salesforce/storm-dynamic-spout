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

package com.salesforce.storm.spout.dynamic;

import com.google.common.base.Strings;
import com.salesforce.storm.spout.dynamic.buffer.MessageBuffer;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.coordinator.SpoutCoordinator;
import com.salesforce.storm.spout.dynamic.coordinator.ThreadContext;
import com.salesforce.storm.spout.dynamic.exception.SpoutAlreadyExistsException;
import com.salesforce.storm.spout.dynamic.exception.SpoutDoesNotExistException;
import com.salesforce.storm.spout.dynamic.exception.SpoutNotOpenedException;
import com.salesforce.storm.spout.dynamic.handler.SpoutHandler;
import com.salesforce.storm.spout.dynamic.metrics.SpoutMetrics;
import com.salesforce.storm.spout.dynamic.metrics.MetricsRecorder;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * DynamicSpout's contain other virtualized spouts, and provide mechanisms for interacting with the spout life cycle
 * through a set of handlers.  This is done in such a way that Storm does not have to be aware of all the contributing
 * spouts in the implementation.
 */
public class DynamicSpout extends BaseRichSpout {

    // Logging
    private static final Logger logger = LoggerFactory.getLogger(DynamicSpout.class);

    /**
     * The Spout configuration.
     */
    private AbstractConfig spoutConfig;

    /**
     * Spout's output collector, for emitting tuples out into the topology.
     */
    private SpoutOutputCollector outputCollector;

    /**
     * The Topology Context object.
     */
    private TopologyContext topologyContext;

    /**
     * SpoutCoordinator is in charge of starting, stopping, and monitoring VirtualSpouts.
     * Its monitoring thread lives as a long running process.
     */
    private SpoutCoordinator spoutCoordinator;

    /**
     * ThreadSafe routing of emitted, acked, and failed tuples between this DynamicSpout instance
     * and the appropriate Virtual Spouts.
     */
    private SpoutMessageBus messageBus;

    /**
     * Manages creating implementation instances.
     */
    private final FactoryManager factoryManager;

    /**
     * Factory creating {@link DelegateSpout} instances.
     */
    private DelegateSpoutFactory virtualSpoutFactory;

    /**
     * Handler for callbacks at various stages of a dynamic spout's lifecycle.
     */
    private SpoutHandler spoutHandler;

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
     * Whether or not the spout has been previously opened.
     */
    private boolean isOpen = false;

    /**
     * Constructor to create our spout.
     * @param spoutConfig Our configuration.
     */
    public DynamicSpout(AbstractConfig spoutConfig) {
        // Save off config instance.
        this.spoutConfig = spoutConfig;

        // Create our factory manager, which must be serializable.
        this.factoryManager = new FactoryManager(getSpoutConfig());
    }

    /**
     * Open is called once the spout instance has been deployed to the Storm cluster
     * and is ready to get to work.
     *
     * @param topologyConfig The Storm Topology configuration.
     * @param topologyContext The Storm Topology context.
     * @param spoutOutputCollector The output collector to emit tuples via.
     * @throws IllegalStateException if you attempt to open the spout multiple times.
     */
    @Override
    public void open(Map topologyConfig, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        if (isOpen) {
            throw new IllegalStateException("This spout has already been opened.");
        }

        // Save references.
        this.topologyContext = topologyContext;
        this.outputCollector = spoutOutputCollector;

        // Ensure a consumer id prefix has been correctly set.
        if (Strings.isNullOrEmpty((String) getSpoutConfigItem(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX))) {
            throw new IllegalStateException("Missing required configuration: " + SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX);
        }

        // We do not use the getters for things like the metricsRecorder and coordinator here
        // because each of these getters perform a check to see if the spout is open, and it's not yet until we've
        // finished setting all of these things up.

        // Initialize Metric Recorder
        this.metricsRecorder = getFactoryManager().createNewMetricsRecorder();
        this.metricsRecorder.open(getSpoutConfig(), getTopologyContext());

        // Create MessageBuffer
        final MessageBuffer messageBuffer = getFactoryManager().createNewMessageBufferInstance();
        messageBuffer.open(getSpoutConfig());

        // Create MessageBus instance and store into SpoutMessageBus reference reducing accessible scope.
        final MessageBus messageBus = new MessageBus(messageBuffer);
        this.messageBus = messageBus;

        // Define thread context, this allows us to use contextually relevant thread names.
        final ThreadContext threadContext = new ThreadContext(
            topologyContext.getThisComponentId(),
            topologyContext.getThisTaskIndex()
        );

        // Create Coordinator instance and call open.
        spoutCoordinator = new SpoutCoordinator(
            getSpoutConfig(),
            threadContext,
            messageBus,
            metricsRecorder
        );
        spoutCoordinator.open();

        // TODO: This should be configurable and created dynamically, the problem is that right now we are still tightly
        // coupled to the VirtualSpout implementation.
        this.virtualSpoutFactory = new VirtualSpoutFactory(
            spoutConfig,
            topologyContext,
            factoryManager,
            metricsRecorder
        );

        // Our spout is open, it's not dependent upon the handler to finish opening for us to be 'opened'
        // This is important, because if we waited most of our getters that check the opened state of the
        // spout would throw an exception and make them unusable.
        isOpen = true;

        this.spoutHandler = getFactoryManager().createSpoutHandler();
        this.spoutHandler.open(spoutConfig, virtualSpoutFactory);
        this.spoutHandler.onSpoutOpen(this, topologyConfig, topologyContext);
    }

    /**
     * Get the next tuple from the spout.
     */
    @Override
    public void nextTuple() {
        // Report any errors
        final Throwable reportedError = getMessageBus().nextReportedError();
        if (reportedError != null) {
            getOutputCollector().reportError(reportedError);
        }

        // Ask the MessageBus for the next message that should be emitted. If it returns null, then there's
        // nothing new to emit! If a Message object is returned, it contains the appropriately mapped MessageId and
        // Values for the tuple that should be emitted.
        final Message message = getMessageBus().nextMessage();
        if (message == null) {
            // Nothing new to emit!
            return;
        }

        // Emit tuple via the output collector.
        getOutputCollector().emit(getOutputStreamId(), message.getValues(), message.getMessageId());

        // Update emit count metric for VirtualSpout this tuple originated from
        getMetricsRecorder()
            .count(SpoutMetrics.VIRTUAL_SPOUT_EMIT, message.getMessageId().getSrcVirtualSpoutId().toString());
    }

    /**
     * Declare the output fields and stream id.
     * @param declarer The output field declarer
     */
    @Override
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        // Handles both explicitly defined and default stream definitions.
        final String streamId = getOutputStreamId();

        // Construct fields from config
        final Object fieldsCfgValue = getSpoutConfigItem(SpoutConfig.OUTPUT_FIELDS);
        final Fields fields;
        if (fieldsCfgValue instanceof List && !((List) fieldsCfgValue).isEmpty() && ((List) fieldsCfgValue).get(0) instanceof String) {
            // List of String values.
            fields = new Fields((List<String>) fieldsCfgValue);
        } else if (fieldsCfgValue instanceof String) {
            // Log deprecation warning.
            logger.warn(
                "Supplying configuration {} as a comma separated string is deprecated.  Please migrate your "
                + "configuration to provide this option as a List.", SpoutConfig.OUTPUT_FIELDS
            );
            // Comma separated
            fields = new Fields(Tools.splitAndTrim((String) fieldsCfgValue));
        } else if (fieldsCfgValue instanceof Fields) {
            fields = (Fields) fieldsCfgValue;
        } else {
            throw new RuntimeException("Invalid configuration value for spout output fields, perhaps this hasn't been configured yet?");
        }

        logger.debug("Declaring stream name {} with fields {}", streamId, fields);
        declarer.declareStream(streamId, fields);
    }

    /**
     * Called to close up shop and end this instance.
     */
    @Override
    public void close() {
        if (!isOpen) {
            logger.warn("This spout is not actually opened, cowardly refusing to try closing it!");
            return;
        }

        logger.info("Stopping the coordinator and closing all spouts");

        // Close Spout Monitor
        if (getSpoutCoordinator() != null) {
            // Call close on spout monitor.
            getSpoutCoordinator().close();

            // Null reference
            spoutCoordinator = null;
        }

        // Close metrics recorder.
        if (getMetricsRecorder() != null) {
            getMetricsRecorder().close();
            metricsRecorder = null;
        }

        if (getSpoutHandler() != null) {
            getSpoutHandler().onSpoutClose(this);
            getSpoutHandler().close();
            spoutHandler = null;
        }

        isOpen = false;
    }

    /**
     * Currently a no-op.  We could make this pause things in the coordinator.
     */
    @Override
    public void activate() {
        logger.debug("Activating spout");
        if (getSpoutHandler() != null) {
            getSpoutHandler().onSpoutActivate(this);
        }
    }

    /**
     * Currently a no-op.  We could make this un-pause things in the coordinator.
     */
    @Override
    public void deactivate() {
        logger.debug("Deactivate spout");
        if (getSpoutHandler() != null) {
            getSpoutHandler().onSpoutDeactivate(this);
        }
    }

    /**
     * Called for a Tuple MessageId when the tuple has been fully processed.
     * @param id The tuple's message id.
     */
    @Override
    public void ack(Object id) {
        // Cast to appropriate object type
        final MessageId messageId = (MessageId) id;

        // Ack the tuple via the Message Bus
        getMessageBus().ack(messageId);

        // Update ack count metric for VirtualSpout this tuple originated from
        getMetricsRecorder().count(SpoutMetrics.VIRTUAL_SPOUT_ACK, messageId.getSrcVirtualSpoutId());
    }

    /**
     * Called for a Tuple MessageId when the tuple has failed during processing.
     * @param id The failed tuple's message id.
     */
    @Override
    public void fail(Object id) {
        // Cast to appropriate object type
        final MessageId messageId = (MessageId) id;

        logger.warn("Failed {}", messageId);

        // Fail the tuple via the MessageBus
        getMessageBus().fail(messageId);
    }

    /**
     * @return The Storm topology config map.
     */
    public AbstractConfig getSpoutConfig() {
        return spoutConfig;
    }

    /**
     * Utility method to get a specific entry in the Storm topology config map.
     * @param key The configuration item to retrieve
     * @return The configuration item's value.
     */
    private Object getSpoutConfigItem(final String key) {
        return getSpoutConfig().get(key);
    }

    /**
     * @return The Storm topology context.
     */
    private TopologyContext getTopologyContext() {
        return topologyContext;
    }

    /**
     * Add a new VirtualSpout to the coordinator, this will get picked up by the coordinator's monitor, opened and
     * managed with teh other currently running spouts.
     *
     * This method is blocking.
     *
     * @param spout New delegate spout
     * @throws SpoutAlreadyExistsException if a spout already exists with the same VirtualSpoutIdentifier.
     */
    public void addVirtualSpout(final DelegateSpout spout) throws SpoutAlreadyExistsException {
        checkSpoutOpened();
        getSpoutCoordinator().addVirtualSpout(spout);
    }

    /**
     * Remove a spout from the coordinator by it's identifier.
     * This method will block until the VirtualSpout has stopped.
     *
     * @param virtualSpoutIdentifier identifier of the spout to remove.
     * @throws SpoutDoesNotExistException If the VirtualSpoutIdentifier is not found.
     */
    public void removeVirtualSpout(final VirtualSpoutIdentifier virtualSpoutIdentifier) throws SpoutDoesNotExistException {
        checkSpoutOpened();

        // This method will block until the instance has stopped.
        getSpoutCoordinator().removeVirtualSpout(virtualSpoutIdentifier);

        logger.info("VirtualSpout {} is no longer running.", virtualSpoutIdentifier);
    }

    /**
     * Check if a given spout already exists in the spout coordinator.
     * @param spoutIdentifier spout identifier to check the coordinator for.
     * @return true when the spout exists, false when it does not.
     */
    public boolean hasVirtualSpout(final VirtualSpoutIdentifier spoutIdentifier) {
        checkSpoutOpened();
        return getSpoutCoordinator().hasVirtualSpout(spoutIdentifier);
    }

    /**
     * Get the total number of virtual spouts in the coordinator.
     *
     * This method crosses the thread barrier to determine its value.
     *
     * @return total number of virtual spouts in the coordinator.
     */
    public int getTotalVirtualSpouts() {
        return getSpoutCoordinator().getTotalSpouts();
    }

    /**
     * @return The spout's output collector.
     */
    private SpoutOutputCollector getOutputCollector() {
        return outputCollector;
    }

    /**
     * @return The factory manager instance.
     */
    public FactoryManager getFactoryManager() {
        // No need to check if the spout is opened because this is setup inside of the constructor
        return factoryManager;
    }

    /**
     * @return The spout's metrics recorder implementation.
     */
    public MetricsRecorder getMetricsRecorder() {
        checkSpoutOpened();
        return metricsRecorder;
    }

    /**
     * @return The virtual spout coordinator.
     */
    SpoutCoordinator getSpoutCoordinator() {
        checkSpoutOpened();
        return spoutCoordinator;
    }

    /**
     * @return The MessageBus for communicating with VirtualSpouts.
     */
    SpoutMessageBus getMessageBus() {
        checkSpoutOpened();
        return messageBus;
    }

    /**
     * @return The SpoutHandler implementation.
     */
    SpoutHandler getSpoutHandler() {
        checkSpoutOpened();
        return spoutHandler;
    }

    /**
     * @return The stream that tuples will be emitted out.
     */
    String getOutputStreamId() {
        if (outputStreamId == null) {
            if (spoutConfig == null) {
                throw new IllegalStateException("Missing required configuration! SpoutConfig not defined!");
            }
            outputStreamId = (String) getSpoutConfigItem(SpoutConfig.OUTPUT_STREAM_ID);
            if (Strings.isNullOrEmpty(outputStreamId)) {
                outputStreamId = Utils.DEFAULT_STREAM_ID;
            }
        }
        return outputStreamId;
    }

    /**
     * Check whether or not the spout has been opened. If it's not violently throw an exception!!!
     */
    private void checkSpoutOpened() {
        if (!isOpen) {
            throw new SpoutNotOpenedException();
        }
    }
}
