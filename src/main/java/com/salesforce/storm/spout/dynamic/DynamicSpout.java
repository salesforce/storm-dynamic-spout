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
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.buffer.MessageBuffer;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.exception.SpoutNotOpenedException;
import com.salesforce.storm.spout.dynamic.handler.SpoutHandler;
import com.salesforce.storm.spout.dynamic.metrics.MetricsRecorder;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
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
     * The Spout configuration map.
     */
    private Map<String, Object> spoutConfig;

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
     * as handles routing emitted, acked, and failed tuples between this Spout instance
     * and the appropriate Virtual Spouts.
     */
    private SpoutCoordinator coordinator;

    /**
     * Manages creating implementation instances.
     */
    private final FactoryManager factoryManager;

    /**
     * Handler for callbacks at various stages of a dynamic spout's lifecycle.
     */
    private SpoutHandler spoutHandler;

    /**
     * For collecting metrics.
     */
    private transient MetricsRecorder metricsRecorder;
    private transient Map<VirtualSpoutIdentifier, Long> emitCountMetrics;
    private long emitCounter = 0L;

    /**
     * Determines which output stream to emit tuples out.
     * Gets set during open().
     */
    private String outputStreamId = null;

    /**
     * Determines which output stream to emit permanently failed tuples out.
     * Gets set during open().
     */
    private String permanentlyFailedOutputStreamId = null;

    /**
     * Whether or not the spout has been previously opened.
     */
    private boolean isOpen = false;

    /**
     * Constructor to create our spout.
     * @param spoutConfig Our configuration.
     */
    public DynamicSpout(Map<String, Object> spoutConfig) {
        // TODO: This method arguments may change to an actual SidelineSpoutConfig object instead of a generic map?
        // Save off config, injecting appropriate default values for anything not explicitly configured.
        this.spoutConfig = Collections.unmodifiableMap(SpoutConfig.setDefaults(spoutConfig));

        // Create our factory manager, which must be serializable.
        factoryManager = new FactoryManager(getSpoutConfig());
    }

    /**
     * Open is called once the spout instance has been deployed to the Storm cluster
     * and is ready to get to work.
     *
     * @param topologyConfig The Storm Topology configuration.
     * @param topologyContext The Storm Topology context.
     * @param spoutOutputCollector The output collector to emit tuples via.
     */
    @Override
    public void open(Map topologyConfig, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        if (isOpen) {
            logger.warn("This spout has already been opened, cowardly refusing to open it again!");
            return;
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

        // Initialize Metrics Collection
        metricsRecorder = getFactoryManager().createNewMetricsRecorder();
        metricsRecorder.open(getSpoutConfig(), getTopologyContext());

        // Create MessageBuffer
        final MessageBuffer messageBuffer = getFactoryManager().createNewMessageBufferInstance();
        messageBuffer.open(getSpoutConfig());

        // Create Spout Coordinator.
        coordinator = new SpoutCoordinator(
            // Our metrics recorder.
            metricsRecorder,
            // Our MessageBuffer/Queue Implementation.
            messageBuffer
        );

        // Call open on coordinator, avoiding getter
        coordinator.open(getSpoutConfig());

        // For emit metrics
        emitCountMetrics = Maps.newHashMap();

        // Our spout is open, it's not dependent upon the handler to finish opening for us to be 'opened'
        // This is important, because if we waited most of our getters that check the opened state of the
        // spout would throw an exception and make them unusable.
        isOpen = true;

        spoutHandler = getFactoryManager().createSpoutHandler();
        spoutHandler.open(spoutConfig);
        spoutHandler.onSpoutOpen(this, topologyConfig, topologyContext);
    }

    /**
     * Get the next tuple from the spout.
     */
    @Override
    public void nextTuple() {
        // Report any errors
        getCoordinator()
            .getErrors()
            .ifPresent(throwable -> getOutputCollector().reportError(throwable));

        // Ask the SpoutCoordinator for the next message that should be emitted. If it returns null, then there's
        // nothing new to emit! If a Message object is returned, it contains the appropriately mapped MessageId and
        // Values for the tuple that should be emitted.
        final Message message = getCoordinator().nextMessage();
        if (message == null) {
            // Nothing new to emit!
            return;
        }

        // If this is a permanently failed message.
        if (message.isPermanentlyFailed()) {
            // Emit tuple via the output collector down the failed stream.
            // Do not attach a messageId because this should be untracked.  We won't listen for
            // any acks, fails, for messages on this stream.
            getOutputCollector().emit(getPermanentlyFailedOutputStreamId(), message.getValues());
            return;
        }

        // Emit tuple via the output collector.
        // Attach the appropriate messageId so it can be tracked.
        getOutputCollector().emit(getOutputStreamId(), message.getValues(), message.getMessageId());

        // Update emit count metric for VirtualSpout this tuple originated from
        getMetricsRecorder().count(VirtualSpout.class, message.getMessageId().getSrcVirtualSpoutId() + ".emit", 1);

        // Everything below is temporary emit metrics for debugging.

        // Update / Display emit metrics
        final VirtualSpoutIdentifier srcId = message.getMessageId().getSrcVirtualSpoutId();
        if (!emitCountMetrics.containsKey(srcId)) {
            emitCountMetrics.put(srcId, 1L);
        } else {
            emitCountMetrics.put(srcId, emitCountMetrics.get(srcId) + 1L);
        }
        emitCounter++;
        if (emitCounter >= 5_000_000L) {
            for (Map.Entry<VirtualSpoutIdentifier, Long> entry : emitCountMetrics.entrySet()) {
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
    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        // Handles both explicitly defined and default stream definitions.
        final String streamId = getOutputStreamId();
        final String permanentlyFailedStreamId = getPermanentlyFailedOutputStreamId();

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

        // Declare a fail stream using the same fields.
        declarer.declareStream(permanentlyFailedStreamId, fields);
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

        // Close coordinator
        if (getCoordinator() != null) {
            getCoordinator().close();
            coordinator = null;
        }

        // Close metrics recorder.
        if (getMetricsRecorder() != null) {
            getMetricsRecorder().close();
            metricsRecorder = null;
        }

        if (spoutHandler != null) {
            spoutHandler.onSpoutClose(this);
            spoutHandler.close();
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
        if (spoutHandler != null) {
            spoutHandler.onSpoutActivate(this);
        }
    }

    /**
     * Currently a no-op.  We could make this un-pause things in the coordinator.
     */
    @Override
    public void deactivate() {
        logger.debug("Deactivate spout");
        if (spoutHandler != null) {
            spoutHandler.onSpoutDeactivate(this);
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

        // Ack the tuple via the coordinator
        getCoordinator().ack(messageId);

        // Update ack count metric for VirtualSpout this tuple originated from
        getMetricsRecorder().count(VirtualSpout.class, messageId.getSrcVirtualSpoutId() + ".ack", 1);
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

        // Fail the tuple via the coordinator
        getCoordinator().fail(messageId);
    }

    /**
     * @return The Storm topology config map.
     */
    public Map<String, Object> getSpoutConfig() {
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
     * Add a spout to the coordinator.
     * @param spout spout to add
     */
    public void addVirtualSpout(final DelegateSpout spout) {
        checkSpoutOpened();
        getCoordinator().addVirtualSpout(spout);
    }

    /**
     * Remove a spout from the coordinator by it's identifier.
     * @param virtualSpoutIdentifier identifier of the spout to remove.
     */
    public void removeVirtualSpout(final VirtualSpoutIdentifier virtualSpoutIdentifier) {
        checkSpoutOpened();
        getCoordinator().removeVirtualSpout(virtualSpoutIdentifier);
    }

    /**
     * Check if a given spout already exists in the spout coordinator.
     * @param spoutIdentifier spout identifier to check the coordinator for.
     * @return true when the spout exists, false when it does not.
     */
    public boolean hasVirtualSpout(final VirtualSpoutIdentifier spoutIdentifier) {
        return getCoordinator().hasVirtualSpout(spoutIdentifier);
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
    SpoutCoordinator getCoordinator() {
        checkSpoutOpened();
        return coordinator;
    }

    /**
     * @return The stream that tuples will be emitted out.
     */
    String getOutputStreamId() {
        if (outputStreamId == null) {
            if (spoutConfig == null) {
                throw new IllegalStateException("Missing required configuration!  SidelineSpoutConfig not defined!");
            }
            outputStreamId = (String) getSpoutConfigItem(SpoutConfig.OUTPUT_STREAM_ID);
            if (Strings.isNullOrEmpty(outputStreamId)) {
                outputStreamId = Utils.DEFAULT_STREAM_ID;
            }
        }
        return outputStreamId;
    }

    /**
     * @return The stream that tuples that have permanently failed will be emitted out.
     */
    String getPermanentlyFailedOutputStreamId() {
        if (permanentlyFailedOutputStreamId == null) {
            if (spoutConfig == null) {
                throw new IllegalStateException("Missing required configuration! SpoutConfig not defined!");
            }
            permanentlyFailedOutputStreamId = (String) getSpoutConfigItem(SpoutConfig.PERMANENTLY_FAILED_OUTPUT_STREAM_ID);
            if (Strings.isNullOrEmpty(permanentlyFailedOutputStreamId)) {
                permanentlyFailedOutputStreamId = "failed";
            }
        }
        return permanentlyFailedOutputStreamId;
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
