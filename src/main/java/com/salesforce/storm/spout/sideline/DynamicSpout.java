package com.salesforce.storm.spout.sideline;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.buffer.MessageBuffer;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * A base class for creating a virtualized spout, one spout implementation that is dynamically juggling multiple
 * spouts underneath it.  This is done in such a way that Storm does not have to be aware of all the contributing
 * spouts in the implementation.
 */
public abstract class DynamicSpout extends BaseRichSpout {

    // Logging
    private static final Logger logger = LoggerFactory.getLogger(DynamicSpout.class);

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
     * as handles routing emitted, acked, and failed tuples between this Spout instance
     * and the appropriate Virtual Spouts.
     */
    private SpoutCoordinator coordinator;

    /**
     * Manages creating implementation instances.
     */
    private final FactoryManager factoryManager;

    /**
     * Stores state from the spout
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
     * Constructor to create our spout.
     * @TODO this method arguments may change to an actual SidelineSpoutConfig object instead of a generic map?
     *
     * @param spoutConfig Our configuration.
     */
    public DynamicSpout(Map spoutConfig) {
        // Save off config, injecting appropriate default values for anything not explicitly configured.
        this.spoutConfig = Collections.unmodifiableMap(SidelineSpoutConfig.setDefaults(spoutConfig));

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

        // Create and open() persistence manager passing appropriate configuration.
        persistenceAdapter = getFactoryManager().createNewPersistenceAdapterInstance();
        getPersistenceAdapter().open(getSpoutConfig());

        // Create MessageBuffer
        final MessageBuffer messageBuffer = getFactoryManager().createNewMessageBufferInstance();
        messageBuffer.open(getSpoutConfig());

        // Create Spout Coordinator.
        coordinator = new SpoutCoordinator(
            // Our metrics recorder.
            getMetricsRecorder(),
            // Our MessageBuffer/Queue Implementation.
            messageBuffer
        );

        // Call open on coordinator.
        getCoordinator().open(getSpoutConfig());

        // For emit metrics
        emitCountMetrics = Maps.newHashMap();

        onOpen(topologyConfig, topologyContext, spoutOutputCollector);
    }

    /**
     * Called after open() this is essentially a callback or a hook to any spout implementing the virtual spout
     * framework.
     *
     * @param topologyConfig The Storm Topology configuration.
     * @param topologyContext The Storm Topology context.
     * @param spoutOutputCollector The output collector to emit tuples via.
     */
    abstract void onOpen(Map topologyConfig, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector);

    /**
     * Get the next tuple from the spout
     */
    @Override
    public void nextTuple() {
        /**
         * Ask the SpoutCoordinator for the next message that should be emitted.
         * If it returns null, then there's nothing new to emit!
         * If a Message object is returned, it contains the appropriately
         * mapped MessageId and Values for the tuple that should be emitted.
         */
        final Message message = getCoordinator().nextMessage();
        if (message == null) {
            // Nothing new to emit!
            return;
        }

        // Emit tuple via the output collector.
        getOutputCollector().emit(getOutputStreamId(), message.getValues(), message.getMessageId());

        // Update emit count metric for VirtualSpout this tuple originated from
        getMetricsRecorder().count(VirtualSpout.class, message.getMessageId().getSrcVirtualSpoutId() + ".emit", 1);

        // Everything below is temporary emit metrics for debugging.

        // Update / Display emit metrics
        final String srcId = message.getMessageId().getSrcVirtualSpoutId();
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

        // Close metrics recorder.
        if (getMetricsRecorder() != null) {
            getMetricsRecorder().close();
            metricsRecorder = null;
        }

        onClose();
    }

    /**
     * Called after close() this is essentially a callback or a hook to any spout implementing the virtual spout
     * framework.
     */
    abstract void onClose();

    /**
     * Currently a no-op.  We could make this pause things in the coordinator.
     */
    @Override
    public void activate() {
        logger.debug("Activating spout");
        onActivate();
    }

    /**
     * Called after activate() this is essentially a callback or a hook to any spout implementing the virtual spout
     * framework.
     */
    abstract void onActivate();

    /**
     * Currently a no-op.  We could make this un-pause things in the coordinator.
     */
    @Override
    public void deactivate() {
        logger.debug("Deactivate spout");
        onDeactivate();
    }

    /**
     * Called after deactivate() this is essentially a callback or a hook to any spout implementing the virtual spout
     * framework.
     */
    abstract void onDeactivate();

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
    protected Map getSpoutConfig() {
        return spoutConfig;
    }

    /**
     * Utility method to get a specific entry in the Storm topology config map.
     * @param key The configuration item to retrieve
     * @return The configuration item's value.
     */
    protected Object getSpoutConfigItem(final String key) {
        return getSpoutConfig().get(key);
    }

    /**
     * @return The Storm topology context.
     */
    protected TopologyContext getTopologyContext() {
        return topologyContext;
    }

    /**
     * Add a delegate spout to the coordinator
     * @param spout Delegate spout to add
     */
    protected void addVirtualSpout(DelegateSpout spout) {
        getCoordinator().addVirtualSpout(spout);
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
    FactoryManager getFactoryManager() {
        return factoryManager;
    }

    /**
     * @return The spout's metrics recorder implementation.
     */
    MetricsRecorder getMetricsRecorder() {
        return metricsRecorder;
    }

    /**
     * @return The virtual spout coordinator.
     */
    SpoutCoordinator getCoordinator() {
        return coordinator;
    }

    /**
     * @return The persistence manager.
     */
    PersistenceAdapter getPersistenceAdapter() {
        return persistenceAdapter;
    }

    /**
     * @return The stream that tuples will be emitted out.
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
     * @param id Id to add after the prefix
     * @return Generates VirtualSpoutId.
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
