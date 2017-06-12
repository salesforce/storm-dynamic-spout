package com.salesforce.storm.spout.sideline.handler;

import com.salesforce.storm.spout.sideline.DynamicSpout;
import com.salesforce.storm.spout.sideline.FactoryManager;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

/**
 * Handlers (or callbacks) used by the DynamicSpout during it's lifecycle. Integrations can hook into the DynamicSpout
 * by creating a SpoutHanlder implementation.
 */
public interface SpoutHandler {

    /**
     * Open the handler.
     * @param spoutConfig Spout configuration.
     */
    default void open(Map spoutConfig) {

    }

    /**
     * Close the handler.
     */
    default void close() {

    }

    /**
     * Called when the DynamicSpout is opened.
     * @param spout DynamicSpout instance.
     * @param topologyConfig Topology configuration.
     * @param topologyContext Topology context.
     */
    default void onSpoutOpen(DynamicSpout spout, Map topologyConfig, TopologyContext topologyContext) {

    }

    /**
     * Called when the DynamicSpout is activated.
     */
    default void onSpoutActivate() {

    }

    /**
     * Called when the DynamicSpout is deactivated.
     */
    default void onSpoutDeactivate() {

    }

    /**
     * Called when the DynamicSpout is closed.
     */
    default void onSpoutClose() {

    }
}
