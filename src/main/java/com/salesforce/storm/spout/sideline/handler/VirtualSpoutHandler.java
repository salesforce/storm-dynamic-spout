package com.salesforce.storm.spout.sideline.handler;

import com.salesforce.storm.spout.sideline.DelegateSpout;
import com.salesforce.storm.spout.sideline.FactoryManager;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;

import java.util.Map;

/**
 * Handlers (or callbacks) used by the VirtualSpout during it's lifecycle. Integrations can hook into the VirtualSpout
 * by creating a VirtualSpoutHandler implementation.
 */
public interface VirtualSpoutHandler {

    /**
     * Open the handler.
     * @param spoutConfig Spout configuration.
     */
    default void open(Map<String, Object> spoutConfig) {

    }

    /**
     * Close the handler.
     */
    default void close() {

    }

    /**
     * Called when the VirtualSpout is opened.
     * @param virtualSpout VirtualSpout instance.
     */
    default void onVirtualSpoutOpen(DelegateSpout virtualSpout) {

    }

    /**
     * Called when the VirtualSpout is activated.
     * @param virtualSpout VirtualSpout instance.
     */
    default void onVirtualSpoutClose(DelegateSpout virtualSpout) {

    }

    /**
     * Called when the VirtualSpout is completed, this happens before the call to onVirtualSpoutClose(), but only when
     * the spout is closing and has reached it's endingOffset.
     * @param virtualSpout VirtualSpout instance.
     */
    default void onVirtualSpoutCompletion(DelegateSpout virtualSpout) {

    }
}
