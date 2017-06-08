package com.salesforce.storm.spout.sideline.handler;

import com.salesforce.storm.spout.sideline.DynamicSpout;
import com.salesforce.storm.spout.sideline.FactoryManager;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

public interface SpoutHandler {

    default void open(Map spoutConfig) {

    }

    default void close() {

    }

    default void onSpoutOpen(DynamicSpout spout, Map topologyConfig, TopologyContext topologyContext) {

    }

    default void onSpoutActivate() {

    }

    default void onSpoutDeactivate() {

    }

    default void onSpoutClose() {

    }
}
