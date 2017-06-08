package com.salesforce.storm.spout.sideline.handler;

import com.salesforce.storm.spout.sideline.FactoryManager;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;

import java.util.Map;

public interface VirtualSpoutHandler {

    default void open(Map config, FactoryManager factoryManager, MetricsRecorder metricsRecorder) {

    }

    default void close() {

    }

    default void onVirtualSpoutOpen() {

    }

    default void onVirtualSpoutClose() {

    }
}
