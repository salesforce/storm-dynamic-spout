package com.salesforce.storm.spout.sideline.handler;

import com.salesforce.storm.spout.sideline.DelegateSpout;
import com.salesforce.storm.spout.sideline.FactoryManager;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;

import java.util.Map;

public interface VirtualSpoutHandler {

    default void open(Map spoutConfig) {

    }

    default void close() {

    }

    default void onVirtualSpoutOpen(DelegateSpout virtualSpout) {

    }

    default void onVirtualSpoutClose(DelegateSpout virtualSpout) {

    }

    default void onVirtualSpoutCompletion(DelegateSpout virtualSpout) {

    }
}
