package com.salesforce.storm.spout.sideline;

import com.salesforce.storm.spout.sideline.consumer.ConsumerState;

public interface DelegateSpout {

    void open();

    void close();

    Message nextTuple();

    void ack(Object msgId);

    void fail(Object msgId);

    VirtualSpoutIdentifier getVirtualSpoutId();

    void flushState();

    void requestStop();

    boolean isStopRequested();

    ConsumerState getCurrentState();

    double getMaxLag();

    int getNumberOfFiltersApplied();
}
