package com.salesforce.storm.spout.sideline;

import com.salesforce.storm.spout.sideline.consumer.ConsumerState;
import com.salesforce.storm.spout.sideline.consumer.Consumer;

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

    ConsumerState getStartingState();

    ConsumerState getEndingState();

    double getMaxLag();

    int getNumberOfFiltersApplied();

    Consumer getConsumer();
}
