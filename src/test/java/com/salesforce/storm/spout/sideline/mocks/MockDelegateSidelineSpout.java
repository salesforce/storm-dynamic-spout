package com.salesforce.storm.spout.sideline.mocks;

import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;

/**
 * A test mock.
 */
public class MockDelegateSidelineSpout implements DelegateSidelineSpout {
    final String virtualSpoutId;
    public volatile boolean requestedStop = false;
    public volatile boolean wasOpenCalled = false;
    public volatile boolean wasCloseCalled = false;
    public volatile RuntimeException exceptionToThrow = null;

    public MockDelegateSidelineSpout(final String virtualSpoutId) {
        this.virtualSpoutId = virtualSpoutId;
    }

    @Override
    public void open() {
        wasOpenCalled = true;
    }

    @Override
    public void close() {
        wasCloseCalled = true;
    }

    @Override
    public KafkaMessage nextTuple() {
        if (exceptionToThrow != null) {
            throw exceptionToThrow;
        }
        return null;
    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public String getVirtualSpoutId() {
        return virtualSpoutId;
    }

    @Override
    public void flushState() {

    }

    @Override
    public synchronized void requestStop() {
        requestedStop = true;
    }

    @Override
    public synchronized boolean isStopRequested() {
        return requestedStop;
    }
}
