package com.salesforce.storm.spout.sideline.mocks;

import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.kafka.ConsumerState;
import com.salesforce.storm.spout.sideline.kafka.DelegateSidelineSpout;

import java.util.Queue;
import java.util.Set;
import java.util.UUID;

/**
 * A test mock.
 */
public class MockDelegateSidelineSpout implements DelegateSidelineSpout {
    private final String virtualSpoutId;
    public volatile boolean requestedStop = false;
    public volatile boolean wasOpenCalled = false;
    public volatile boolean wasCloseCalled = false;
    public volatile boolean flushStateCalled = false;
    public volatile RuntimeException exceptionToThrow = null;
    public volatile Set<TupleMessageId> failedTupleIds = Sets.newConcurrentHashSet();
    public volatile Set<TupleMessageId> ackedTupleIds = Sets.newConcurrentHashSet();

    public volatile Queue<KafkaMessage> emitQueue = Queues.newConcurrentLinkedQueue();

    public MockDelegateSidelineSpout() {
        this.virtualSpoutId = this.getClass().getSimpleName() + UUID.randomUUID().toString();
    }

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
        return emitQueue.poll();
    }

    @Override
    public void ack(Object id) {
        ackedTupleIds.add((TupleMessageId) id);
    }

    @Override
    public void fail(Object id) {
        failedTupleIds.add((TupleMessageId) id);
    }

    @Override
    public String getVirtualSpoutId() {
        return virtualSpoutId;
    }

    @Override
    public void flushState() {
        flushStateCalled = true;
    }

    @Override
    public synchronized void requestStop() {
        requestedStop = true;
    }

    @Override
    public synchronized boolean isStopRequested() {
        return requestedStop;
    }

    @Override
    public ConsumerState getCurrentState() {
        return ConsumerState.builder().build();
    }

    @Override
    public double getMaxLag() {
        return 0;
    }

    @Override
    public int getNumberOfFiltersApplied() {
        return 0;
    }
}
