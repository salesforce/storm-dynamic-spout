package com.salesforce.storm.spout.sideline.mocks;

import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.sideline.Message;
import com.salesforce.storm.spout.sideline.MessageId;
import com.salesforce.storm.spout.sideline.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.kafka.ConsumerState;
import com.salesforce.storm.spout.sideline.DelegateSpout;

import java.util.Queue;
import java.util.Set;
import java.util.UUID;

/**
 * A test mock.
 */
public class MockDelegateSpout implements DelegateSpout {
    private final VirtualSpoutIdentifier virtualSpoutId;
    public volatile boolean requestedStop = false;
    public volatile boolean wasOpenCalled = false;
    public volatile boolean wasCloseCalled = false;
    public volatile boolean flushStateCalled = false;
    public volatile RuntimeException exceptionToThrow = null;
    public volatile Set<MessageId> failedTupleIds = Sets.newConcurrentHashSet();
    public volatile Set<MessageId> ackedTupleIds = Sets.newConcurrentHashSet();

    public volatile Queue<Message> emitQueue = Queues.newConcurrentLinkedQueue();

    public MockDelegateSpout() {
        this.virtualSpoutId = new VirtualSpoutIdentifier(this.getClass().getSimpleName() + UUID.randomUUID().toString());
    }

    public MockDelegateSpout(final VirtualSpoutIdentifier virtualSpoutId) {
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
    public Message nextTuple() {
        if (exceptionToThrow != null) {
            throw exceptionToThrow;
        }
        return emitQueue.poll();
    }

    @Override
    public void ack(Object id) {
        ackedTupleIds.add((MessageId) id);
    }

    @Override
    public void fail(Object id) {
        failedTupleIds.add((MessageId) id);
    }

    @Override
    public VirtualSpoutIdentifier getVirtualSpoutId() {
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
