package com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers;

import com.google.common.collect.Sets;
import com.salesforce.storm.spout.sideline.TupleMessageId;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * This implementation will always retry failed tuples at the earliest chance it can.
 * No back-off strategy, no maximum times a tuple can fail.
 */
public class RetryFailedTuplesFirstRetryManager implements FailedMsgRetryManager {
    /**
     * This Set holds which Tuples are in flight.
     */
    private Set<TupleMessageId> messageIdsInFlight;

    /**
     * Our FIFO queue of failed messageIds.
     */
    private Queue<TupleMessageId> failedMessageIds;

    @Override
    public void open(Map stormConfig) {
        messageIdsInFlight = Sets.newHashSet();
        failedMessageIds = new LinkedList<>();
    }

    @Override
    public void failed(TupleMessageId messageId) {
        messageIdsInFlight.remove(messageId);
        failedMessageIds.add(messageId);
    }

    @Override
    public void acked(TupleMessageId messageId) {
        messageIdsInFlight.remove(messageId);
        failedMessageIds.remove(messageId);
    }

    @Override
    public TupleMessageId nextFailedMessageToRetry() {
        final TupleMessageId nextMessageId = failedMessageIds.poll();
        if (nextMessageId == null) {
            return null;
        }
        messageIdsInFlight.add(nextMessageId);
        return nextMessageId;
    }

    @Override
    public boolean retryFurther(TupleMessageId messageId) {
        // We always retry.
        return true;
    }
}
