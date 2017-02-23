package com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers;

import com.salesforce.storm.spout.sideline.TupleMessageId;

import java.util.Map;
import java.util.Set;

/**
 * Never retry failed messages.
 */
public class NoRetryFailedMsgRetryManager implements FailedMsgRetryManager {
    @Override
    public void prepare(Map stormConfig) {
        // Nothing to do
    }

    @Override
    public void failed(TupleMessageId messageId) {
        // Nothing to do
    }

    @Override
    public void acked(TupleMessageId messageId) {
        // Nothing to do
    }

    @Override
    public void retryStarted(TupleMessageId messageId) {
        // Nothing to do
    }

    @Override
    public TupleMessageId nextFailedMessageToRetry() {
        return null;
    }

    @Override
    public boolean shouldReEmitMsg(TupleMessageId messageId) {
        // Never re-emit
        return false;
    }

    @Override
    public boolean retryFurther(Long offset) {
        return false;
    }

    @Override
    public Set<TupleMessageId> clearOffsetsBefore(TupleMessageId messageId) {
        return null;
    }
}
