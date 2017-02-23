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

    /**
     * @param messageId
     * @return Always return false.
     */
    @Override
    public boolean retryFurther(TupleMessageId messageId) {
        return false;
    }
}