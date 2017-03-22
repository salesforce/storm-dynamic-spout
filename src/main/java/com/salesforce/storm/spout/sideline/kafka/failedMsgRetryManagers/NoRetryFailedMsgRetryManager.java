package com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers;

import com.salesforce.storm.spout.sideline.TupleMessageId;

import java.util.Map;

/**
 * This implementation will never retry failed messages.  One and done.
 */
public class NoRetryFailedMsgRetryManager implements FailedMsgRetryManager {
    @Override
    public void open(Map stormConfig) {
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

    /**
     * @return - always null, never retry any messages.
     */
    @Override
    public TupleMessageId nextFailedMessageToRetry() {
        return null;
    }

    /**
     * @return Always return false.  Never want to replay messages.
     */
    @Override
    public boolean retryFurther(TupleMessageId messageId) {
        return false;
    }
}
