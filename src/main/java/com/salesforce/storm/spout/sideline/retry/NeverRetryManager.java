package com.salesforce.storm.spout.sideline.retry;

import com.salesforce.storm.spout.sideline.MessageId;

import java.util.Map;

/**
 * This implementation will never retry failed messages.  One and done.
 */
public class NeverRetryManager implements RetryManager {
    @Override
    public void open(Map spoutConfig) {
        // Nothing to do
    }

    @Override
    public void failed(MessageId messageId) {
        // Nothing to do
    }

    @Override
    public void acked(MessageId messageId) {
        // Nothing to do
    }

    /**
     * @return - always null, never retry any messages.
     */
    @Override
    public MessageId nextFailedMessageToRetry() {
        return null;
    }

    /**
     * @return Always return false.  Never want to replay messages.
     */
    @Override
    public boolean retryFurther(MessageId messageId) {
        return false;
    }
}
