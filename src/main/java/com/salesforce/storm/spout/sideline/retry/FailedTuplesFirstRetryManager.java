package com.salesforce.storm.spout.sideline.retry;

import com.google.common.collect.Sets;
import com.salesforce.storm.spout.sideline.MessageId;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * This implementation will always retry failed tuples at the earliest chance it can.
 * No back-off strategy, no maximum times a tuple can fail.
 */
public class FailedTuplesFirstRetryManager implements RetryManager {
    /**
     * This Set holds which Tuples are in flight.
     */
    private Set<MessageId> messageIdsInFlight;

    /**
     * Our FIFO queue of failed messageIds.
     */
    private Queue<MessageId> failedMessageIds;

    @Override
    public void open(Map spoutConfig) {
        messageIdsInFlight = Sets.newHashSet();
        failedMessageIds = new LinkedList<>();
    }

    @Override
    public void failed(MessageId messageId) {
        messageIdsInFlight.remove(messageId);
        failedMessageIds.add(messageId);
    }

    @Override
    public void acked(MessageId messageId) {
        messageIdsInFlight.remove(messageId);
        failedMessageIds.remove(messageId);
    }

    @Override
    public MessageId nextFailedMessageToRetry() {
        final MessageId nextMessageId = failedMessageIds.poll();
        if (nextMessageId == null) {
            return null;
        }
        messageIdsInFlight.add(nextMessageId);
        return nextMessageId;
    }

    @Override
    public boolean retryFurther(MessageId messageId) {
        // We always retry.
        return true;
    }

    /**
     * @return - the messageIds currently in flight.
     */
    Set<MessageId> getMessageIdsInFlight() {
        return messageIdsInFlight;
    }

    /**
     * @return - the messageIds currently marked as having failed, excluding those in flight.
     */
    Queue<MessageId> getFailedMessageIds() {
        return failedMessageIds;
    }
}
