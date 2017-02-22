package com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import org.apache.storm.shade.org.joda.time.DateTime;

import java.util.Map;
import java.util.Set;

/**
 * Super naive implementation of a retry manager.
 */
public class DefaultFailedMsgRetryManager implements FailedMsgRetryManager {

    // Configuration
    private final int maxRetries = 50;
    private final long minRetryTimeMs = 1000;

    /**
     * Where we track failed messageIds.
     */
    private Map<TupleMessageId, FailedMessage> failedTuples;
    private Set<TupleMessageId> retriesInFlight;

    @Override
    public void prepare(Map stormConfig) {
        failedTuples = Maps.newHashMap();
        retriesInFlight = Sets.newHashSet();
    }

    @Override
    public void failed(TupleMessageId messageId) {
        // If we haven't tracked it yet
        if (!failedTuples.containsKey(messageId)) {
            // Track new failed message.
            failedTuples.put(messageId, new FailedMessage(1, DateTime.now().getMillis() + minRetryTimeMs));

            // Mark it as no longer in flight to be safe.
            retriesInFlight.remove(messageId);

            // Done!
            return;
        }

        // If its already tracked, we should increment some stuff
        final FailedMessage previousEntry = failedTuples.get(messageId);
        final int newFailCount = previousEntry.getFailCount() + 1;
        final long newRetry = DateTime.now().getMillis() + (minRetryTimeMs * newFailCount);

        // Update entry.
        failedTuples.put(messageId, new FailedMessage(newFailCount, newRetry));

        // Mark it as no longer in flight
        retriesInFlight.remove(messageId);
    }

    @Override
    public void acked(TupleMessageId messageId) {
        // Remove it
        failedTuples.remove(messageId);

        // Remove from in flight
        retriesInFlight.remove(messageId);
    }

    @Override
    public void retryStarted(TupleMessageId messageId) {
        retriesInFlight.add(messageId);
    }

    @Override
    public TupleMessageId nextFailedMessageToRetry() {
        // Naive implementation for now.  We should use a sorted set to remove the need to
        // search for the next entry.

        // Get now timestamp
        final long now = DateTime.now().getMillis();

        // Loop thru fails
        for (TupleMessageId messageId : failedTuples.keySet()) {
            // If its expired and not already in flight
            if (failedTuples.get(messageId).getNextRetry() > now && !retriesInFlight.contains(messageId)) {
                // return msg id.
                return messageId;
            }
        }
        return null;
    }

    @Override
    public boolean shouldReEmitMsg(TupleMessageId messageId) {
        // If we haven't tracked it yet
        if (!failedTuples.containsKey(messageId)) {
            // Then we should.
            return true;
        }

        // If we have exceeded our max retry limit
        final FailedMessage failedMessage = failedTuples.get(messageId);
        if (failedMessage.getFailCount() >= maxRetries) {
            // Then we shouldn't retry
            return false;
        }
        return true;
    }

    @Override
    public boolean retryFurther(Long offset) {
        // Not implemented.  Not needed in terface?
        return false;
    }

    @Override
    public Set<TupleMessageId> clearOffsetsBefore(TupleMessageId messageId) {
        // Not implemented.
        return null;
    }

    /**
     * Internal helper class.
     */
    private static class FailedMessage {
        private final int failCount;
        private final long nextRetry;

        public FailedMessage(int failCount, long nextRetry) {
            this.failCount = failCount;
            this.nextRetry = nextRetry;
        }

        public int getFailCount() {
            return failCount;
        }

        public long getNextRetry() {
            return nextRetry;
        }
    }
}
