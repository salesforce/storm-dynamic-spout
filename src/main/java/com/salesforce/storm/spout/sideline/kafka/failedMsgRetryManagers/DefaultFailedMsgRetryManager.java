package com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import org.apache.storm.shade.org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This Retry Manager implementation does 2 things.
 * It attempts retries of failed tuples a maximum of MAX_RETRIES times.
 * After a tuple fails more than that, it will be "ack'd" or marked as completed.
 * Each retry is attempted using an exponential backoff time period.
 * The first retry will be attempted within MIN_RETRY_TIME_MS milliseconds.  Each attempt
 * after that will be retried at (FAIL_COUNT * MIN_RETRY_TIME_MS) milliseconds.
 *
 * Note: Super naive implementation of a retry manager.
 * @TODO: Refactor this to use more efficient sorted data structures.
 */
public class DefaultFailedMsgRetryManager implements FailedMsgRetryManager {
    // Logging
    private static final Logger logger = LoggerFactory.getLogger(DefaultFailedMsgRetryManager.class);

    // Configuration
    private final int maxRetries;
    private final long minRetryTimeMs;

    /**
     * Where we track failed messageIds.
     */
    private Map<TupleMessageId, FailedMessage> failedTuples;
    private Set<TupleMessageId> retriesInFlight;

    /**
     * Defaults, attempt a max of 20 retries, with a minimum delay of 1 second.
     */
    public DefaultFailedMsgRetryManager() {
        this(20, TimeUnit.SECONDS.toMillis(1));
    }

    /**
     * Configure the parameters of this manager.
     * @param maxRetries - Maximum number of retries to attempt on a failed tuple.
     * @param minRetryTimeMs - minimum time between retries.
     */
    public DefaultFailedMsgRetryManager(int maxRetries, long minRetryTimeMs) {
        this.maxRetries = maxRetries;
        this.minRetryTimeMs = minRetryTimeMs;
    }

    /**
     * Called to initialize this implementation.
     * @param stormConfig - not used, at least for now.
     */
    @Override
    public void prepare(Map stormConfig) {
        failedTuples = Maps.newHashMap();
        retriesInFlight = Sets.newHashSet();
    }

    /**
     * Mark a messageId as having failed and start tracking it.
     * @param messageId - messageId to track as having failed.
     */
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
            logger.info("{} <= {} for {}", failedTuples.get(messageId).getNextRetry(), now, messageId);
            if (failedTuples.get(messageId).getNextRetry() <= now && !retriesInFlight.contains(messageId)) {
                // return msg id.
                return messageId;
            }
        }
        return null;
    }

    @Override
    public boolean retryFurther(TupleMessageId messageId) {
        // If max retries is set to 0, we will never retry any tuple.
        if (getMaxRetries() == 0) {
           return false;
        }

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

    /**
     * @return - max number of times we'll retry a failed tuple.
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * @return - minimum time between retries, in milliseconds.
     */
    public long getMinRetryTimeMs() {
        return minRetryTimeMs;
    }

    /**
     * Used internally and in tests.
     * @return - returns all the failed tuple message Ids that are being tracked.
     */
    protected Map<TupleMessageId, FailedMessage> getFailedTuples() {
        return failedTuples;
    }

    /**
     * Used internally and in tests.
     * @return - returns all of the message Ids in flight / being processed by the topology currently.
     */
    protected Set<TupleMessageId> getRetriesInFlight() {
        return retriesInFlight;
    }

    /**
     * Internal helper class.
     */
    protected static class FailedMessage {
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
