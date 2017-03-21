package com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

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
    private int maxRetries = 25;
    private long minRetryTimeMs = 1000;

    /**
     * Where we track failed messageIds.
     */
    private Map<TupleMessageId, FailedMessage> failedTuples;
    private Set<TupleMessageId> retriesInFlight;

    /**
     * Used to control timing around retries.
     * Also allows us to inject a mock clock for testing.
     */
    private transient Clock clock = Clock.systemUTC();

    /**
     * Called to initialize this implementation.
     * @param stormConfig - not used, at least for now.
     */
    public void open(Map stormConfig) {
        // Load config options.
        if (stormConfig.containsKey(SidelineSpoutConfig.FAILED_MSG_RETRY_MANAGER_MAX_RETRIES)) {
            maxRetries = (int) stormConfig.get(SidelineSpoutConfig.FAILED_MSG_RETRY_MANAGER_MAX_RETRIES);
        }
        if (stormConfig.containsKey(SidelineSpoutConfig.FAILED_MSG_RETRY_MANAGER_MIN_RETRY_TIME_MS)) {
            minRetryTimeMs = (long) stormConfig.get(SidelineSpoutConfig.FAILED_MSG_RETRY_MANAGER_MIN_RETRY_TIME_MS);
        }

        // Init datastructures.
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
            // Determine when we should retry this msg
            final long retryTime = getClock().millis() + minRetryTimeMs;

            // Track new failed message.
            failedTuples.put(messageId, new FailedMessage(1, retryTime));

            // Mark it as no longer in flight to be safe.
            retriesInFlight.remove(messageId);

            // Done!
            return;
        }

        // If its already tracked, we should increment some stuff
        final FailedMessage previousEntry = failedTuples.get(messageId);
        final int newFailCount = previousEntry.getFailCount() + 1;
        final long newRetry = getClock().millis() + (minRetryTimeMs * newFailCount);

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
        final long now = getClock().millis();

        // Loop thru fails
        for (TupleMessageId messageId : failedTuples.keySet()) {
            // If its expired and not already in flight
            if (!retriesInFlight.contains(messageId) && failedTuples.get(messageId).getNextRetry() <= now ) {
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
     * @return - return our clock implementation.  Useful for testing.
     */
    protected Clock getClock() {
        return clock;
    }

    /**
     * For injecting a clock implementation.  Useful for testing.
     * @param clock - the clock implementation to use.
     */
    protected void setClock(Clock clock) {
        this.clock = clock;
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
