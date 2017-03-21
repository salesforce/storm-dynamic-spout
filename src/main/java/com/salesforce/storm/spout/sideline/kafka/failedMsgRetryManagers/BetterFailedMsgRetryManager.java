package com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

public class BetterFailedMsgRetryManager implements FailedMsgRetryManager {
    // Logging
    private static final Logger logger = LoggerFactory.getLogger(DefaultFailedMsgRetryManager.class);

    // Configuration
    private int maxRetries = 25;
    private long minRetryTimeMs = 1000;

    /**
     * This Set holds which Tuples are in flight.
     */
    private Set<TupleMessageId> retriesInFlight;

    /**
     * This map hows how many times each messageId has failed.
     */
    private Map<TupleMessageId, Integer> numberOfTimesFailed;

    /**
     * This is a sorted Tree of timestamps, where each timestamp points to a queue of
     * failed messageIds.
     */
    private TreeMap<Long, Queue<TupleMessageId>> failedMessageIds;

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

        // Init data structures.
        retriesInFlight = Sets.newHashSet();
        numberOfTimesFailed = Maps.newHashMap();
        failedMessageIds = Maps.newTreeMap();
    }

    /**
     * Mark a messageId as having failed and start tracking it.
     * @param messageId - messageId to track as having failed.
     */
    @Override
    public void failed(TupleMessageId messageId) {
        final int failCount = numberOfTimesFailed.getOrDefault(messageId, 0) + 1;
        numberOfTimesFailed.put(messageId, failCount);

        // Determine when we should retry this msg next
        final long retryTime = getClock().millis() + (minRetryTimeMs * failCount);

        // If we had previous fails
        if (failCount > 1) {
            // Make sure they're removed.  This kind of sucks.
            // This may not be needed in reality...just because of how we've setup our tests :/
            for (Long key: failedMessageIds.keySet()) {
                Queue queue = failedMessageIds.get(key);
                if (queue.remove(messageId)) {
                    break;
                }
            }
        }

        // Grab the queue for this timestamp,
        // If it doesn't exist, create a new queue and return it.
        Queue<TupleMessageId> queue = failedMessageIds.computeIfAbsent(retryTime, k -> Lists.newLinkedList());

        // Add our message to the queue.
        queue.add(messageId);

        // Mark it as no longer in flight to be safe.
        retriesInFlight.remove(messageId);

        // Done!
    }

    @Override
    public void acked(TupleMessageId messageId) {
        // Remove from in flight
        retriesInFlight.remove(messageId);

        // Remove fail count tracking
        numberOfTimesFailed.remove(messageId);
    }

    @Override
    public TupleMessageId nextFailedMessageToRetry() {
        final long now = getClock().millis();
        final Long lowestKey = failedMessageIds.floorKey(now);
        if (lowestKey == null) {
            // Nothing has expired
            return null;
        }
        Queue<TupleMessageId> queue = failedMessageIds.get(lowestKey);
        final TupleMessageId messageId = queue.poll();

        // If our queue is now empty
        if (queue.isEmpty()) {
            // remove it
            failedMessageIds.remove(lowestKey);
        }

        // Mark it as in flight.
        retriesInFlight.add(messageId);
        return messageId;
    }

    @Override
    public boolean retryFurther(TupleMessageId messageId) {
        // If max retries is set to 0, we will never retry any tuple.
        if (getMaxRetries() == 0) {
            return false;
        }

        // If we haven't tracked it yet
        final int numberOfTimesHasFailed = numberOfTimesFailed.getOrDefault(messageId, 0);

        // If we have exceeded our max retry limit
        if (numberOfTimesHasFailed >= maxRetries) {
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
     * @return - returns all of the message Ids in flight / being processed by the topology currently.
     */
    protected Set<TupleMessageId> getRetriesInFlight() {
        return retriesInFlight;
    }

    protected Map<TupleMessageId, Integer> getNumberOfTimesFailed() {
        return numberOfTimesFailed;
    }

    protected TreeMap<Long, Queue<TupleMessageId>> getFailedMessageIds() {
        return failedMessageIds;
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
}
