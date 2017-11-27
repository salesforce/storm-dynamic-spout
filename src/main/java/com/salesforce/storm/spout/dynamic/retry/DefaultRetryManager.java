/**
 * Copyright (c) 2017, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 *   disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * * Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or promote products
 *   derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.salesforce.storm.spout.dynamic.retry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;

import java.time.Clock;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

/**
 * This Retry Manager implementation does 2 things.
 * It attempts retries of failed tuples a maximum of MAX_RETRIES times.
 * After a tuple fails more than that, it will be "acked" or marked as completed.
 * Each retry is attempted using an exponential back-off time period.
 * The first retry will be attempted within MIN_RETRY_TIME_MS milliseconds.  Each attempt
 * after that will be retried at (FAIL_COUNT * MIN_RETRY_TIME_MS) milliseconds.
 */
public class DefaultRetryManager implements RetryManager {
    /**
     * Define our retry limit.
     * A value of less than 0 will mean we'll retry forever
     * A value of 0 means we'll never retry.
     * A value of greater than 0 sets an upper bound of number of retries.
     */
    private int retryLimit = -1;

    // Initial delay after a tuple fails for the first time, in milliseconds.
    private long initialRetryDelayMs = 2000;

    // Each time we fail, double our delay, so 4, 8, 16 seconds, etc.
    private double retryDelayMultiplier = 2.0;

    // Maximum delay between successive retries, defaults to 15 minutes.
    private long retryDelayMaxMs = 900000;

    /**
     * This Set holds which tuples are in flight.
     */
    private Set<MessageId> retriesInFlight;

    /**
     * This map hows how many times each messageId has failed.
     */
    private Map<MessageId, Integer> numberOfTimesFailed;

    /**
     * This is a sorted Tree of timestamps, where each timestamp points to a queue of
     * failed messageIds.
     */
    private TreeMap<Long, Queue<MessageId>> failedMessageIds;

    /**
     * Used to control timing around retries.
     * Also allows us to inject a mock clock for testing.
     */
    private transient Clock clock = Clock.systemUTC();

    /**
     * Called to initialize this implementation.
     * @param spoutConfig used to pass in any configuration values.
     */
    public void open(final AbstractConfig spoutConfig) {
        // Load config options.
        if (spoutConfig.hasNonNullValue(SpoutConfig.RETRY_MANAGER_RETRY_LIMIT)) {
            retryLimit = spoutConfig.getInt(SpoutConfig.RETRY_MANAGER_RETRY_LIMIT);
        }
        if (spoutConfig.hasNonNullValue(SpoutConfig.RETRY_MANAGER_INITIAL_DELAY_MS)) {
            initialRetryDelayMs = spoutConfig.getLong(SpoutConfig.RETRY_MANAGER_INITIAL_DELAY_MS);
        }
        if (spoutConfig.hasNonNullValue(SpoutConfig.RETRY_MANAGER_DELAY_MULTIPLIER)) {
            retryDelayMultiplier = spoutConfig.getDouble(SpoutConfig.RETRY_MANAGER_DELAY_MULTIPLIER);
        }
        if (spoutConfig.hasNonNullValue(SpoutConfig.RETRY_MANAGER_MAX_DELAY_MS)) {
            retryDelayMaxMs = spoutConfig.getLong(SpoutConfig.RETRY_MANAGER_MAX_DELAY_MS);
        }

        // Init data structures.
        retriesInFlight = Sets.newHashSet();
        numberOfTimesFailed = Maps.newHashMap();
        failedMessageIds = Maps.newTreeMap();
    }

    /**
     * Mark a messageId as having failed and start tracking it.
     * @param messageId messageId to track as having failed.
     */
    @Override
    public void failed(MessageId messageId) {
        final int failCount = numberOfTimesFailed.getOrDefault(messageId, 0) + 1;
        numberOfTimesFailed.put(messageId, failCount);

        // Determine when we should retry this msg next
        // Calculate how many milliseconds to wait until the next retry
        long additionalTime = (long) (failCount * getInitialRetryDelayMs() * getRetryDelayMultiplier());
        if (additionalTime > getRetryDelayMaxMs()) {
            // If its over our configured max delay, use max delay
            additionalTime = getRetryDelayMaxMs();
        }
        // Calculate the timestamp for the retry.
        final long retryTime = getClock().millis() + additionalTime;

        // If we had previous fails
        if (failCount > 1) {
            // Make sure they're removed.  This kind of sucks.
            // This may not be needed in reality...just because of how we've setup our tests :/
            for (Queue queue: failedMessageIds.values()) {
                if (queue.remove(messageId)) {
                    break;
                }
            }
        }

        // Grab the queue for this timestamp,
        // If it doesn't exist, create a new queue and return it.
        Queue<MessageId> queue = failedMessageIds.computeIfAbsent(retryTime, k -> Lists.newLinkedList());

        // Add our message to the queue.
        queue.add(messageId);

        // Mark it as no longer in flight to be safe.
        retriesInFlight.remove(messageId);

        // Done!
    }

    @Override
    public void acked(MessageId messageId) {
        // Remove from in flight
        retriesInFlight.remove(messageId);

        // Remove fail count tracking
        numberOfTimesFailed.remove(messageId);
    }

    @Override
    public MessageId nextFailedMessageToRetry() {
        final long now = getClock().millis();
        final Long lowestKey = failedMessageIds.floorKey(now);
        if (lowestKey == null) {
            // Nothing has expired
            return null;
        }
        Queue<MessageId> queue = failedMessageIds.get(lowestKey);
        final MessageId messageId = queue.poll();

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
    public boolean retryFurther(MessageId messageId) {
        // If max retries is set to 0, we will never retry any tuple.
        if (getRetryLimit() == 0) {
            return false;
        }

        // If max retries is less than 0, we'll retry forever
        if (getRetryLimit() < 0) {
            return true;
        }

        // Find out how many times this tuple has failed previously.
        final int numberOfTimesHasFailed = numberOfTimesFailed.getOrDefault(messageId, 0);

        // If we have exceeded our max retry limit
        if (numberOfTimesHasFailed >= retryLimit) {
            // Then we shouldn't retry
            return false;
        }
        return true;
    }

    /**
     * @return max number of times we'll retry a failed tuple.
     */
    public int getRetryLimit() {
        return retryLimit;
    }

    /**
     * @return minimum time between retries, in milliseconds.
     */
    public long getInitialRetryDelayMs() {
        return initialRetryDelayMs;
    }

    /**
     * @return The configured retry delay multiplier.
     */
    public double getRetryDelayMultiplier() {
        return retryDelayMultiplier;
    }

    /**
     * @return The configured max delay time, in milliseconds.
     */
    public long getRetryDelayMaxMs() {
        return retryDelayMaxMs;
    }

    /**
     * Used internally and in tests.
     * @return returns all of the message Ids in flight / being processed by the topology currently.
     */
    Set<MessageId> getRetriesInFlight() {
        return retriesInFlight;
    }

    /**
     * Used internally and in tests.
     */
    Map<MessageId, Integer> getNumberOfTimesFailed() {
        return numberOfTimesFailed;
    }

    /**
     * Used internally and in tests.
     */
    TreeMap<Long, Queue<MessageId>> getFailedMessageIds() {
        return failedMessageIds;
    }

    /**
     * @return The configured clock implementation.  Useful for testing.
     */
    Clock getClock() {
        return clock;
    }

    /**
     * For injecting a clock implementation.  Useful for testing.
     * @param clock the clock implementation to use.
     */
    void setClock(Clock clock) {
        this.clock = clock;
    }
}
