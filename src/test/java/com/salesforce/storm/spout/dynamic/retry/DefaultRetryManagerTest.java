/*
 * Copyright (c) 2017, 2018, Salesforce.com, Inc.
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

import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test that the {@link DefaultRetryManager} retries tuples.
 */
public class DefaultRetryManagerTest {
    /**
     * Used to mock the system clock.
     */
    private static final long FIXED_TIME = 100000L;
    private Clock mockClock;

    /**
     * Handles mocking Clock using Java 1.8's Clock interface.
     */
    @BeforeEach
    public void setup() {
        // Set our clock to be fixed.
        mockClock = Clock.fixed(Instant.ofEpochMilli(FIXED_TIME), ZoneId.of("UTC"));
    }

    /**
     * Tests that the open() set properties from the config.
     */
    @Test
    public void testOpen() {
        final int expectedMaxRetries = 44;
        final long expectedMinRetryTimeMs = 4455;
        final double expectedDelayMultiplier = 4.56;
        final long expectedMaxDelayMs = 1000;

        // Build config.
        Map<String, Object> stormConfig = getDefaultConfig(
            expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, expectedMaxDelayMs
        );

        // Create instance and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.open(stormConfig);

        assertEquals(expectedMaxRetries, retryManager.getRetryLimit(), "Wrong max retries");
        assertEquals(expectedMinRetryTimeMs, retryManager.getInitialRetryDelayMs(), "Wrong retry time");
        assertEquals(expectedDelayMultiplier, retryManager.getRetryDelayMultiplier(), 0.001, "Wrong retry time");
        assertEquals(expectedMaxDelayMs, retryManager.getRetryDelayMaxMs(), "Wrong retry time");
    }

    /**
     * Tests that the open() uses default values if not configured.
     */
    @Test
    public void testOpenWithNoConfigUsesDefaults() {
        // Build config.
        Map<String, Object> stormConfig = getDefaultConfig(null, null, null, null);

        // Create instance and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.open(stormConfig);

        assertEquals(-1, retryManager.getRetryLimit(), "Wrong max retries");
        assertEquals(2000, retryManager.getInitialRetryDelayMs(), "Wrong retry time");
        assertEquals(15 * 60 * 1000, retryManager.getRetryDelayMaxMs(), "Wrong max delay");
        assertEquals(2.0, retryManager.getRetryDelayMultiplier(), 0.01, "Wrong delay multiplier");
    }

    /**
     * Tests tracking a new failed messageIds.
     */
    @Test
    public void testFailedSimpleCase() {
        // construct manager
        final int expectedMaxRetries = 10;
        final long expectedMinRetryTimeMs = 1000;

        // Use a wacky multiplier, because why not?
        final double expectedDelayMultiplier = 44.5;

        // Build config.
        Map<String, Object> stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        // Calculate the 1st retry times
        final long firstRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 0));

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId1 = new MessageId("MyTopic", 0, 101L, consumerId);
        final MessageId messageId2 = new MessageId("MyTopic", 0, 102L, consumerId);
        final MessageId messageId3 = new MessageId("MyTopic", 0, 103L, consumerId);

        // Mark first as having failed
        retryManager.failed(messageId1);

        // Validate it has failed
        validateExpectedFailedMessageId(retryManager, messageId1, 1, firstRetryTime, false);

        // Mark second as having failed
        retryManager.failed(messageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, messageId1, 1, firstRetryTime, false);
        validateExpectedFailedMessageId(retryManager, messageId2, 1, firstRetryTime, false);

        // Mark 3rd as having failed
        retryManager.failed(messageId3);

        // Validate it has all three as failed
        validateExpectedFailedMessageId(retryManager, messageId1, 1, firstRetryTime, false);
        validateExpectedFailedMessageId(retryManager, messageId2, 1, firstRetryTime, false);
        validateExpectedFailedMessageId(retryManager, messageId3, 1, firstRetryTime, false);
    }

    /**
     * Tests tracking a new failed messageId.
     */
    @Test
    public void testFailedMultipleFails() {
        // construct manager
        final int expectedMaxRetries = 10;
        final long expectedMinRetryTimeMs = 1000;

        // Use a wacky multiplier, because why not?
        final double expectedDelayMultiplier = 7.25;

        // Build config.
        Map<String, Object> stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId1 = new MessageId("MyTopic", 0, 101L, consumerId);
        final MessageId messageId2 = new MessageId("MyTopic", 0, 102L, consumerId);

        // Calculate the 1st, 2nd, and 3rd fail retry times
        final long firstRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 0));
        final long secondRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 1));
        final long thirdRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 2));

        // Mark first as having failed
        retryManager.failed(messageId1);

        // Validate it has failed
        validateExpectedFailedMessageId(retryManager, messageId1, 1, firstRetryTime, false);

        // Mark second as having failed
        retryManager.failed(messageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, messageId1, 1, firstRetryTime, false);
        validateExpectedFailedMessageId(retryManager, messageId2, 1, firstRetryTime, false);

        // Now fail messageId1 a second time.
        retryManager.failed(messageId1);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, messageId1, 2, secondRetryTime, false);
        validateExpectedFailedMessageId(retryManager, messageId2, 1, firstRetryTime, false);

        // Now fail messageId1 a 3rd time.
        retryManager.failed(messageId1);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, messageId1, 3, thirdRetryTime, false);
        validateExpectedFailedMessageId(retryManager, messageId2, 1, firstRetryTime, false);

        // Now fail messageId2 a 2nd time.
        retryManager.failed(messageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, messageId1, 3, thirdRetryTime, false);
        validateExpectedFailedMessageId(retryManager, messageId2, 2, secondRetryTime, false);
    }

    /**
     * Tests what happens if a tuple fails more than our max fail limit.
     */
    @Test
    public void testRetryDelayWhenExceedsMaxTimeDelaySetting() {
        // construct manager
        final int expectedMaxRetries = 10;
        final long expectedMinRetryTimeMs = 1000;

        // Use a multiplier of 10
        final double expectedDelayMultiplier = 10;

        // Set a max delay of 12 seconds
        final long expectedMaxDelay = 12000;

        // Build config.
        Map<String, Object> stormConfig = getDefaultConfig(
            expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, expectedMaxDelay
        );

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId1 = new MessageId("MyTopic", 0, 101L, consumerId);

        // Calculate the 1st, 2nd, and 3rd fail retry times
        final long firstRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 0)); // 1 second
        final long secondRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 1)); // 10 seconds
        final long thirdRetryTime = FIXED_TIME + expectedMaxDelay;

        // Mark first as having failed
        retryManager.failed(messageId1);

        // Validate it has failed
        validateExpectedFailedMessageId(retryManager, messageId1, 1, firstRetryTime, false);

        // Now fail messageId1 a second time.
        retryManager.failed(messageId1);

        // Validate it incremented our delay time, still below configured max delay
        validateExpectedFailedMessageId(retryManager, messageId1, 2, secondRetryTime, false);

        // Now fail messageId1 a 3rd time.
        retryManager.failed(messageId1);

        // Validate its pinned at configured max delay
        validateExpectedFailedMessageId(retryManager, messageId1, 3, thirdRetryTime, false);

        // Now fail messageId1 a 4th time.
        retryManager.failed(messageId1);

        // Validate its still pinned at configured max delay
        validateExpectedFailedMessageId(retryManager, messageId1, 4, thirdRetryTime, false);
    }


    /**
     * Tests that all previously un-tracked messageIds should be retried.
     */
    @Test
    public void testRetryFurtherForUntrackedMessageId() {
        // construct manager
        final int expectedMaxRetries = 2;
        final long expectedMinRetryTimeMs = 1000;

        // Build config.
        Map<String, Object> stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, null, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId = new MessageId("MyTopic", 0, 100L, consumerId);

        assertTrue(retryManager.retryFurther(messageId), "Should always be true because its untracked");
        assertTrue(retryManager.retryFurther(messageId), "Should always be true because its untracked");
        assertTrue(retryManager.retryFurther(messageId), "Should always be true because its untracked");
    }

    /**
     * Tests retryFurther always returns false if maxRetries is configured to 0.
     */
    @Test
    public void testRetryFurtherForUntrackedMessageIdWithMaxRetriesSetToZero() {
        // construct manager
        final int expectedMaxRetries = 0;
        final long expectedMinRetryTimeMs = 1000;

        // Build config.
        Map<String, Object> stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, null, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId = new MessageId("MyTopic", 0, 100L, consumerId);

        for (int x = 0; x < 100; x++) {
            assertFalse(retryManager.retryFurther(messageId), "Should always be false because we are configured to never retry");
        }
    }

    /**
     * Tests retryFurther always returns true if maxRetries is configured to a value less than 0.
     */
    @Test
    public void testRetryFurtherWithMaxRetriesSetNegative() {
        // construct manager
        final int expectedMaxRetries = -1;
        final long expectedMinRetryTimeMs = 1000;

        // Build config.
        Map<String, Object> stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, null, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId = new MessageId("MyTopic", 0, 100L, consumerId);

        for (int x = 0;  x < 100; x++) {
            // Fail tuple
            retryManager.failed(messageId);

            // See if we should retry
            assertTrue(retryManager.retryFurther(messageId), "Should always be true because we are configured to always retry");
        }
    }

    /**
     * Tests what happens if a tuple fails more than our max fail limit.
     */
    @Test
    public void testRetryFurtherWhenMessageExceedsRetryLimit() {
        // construct manager
        final int expectedMaxRetries = 3;
        final long expectedMinRetryTimeMs = 1000;

        // Use a wacky multiplier, because why not?
        final double expectedDelayMultiplier = 1.5;

        // Build config.
        Map<String, Object> stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId1 = new MessageId("MyTopic", 0, 101L, consumerId);
        final MessageId messageId2 = new MessageId("MyTopic", 0, 102L, consumerId);

        // Calculate the 1st, 2nd, and 3rd fail retry times
        final long firstRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 0));
        final long secondRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 1));
        final long thirdRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 2));

        // Mark first as having failed
        retryManager.failed(messageId1);

        // Validate it has failed
        validateExpectedFailedMessageId(retryManager, messageId1, 1, firstRetryTime, false);

        // Mark second as having failed
        retryManager.failed(messageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, messageId1, 1, firstRetryTime, false);
        validateExpectedFailedMessageId(retryManager, messageId2, 1, firstRetryTime, false);

        // Validate that we can retry both of these
        assertTrue(retryManager.retryFurther(messageId1), "Should be able to retry");
        assertTrue(retryManager.retryFurther(messageId2), "Should be able to retry");

        // Fail messageId1 a 2nd time
        retryManager.failed(messageId1);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, messageId1, 2, secondRetryTime, false);
        validateExpectedFailedMessageId(retryManager, messageId2, 1, firstRetryTime, false);

        // Validate that we can retry both of these
        assertTrue(retryManager.retryFurther(messageId1), "Should be able to retry");
        assertTrue(retryManager.retryFurther(messageId2), "Should be able to retry");

        // Fail messageId1 a 3rd time
        retryManager.failed(messageId1);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, messageId1, 3, thirdRetryTime, false);
        validateExpectedFailedMessageId(retryManager, messageId2, 1, firstRetryTime, false);

        // Validate that messageId1 cannot be retried, messageId2 can.
        assertFalse(retryManager.retryFurther(messageId1), "Should NOT be able to retry");
        assertTrue(retryManager.retryFurther(messageId2), "Should be able to retry");
    }

    /**
     * Tests calling nextFailedMessageToRetry() when nothing should have been expired.
     */
    @Test
    public void testNextFailedMessageToRetryWithNothingExpired() {
        // construct manager
        final int expectedMaxRetries = 3;
        final long expectedMinRetryTimeMs = 1000;

        // Use a wacky multiplier, because why not?
        final double expectedDelayMultiplier = 4.2;

        // Build config.
        Map<String, Object> stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId1 = new MessageId("MyTopic", 0, 101L, consumerId);
        final MessageId messageId2 = new MessageId("MyTopic", 0, 102L, consumerId);

        // Mark both as having been failed.
        retryManager.failed(messageId1);
        retryManager.failed(messageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(
            retryManager,
            messageId1,
            1,
            (FIXED_TIME + (long) expectedMinRetryTimeMs),
            false
        );
        validateExpectedFailedMessageId(
            retryManager,
            messageId2,
            1,
            (FIXED_TIME + (long) expectedMinRetryTimeMs),
            false
        );

        // Ask for the next tuple to retry, should be empty
        assertNull(retryManager.nextFailedMessageToRetry(), "Should be null");
        assertNull(retryManager.nextFailedMessageToRetry(), "Should be null");
        assertNull(retryManager.nextFailedMessageToRetry(), "Should be null");
        assertNull(retryManager.nextFailedMessageToRetry(), "Should be null");
    }

    /**
     * Tests calling nextFailedMessageToRetry() when nothing should have been expired.
     */
    @Test
    public void testNextFailedMessageToRetryWithExpiring() {
        // construct manager
        final int expectedMaxRetries = 3;
        final long expectedMinRetryTimeMs = 1000;

        // Use a wacky multiplier, because why not?
        final double expectedDelayMultiplier = 6.2;

        // Build config.
        Map<String, Object> stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId1 = new MessageId("MyTopic", 0, 101L, consumerId);
        final MessageId messageId2 = new MessageId("MyTopic", 0, 102L, consumerId);

        // Mark messageId1 as having failed
        retryManager.failed(messageId1);

        // Mark messageId2 as having failed twice
        retryManager.failed(messageId2);
        retryManager.failed(messageId2);

        // Calculate the first and 2nd fail retry times
        final long firstRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 0));
        final long secondRetryTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 1));

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, messageId1, 1, firstRetryTime, false);
        validateExpectedFailedMessageId(retryManager, messageId2, 2, secondRetryTime, false);

        // Ask for the next tuple to retry, should be empty
        assertNull(retryManager.nextFailedMessageToRetry(), "Should be null");
        assertNull(retryManager.nextFailedMessageToRetry(), "Should be null");
        assertNull(retryManager.nextFailedMessageToRetry(), "Should be null");
        assertNull(retryManager.nextFailedMessageToRetry(), "Should be null");

        // Now advance time by exactly expectedMinRetryTimeMs milliseconds
        retryManager.setClock(
            Clock.fixed(
            Instant.ofEpochMilli(FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 0))),
            ZoneOffset.UTC
        ));

        // Now messageId1 should expire next,
        MessageId nextMessageIdToBeRetried = retryManager.nextFailedMessageToRetry();
        assertNotNull(nextMessageIdToBeRetried, "result should not be null");
        assertEquals(messageId1, nextMessageIdToBeRetried, "Should be our messageId1");

        // Validate the internal state.
        validateTupleNotInFailedSetButIsInFlight(retryManager, messageId1);
        validateExpectedFailedMessageId(retryManager, messageId2, 2, secondRetryTime, false);

        // Calling nextFailedMessageToRetry should result in null.
        assertNull(retryManager.nextFailedMessageToRetry(), "Should be null");
        assertNull(retryManager.nextFailedMessageToRetry(), "Should be null");
        assertNull(retryManager.nextFailedMessageToRetry(), "Should be null");
        assertNull(retryManager.nextFailedMessageToRetry(), "Should be null");

        // Advance time again, by expected retry time, plus a few MS
        final long newFixedTime = FIXED_TIME + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 1)) + 10;
        retryManager.setClock(Clock.fixed(Instant.ofEpochMilli(newFixedTime), ZoneId.of("UTC")));

        // Now messageId1 should expire next,
        nextMessageIdToBeRetried = retryManager.nextFailedMessageToRetry();
        assertNotNull(nextMessageIdToBeRetried, "result should not be null");

        // Validate state.
        validateTupleNotInFailedSetButIsInFlight(retryManager, messageId1);
        validateTupleNotInFailedSetButIsInFlight(retryManager, messageId2);

        // call ack, validate its no longer tracked
        retryManager.acked(messageId1);
        validateTupleIsNotBeingTracked(retryManager, messageId1);
        validateTupleNotInFailedSetButIsInFlight(retryManager, messageId2);

        // Calculate retry time for 3rd fail against new fixed time
        final long thirdRetryTime = newFixedTime + (long) (expectedMinRetryTimeMs * Math.pow(expectedDelayMultiplier, 2));

        // Mark tuple2 as having failed
        retryManager.failed(messageId2);
        validateTupleIsNotBeingTracked(retryManager, messageId1);
        validateExpectedFailedMessageId(retryManager, messageId2, 3, thirdRetryTime, false);
    }

    /**
     * Validates that when we have multiple failed tuples that need to be retried,
     * we retry the earliest ones first.
     */
    @Test
    public void testRetryEarliestFailed() {
        // construct manager
        final int expectedMaxRetries = 3;
        final long expectedMinRetryTimeMs = 0;
        final double expectedDelayMultiplier = 0.5;

        // Build config.
        Map<String, Object> stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId1 = new MessageId("MyTopic", 0, 101L, consumerId);
        final MessageId messageId2 = new MessageId("MyTopic", 0, 102L, consumerId);
        final MessageId messageId3 = new MessageId("MyTopic", 0, 103L, consumerId);

        // Fail messageId 1 @ T0
        retryManager.failed(messageId1);

        // Increment clock to T1 and fail messageId 2
        retryManager.setClock(Clock.fixed(Instant.ofEpochMilli(FIXED_TIME + 100), ZoneId.of("UTC")));
        retryManager.failed(messageId2);

        // Increment clock to T2 and fail messageId 3
        retryManager.setClock(Clock.fixed(Instant.ofEpochMilli(FIXED_TIME + 200), ZoneId.of("UTC")));
        retryManager.failed(messageId3);

        // call 3 times, see what comes out.
        // We'd expect to get messageId1 since its the oldest, followed by messageId2, and then messageId3
        final MessageId result1 = retryManager.nextFailedMessageToRetry();
        final MessageId result2 = retryManager.nextFailedMessageToRetry();
        final MessageId result3 = retryManager.nextFailedMessageToRetry();

        assertEquals(messageId1, result1, "Result1 should be messageId1");
        assertEquals(messageId2, result2, "Result2 should be messageId1");
        assertEquals(messageId3, result3, "Result3 should be messageId1");
    }

    private void validateExpectedFailedMessageId(
        DefaultRetryManager retryManager,
        MessageId messageId,
        int expectedFailCount,
        long expectedRetryTime,
        boolean expectedToBeInFlight
    ) {
        // Find its queue
        Queue<MessageId> failQueue = retryManager.getFailedMessageIds().get(expectedRetryTime);
        assertNotNull(
            failQueue,
            "Queue should exist for our retry time of " + expectedRetryTime + " has [" + retryManager.getFailedMessageIds().keySet() + "]"
        );
        assertTrue(failQueue.contains(messageId), "Queue should contain our tuple messageId");

        // This messageId should have the right number of fails associated with it.
        assertEquals(
            (Integer) expectedFailCount,
            retryManager.getNumberOfTimesFailed().get(messageId),
            "Should have expected number of fails"
        );

        // Should this be marked as in flight?
        assertEquals(expectedToBeInFlight, retryManager.getRetriesInFlight().contains(messageId), "Should or should not be in flight");
    }

    private void validateTupleNotInFailedSetButIsInFlight(DefaultRetryManager retryManager, MessageId messageId) {
        // Loop through all failed tuples
        for (Long key : retryManager.getFailedMessageIds().keySet()) {
            Queue queue = retryManager.getFailedMessageIds().get(key);
            assertFalse(queue.contains(messageId), "Should not contain our messageId");
        }
        assertTrue(retryManager.getRetriesInFlight().contains(messageId), "Should be tracked as in flight");
    }

    private void validateTupleIsNotBeingTracked(DefaultRetryManager retryManager, MessageId messageId) {
        // Loop through all failed tuples
        for (Long key : retryManager.getFailedMessageIds().keySet()) {
            Queue queue = retryManager.getFailedMessageIds().get(key);
            assertFalse(queue.contains(messageId), "Should not contain our messageId");
        }
        assertFalse(retryManager.getRetriesInFlight().contains(messageId), "Should not be tracked as in flight");
        assertFalse(retryManager.getNumberOfTimesFailed().containsKey(messageId), "Should not have a fail count");
    }

    // Helper method
    private Map<String, Object> getDefaultConfig(Integer maxRetries, Long minRetryTimeMs, Double delayMultiplier, Long expectedMaxDelayMs) {
        Map<String, Object> stormConfig = new HashMap<>();
        if (maxRetries != null) {
            stormConfig.put(SpoutConfig.RETRY_MANAGER_RETRY_LIMIT, maxRetries);
        }
        if (minRetryTimeMs != null) {
            stormConfig.put(SpoutConfig.RETRY_MANAGER_INITIAL_DELAY_MS, minRetryTimeMs);
        }
        if (delayMultiplier != null) {
            stormConfig.put(SpoutConfig.RETRY_MANAGER_DELAY_MULTIPLIER, delayMultiplier);
        }
        if (expectedMaxDelayMs != null) {
            stormConfig.put(SpoutConfig.RETRY_MANAGER_MAX_DELAY_MS, expectedMaxDelayMs);
        }
        return stormConfig;
    }
}