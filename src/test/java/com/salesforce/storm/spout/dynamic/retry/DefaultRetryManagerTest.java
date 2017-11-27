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

import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
    @Before
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
        final AbstractConfig spoutConfig = getDefaultConfig(
            expectedMaxRetries,
            expectedMinRetryTimeMs,
            expectedDelayMultiplier,
            expectedMaxDelayMs);

        // Create instance and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.open(spoutConfig);

        assertEquals("Wrong max retries", expectedMaxRetries, retryManager.getRetryLimit());
        assertEquals("Wrong retry time", expectedMinRetryTimeMs, retryManager.getInitialRetryDelayMs());
        assertEquals("Wrong retry time", expectedDelayMultiplier, retryManager.getRetryDelayMultiplier(), 0.001);
        assertEquals("Wrong retry time", expectedMaxDelayMs, retryManager.getRetryDelayMaxMs());
    }

    /**
     * Tests that the open() uses default values if not configured.
     */
    @Test
    public void testOpenWithNoConfigUsesDefaults() {
        // Build config.
        final AbstractConfig spoutConfig = getDefaultConfig(null, null, null, null);

        // Create instance and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.open(spoutConfig);

        assertEquals("Wrong max retries", -1, retryManager.getRetryLimit());
        assertEquals("Wrong retry time", 2000, retryManager.getInitialRetryDelayMs());
        assertEquals("Wrong max delay", 15 * 60 * 1000, retryManager.getRetryDelayMaxMs());
        assertEquals("Wrong delay multiplier", 2.0, retryManager.getRetryDelayMultiplier(), 0.01);
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
        final AbstractConfig spoutConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(spoutConfig);

        // Calculate the 1st retry times
        final long firstRetryTime = FIXED_TIME + (long) (1 * expectedMinRetryTimeMs * expectedDelayMultiplier);

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
        final double expectedDelayMultiplier = 11.25;

        // Build config.
        final AbstractConfig spoutConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(spoutConfig);

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId1 = new MessageId("MyTopic", 0, 101L, consumerId);
        final MessageId messageId2 = new MessageId("MyTopic", 0, 102L, consumerId);

        // Calculate the 1st, 2nd, and 3rd fail retry times
        final long firstRetryTime = FIXED_TIME + (long) (1 * expectedMinRetryTimeMs * expectedDelayMultiplier);
        final long secondRetryTime = FIXED_TIME + (long) (2 * expectedMinRetryTimeMs * expectedDelayMultiplier);
        final long thirdRetryTime = FIXED_TIME + (long) (3 * expectedMinRetryTimeMs * expectedDelayMultiplier);

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
        final double expectedDelayMultiplier = 5;

        // Set a max delay of 12 seconds
        final long expectedMaxDelay = 12000;

        // Build config.
        final AbstractConfig spoutConfig = getDefaultConfig(
            expectedMaxRetries,
            expectedMinRetryTimeMs,
            expectedDelayMultiplier,
            expectedMaxDelay);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(spoutConfig);

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId1 = new MessageId("MyTopic", 0, 101L, consumerId);

        // Calculate the 1st, 2nd, and 3rd fail retry times
        final long firstRetryTime = FIXED_TIME + (long) (1 * expectedMinRetryTimeMs * expectedDelayMultiplier);
        final long secondRetryTime = FIXED_TIME + (long) (2 * expectedMinRetryTimeMs * expectedDelayMultiplier);
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
        final AbstractConfig spoutConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, null, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(spoutConfig);

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId = new MessageId("MyTopic", 0, 100L, consumerId);

        assertTrue("Should always be true because its untracked", retryManager.retryFurther(messageId));
        assertTrue("Should always be true because its untracked", retryManager.retryFurther(messageId));
        assertTrue("Should always be true because its untracked", retryManager.retryFurther(messageId));
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
        final AbstractConfig spoutConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, null, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(spoutConfig);

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId = new MessageId("MyTopic", 0, 100L, consumerId);

        for (int x = 0; x < 100; x++) {
            assertFalse("Should always be false because we are configured to never retry", retryManager.retryFurther(messageId));
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
        final AbstractConfig spoutConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, null, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(spoutConfig);

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId = new MessageId("MyTopic", 0, 100L, consumerId);

        for (int x = 0;  x < 100; x++) {
            // Fail tuple
            retryManager.failed(messageId);

            // See if we should retry
            assertTrue("Should always be true because we are configured to always retry", retryManager.retryFurther(messageId));
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
        final AbstractConfig spoutConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(spoutConfig);

        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // Define our tuple message id
        final MessageId messageId1 = new MessageId("MyTopic", 0, 101L, consumerId);
        final MessageId messageId2 = new MessageId("MyTopic", 0, 102L, consumerId);

        // Calculate the 1st, 2nd, and 3rd fail retry times
        final long firstRetryTime = FIXED_TIME + (long) (1 * expectedMinRetryTimeMs * expectedDelayMultiplier);
        final long secondRetryTime = FIXED_TIME + (long) (2 * expectedMinRetryTimeMs * expectedDelayMultiplier);
        final long thirdRetryTime = FIXED_TIME + (long) (3 * expectedMinRetryTimeMs * expectedDelayMultiplier);

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
        assertTrue("Should be able to retry", retryManager.retryFurther(messageId1));
        assertTrue("Should be able to retry", retryManager.retryFurther(messageId2));

        // Fail messageId1 a 2nd time
        retryManager.failed(messageId1);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, messageId1, 2, secondRetryTime, false);
        validateExpectedFailedMessageId(retryManager, messageId2, 1, firstRetryTime, false);

        // Validate that we can retry both of these
        assertTrue("Should be able to retry", retryManager.retryFurther(messageId1));
        assertTrue("Should be able to retry", retryManager.retryFurther(messageId2));

        // Fail messageId1 a 3rd time
        retryManager.failed(messageId1);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, messageId1, 3, thirdRetryTime, false);
        validateExpectedFailedMessageId(retryManager, messageId2, 1, firstRetryTime, false);

        // Validate that messageId1 cannot be retried, messageId2 can.
        assertFalse("Should NOT be able to retry", retryManager.retryFurther(messageId1));
        assertTrue("Should be able to retry", retryManager.retryFurther(messageId2));
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
        final AbstractConfig spoutConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(spoutConfig);

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
            (FIXED_TIME + (long) (expectedMinRetryTimeMs * expectedDelayMultiplier)),
            false
        );
        validateExpectedFailedMessageId(
            retryManager,
            messageId2,
            1,
            (FIXED_TIME + (long) (expectedMinRetryTimeMs * expectedDelayMultiplier)),
            false
        );

        // Ask for the next tuple to retry, should be empty
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
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
        final AbstractConfig spoutConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(spoutConfig);

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
        final long firstRetryTime = FIXED_TIME + (long) (1 * expectedMinRetryTimeMs * expectedDelayMultiplier);
        final long secondRetryTime = FIXED_TIME + (long) (2 * expectedMinRetryTimeMs * expectedDelayMultiplier);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, messageId1, 1, firstRetryTime, false);
        validateExpectedFailedMessageId(retryManager, messageId2, 2, secondRetryTime, false);

        // Ask for the next tuple to retry, should be empty
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());

        // Now advance time by exactly expectedMinRetryTimeMs milliseconds
        retryManager.setClock(
            Clock.fixed(Instant.ofEpochMilli(FIXED_TIME + (long) (expectedMinRetryTimeMs * expectedDelayMultiplier)), ZoneId.of("UTC"))
        );

        // Now messageId1 should expire next,
        MessageId nextMessageIdToBeRetried = retryManager.nextFailedMessageToRetry();
        assertNotNull("result should not be null", nextMessageIdToBeRetried);
        assertEquals("Should be our messageId1", messageId1, nextMessageIdToBeRetried);

        // Validate the internal state.
        validateTupleNotInFailedSetButIsInFlight(retryManager, messageId1);
        validateExpectedFailedMessageId(retryManager, messageId2, 2, secondRetryTime, false);

        // Calling nextFailedMessageToRetry should result in null.
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());

        // Advance time again, by 2x expected retry time, plus a few MS
        final long newFixedTime = FIXED_TIME + (long) (2 * expectedMinRetryTimeMs * expectedDelayMultiplier) + 10;
        retryManager.setClock(Clock.fixed(Instant.ofEpochMilli(newFixedTime), ZoneId.of("UTC")));

        // Now messageId1 should expire next,
        nextMessageIdToBeRetried = retryManager.nextFailedMessageToRetry();
        assertNotNull("result should not be null", nextMessageIdToBeRetried);

        // Validate state.
        validateTupleNotInFailedSetButIsInFlight(retryManager, messageId1);
        validateTupleNotInFailedSetButIsInFlight(retryManager, messageId2);

        // call ack, validate its no longer tracked
        retryManager.acked(messageId1);
        validateTupleIsNotBeingTracked(retryManager, messageId1);
        validateTupleNotInFailedSetButIsInFlight(retryManager, messageId2);

        // Calculate time for 3rd fail, against new fixed time
        final long thirdRetryTime = newFixedTime + (long) (3 * expectedMinRetryTimeMs * expectedDelayMultiplier);

        // Mark tuple2 as having failed
        retryManager.failed(messageId2);
        validateTupleIsNotBeingTracked(retryManager, messageId1);
        validateExpectedFailedMessageId(retryManager, messageId2, 3, thirdRetryTime, false);
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
            "Queue should exist for our retry time of " + expectedRetryTime + " has [" + retryManager.getFailedMessageIds().keySet() + "]",
            failQueue
        );
        assertTrue("Queue should contain our tuple messageId", failQueue.contains(messageId));

        // This messageId should have the right number of fails associated with it.
        assertEquals(
            "Should have expected number of fails",
            (Integer) expectedFailCount, retryManager.getNumberOfTimesFailed().get(messageId)
        );

        // Should this be marked as in flight?
        assertEquals("Should or should not be in flight", expectedToBeInFlight, retryManager.getRetriesInFlight().contains(messageId));
    }

    private void validateTupleNotInFailedSetButIsInFlight(DefaultRetryManager retryManager, MessageId messageId) {
        // Loop through all failed tuples
        for (Long key : retryManager.getFailedMessageIds().keySet()) {
            Queue queue = retryManager.getFailedMessageIds().get(key);
            assertFalse("Should not contain our messageId", queue.contains(messageId));
        }
        assertTrue("Should be tracked as in flight", retryManager.getRetriesInFlight().contains(messageId));
    }

    private void validateTupleIsNotBeingTracked(DefaultRetryManager retryManager, MessageId messageId) {
        // Loop through all failed tuples
        for (Long key : retryManager.getFailedMessageIds().keySet()) {
            Queue queue = retryManager.getFailedMessageIds().get(key);
            assertFalse("Should not contain our messageId", queue.contains(messageId));
        }
        assertFalse("Should not be tracked as in flight", retryManager.getRetriesInFlight().contains(messageId));
        assertFalse("Should not have a fail count", retryManager.getNumberOfTimesFailed().containsKey(messageId));
    }

    // Helper method
    private AbstractConfig getDefaultConfig(Integer maxRetries, Long minRetryTimeMs, Double delayMultiplier, Long expectedMaxDelayMs) {
        final Map<String, Object> config = new HashMap<>();
        if (maxRetries != null) {
            config.put(SpoutConfig.RETRY_MANAGER_RETRY_LIMIT, maxRetries);
        }
        if (minRetryTimeMs != null) {
            config.put(SpoutConfig.RETRY_MANAGER_INITIAL_DELAY_MS, minRetryTimeMs);
        }
        if (delayMultiplier != null) {
            config.put(SpoutConfig.RETRY_MANAGER_DELAY_MULTIPLIER, delayMultiplier);
        }
        if (expectedMaxDelayMs != null) {
            config.put(SpoutConfig.RETRY_MANAGER_MAX_DELAY_MS, expectedMaxDelayMs);
        }
        return new AbstractConfig(new ConfigDefinition(), config);
    }
}