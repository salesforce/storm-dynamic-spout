package com.salesforce.storm.spout.sideline.kafka.retryManagers;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
        final long expectedMaxDelayMS = 1000;

        // Build config.
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, expectedMaxDelayMS);

        // Create instance and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.open(stormConfig);

        assertEquals("Wrong max retries", expectedMaxRetries, retryManager.getRetryLimit());
        assertEquals("Wrong retry time", expectedMinRetryTimeMs, retryManager.getInitialRetryDelayMs());
        assertEquals("Wrong retry time", expectedDelayMultiplier, retryManager.getRetryDelayMultiplier(), 0.001);
        assertEquals("Wrong retry time", expectedMaxDelayMS, retryManager.getRetryDelayMaxMs());
    }

    /**
     * Tests that the open() uses default values if not configured.
     */
    @Test
    public void testOpenWithNoConfigUsesDefaults() {
        // Build config.
        Map stormConfig = getDefaultConfig(null, null, null, null);

        // Create instance and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.open(stormConfig);

        assertEquals("Wrong max retries", -1, retryManager.getRetryLimit());
        assertEquals("Wrong retry time", 2000, retryManager.getInitialRetryDelayMs());
        assertEquals("Wrong max delay", 60 * 1000, retryManager.getRetryDelayMaxMs());
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
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        // Calculate the 1st retry times
        final long firstRetryTime = FIXED_TIME + (long) (1 * expectedMinRetryTimeMs * expectedDelayMultiplier);

        // Define our tuple message id
        final TupleMessageId tupleMessageId1 = new TupleMessageId("MyTopic", 0, 101L, "MyConsumerId");
        final TupleMessageId tupleMessageId2 = new TupleMessageId("MyTopic", 0, 102L, "MyConsumerId");
        final TupleMessageId tupleMessageId3 = new TupleMessageId("MyTopic", 0, 103L, "MyConsumerId");

        // Mark first as having failed
        retryManager.failed(tupleMessageId1);

        // Validate it has failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, firstRetryTime, false);

        // Mark second as having failed
        retryManager.failed(tupleMessageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, firstRetryTime, false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, firstRetryTime, false);

        // Mark 3rd as having failed
        retryManager.failed(tupleMessageId3);

        // Validate it has all three as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, firstRetryTime, false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, firstRetryTime, false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId3, 1, firstRetryTime, false);
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
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        // Define our tuple message id
        final TupleMessageId tupleMessageId1 = new TupleMessageId("MyTopic", 0, 101L, "MyConsumerId");
        final TupleMessageId tupleMessageId2 = new TupleMessageId("MyTopic", 0, 102L, "MyConsumerId");

        // Calculate the 1st, 2nd, and 3rd fail retry times
        final long firstRetryTime = FIXED_TIME + (long) (1 * expectedMinRetryTimeMs * expectedDelayMultiplier);
        final long secondRetryTime = FIXED_TIME + (long) (2 * expectedMinRetryTimeMs * expectedDelayMultiplier);
        final long thirdRetryTime = FIXED_TIME + (long) (3 * expectedMinRetryTimeMs * expectedDelayMultiplier);

        // Mark first as having failed
        retryManager.failed(tupleMessageId1);

        // Validate it has failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, firstRetryTime, false);

        // Mark second as having failed
        retryManager.failed(tupleMessageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, firstRetryTime, false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, firstRetryTime, false);

        // Now fail messageId1 a second time.
        retryManager.failed(tupleMessageId1);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 2, secondRetryTime, false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, firstRetryTime, false);

        // Now fail messageId1 a 3rd time.
        retryManager.failed(tupleMessageId1);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 3, thirdRetryTime, false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, firstRetryTime, false);

        // Now fail messageId2 a 2nd time.
        retryManager.failed(tupleMessageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 3, thirdRetryTime, false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 2, secondRetryTime, false);
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
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, expectedMaxDelay);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        // Define our tuple message id
        final TupleMessageId tupleMessageId1 = new TupleMessageId("MyTopic", 0, 101L, "MyConsumerId");

        // Calculate the 1st, 2nd, and 3rd fail retry times
        final long firstRetryTime = FIXED_TIME + (long) (1 * expectedMinRetryTimeMs * expectedDelayMultiplier);
        final long secondRetryTime = FIXED_TIME + (long) (2 * expectedMinRetryTimeMs * expectedDelayMultiplier);
        final long thirdRetryTime = FIXED_TIME + expectedMaxDelay;

        // Mark first as having failed
        retryManager.failed(tupleMessageId1);

        // Validate it has failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, firstRetryTime, false);

        // Now fail messageId1 a second time.
        retryManager.failed(tupleMessageId1);

        // Validate it incremented our delay time, still below configured max delay
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 2, secondRetryTime, false);

        // Now fail messageId1 a 3rd time.
        retryManager.failed(tupleMessageId1);

        // Validate its pinned at configured max delay
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 3, thirdRetryTime, false);

        // Now fail messageId1 a 4th time.
        retryManager.failed(tupleMessageId1);

        // Validate its still pinned at configured max delay
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 4, thirdRetryTime, false);
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
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, null, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        // Define our tuple message id
        final TupleMessageId tupleMessageId = new TupleMessageId("MyTopic", 0, 100L, "MyConsumerId");

        assertTrue("Should always be true because its untracked", retryManager.retryFurther(tupleMessageId));
        assertTrue("Should always be true because its untracked", retryManager.retryFurther(tupleMessageId));
        assertTrue("Should always be true because its untracked", retryManager.retryFurther(tupleMessageId));
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
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, null, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        // Define our tuple message id
        final TupleMessageId tupleMessageId = new TupleMessageId("MyTopic", 0, 100L, "MyConsumerId");

        for (int x=0; x<100; x++) {
            assertFalse("Should always be false because we are configured to never retry", retryManager.retryFurther(tupleMessageId));
        }
    }

    /**
     * Tests retryFurther always returns true if maxRetries is configured to a value less than 0
     */
    @Test
    public void testRetryFurtherWithMaxRetriesSetNegative() {
        // construct manager
        final int expectedMaxRetries = -1;
        final long expectedMinRetryTimeMs = 1000;

        // Build config.
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, null, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        // Define our tuple message id
        final TupleMessageId tupleMessageId = new TupleMessageId("MyTopic", 0, 100L, "MyConsumerId");

        for (int x=0; x<100; x++) {
            // Fail tuple
            retryManager.failed(tupleMessageId);

            // See if we should retry
            assertTrue("Should always be true because we are configured to always retry", retryManager.retryFurther(tupleMessageId));
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
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        // Define our tuple message id
        final TupleMessageId tupleMessageId1 = new TupleMessageId("MyTopic", 0, 101L, "MyConsumerId");
        final TupleMessageId tupleMessageId2 = new TupleMessageId("MyTopic", 0, 102L, "MyConsumerId");

        // Calculate the 1st, 2nd, and 3rd fail retry times
        final long firstRetryTime = FIXED_TIME + (long) (1 * expectedMinRetryTimeMs * expectedDelayMultiplier);
        final long secondRetryTime = FIXED_TIME + (long) (2 * expectedMinRetryTimeMs * expectedDelayMultiplier);
        final long thirdRetryTime = FIXED_TIME + (long) (3 * expectedMinRetryTimeMs * expectedDelayMultiplier);

        // Mark first as having failed
        retryManager.failed(tupleMessageId1);

        // Validate it has failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, firstRetryTime, false);

        // Mark second as having failed
        retryManager.failed(tupleMessageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, firstRetryTime, false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, firstRetryTime, false);

        // Validate that we can retry both of these
        assertTrue("Should be able to retry", retryManager.retryFurther(tupleMessageId1));
        assertTrue("Should be able to retry", retryManager.retryFurther(tupleMessageId2));

        // Fail messageId1 a 2nd time
        retryManager.failed(tupleMessageId1);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 2, secondRetryTime, false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, firstRetryTime, false);

        // Validate that we can retry both of these
        assertTrue("Should be able to retry", retryManager.retryFurther(tupleMessageId1));
        assertTrue("Should be able to retry", retryManager.retryFurther(tupleMessageId2));

        // Fail messageId1 a 3rd time
        retryManager.failed(tupleMessageId1);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 3, thirdRetryTime, false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, firstRetryTime, false);

        // Validate that messageId1 cannot be retried, messageId2 can.
        assertFalse("Should NOT be able to retry", retryManager.retryFurther(tupleMessageId1));
        assertTrue("Should be able to retry", retryManager.retryFurther(tupleMessageId2));
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
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        // Define our tuple message id
        final TupleMessageId tupleMessageId1 = new TupleMessageId("MyTopic", 0, 101L, "MyConsumerId");
        final TupleMessageId tupleMessageId2 = new TupleMessageId("MyTopic", 0, 102L, "MyConsumerId");

        // Mark both as having been failed.
        retryManager.failed(tupleMessageId1);
        retryManager.failed(tupleMessageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, (FIXED_TIME + (long) (expectedMinRetryTimeMs * expectedDelayMultiplier)), false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, (FIXED_TIME + (long) (expectedMinRetryTimeMs * expectedDelayMultiplier)), false);

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
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs, expectedDelayMultiplier, null);

        // Create instance, inject our mock clock,  and call open.
        DefaultRetryManager retryManager = new DefaultRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        // Define our tuple message id
        final TupleMessageId tupleMessageId1 = new TupleMessageId("MyTopic", 0, 101L, "MyConsumerId");
        final TupleMessageId tupleMessageId2 = new TupleMessageId("MyTopic", 0, 102L, "MyConsumerId");

        // Mark tupleMessageID1 as having failed
        retryManager.failed(tupleMessageId1);

        // Mark tupleMessageId2 as having failed twice
        retryManager.failed(tupleMessageId2);
        retryManager.failed(tupleMessageId2);

        // Calculate the first and 2nd fail retry times
        final long firstRetryTime = FIXED_TIME + (long) (1 * expectedMinRetryTimeMs * expectedDelayMultiplier);
        final long secondRetryTime = FIXED_TIME + (long) (2 * expectedMinRetryTimeMs * expectedDelayMultiplier);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, firstRetryTime, false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 2, secondRetryTime, false);

        // Ask for the next tuple to retry, should be empty
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());

        // Now advance time by exactly expectedMinRetryTimeMs milliseconds
        retryManager.setClock(Clock.fixed(Instant.ofEpochMilli(FIXED_TIME + (long) (expectedMinRetryTimeMs * expectedDelayMultiplier)), ZoneId.of("UTC")));

        // Now tupleMessageId1 should expire next,
        TupleMessageId nextMessageIdToBeRetried = retryManager.nextFailedMessageToRetry();
        assertNotNull("result should not be null", nextMessageIdToBeRetried);
        assertEquals("Should be our tupleMessageId1", tupleMessageId1, nextMessageIdToBeRetried);

        // Validate the internal state.
        validateTupleNotInFailedSetButIsInFlight(retryManager, tupleMessageId1);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 2, secondRetryTime, false);

        // Calling nextFailedMessageToRetry should result in null.
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());

        // Advance time again, by 2x expected retry time, plus a few MS
        final long newFixedTime = FIXED_TIME + (long) (2 * expectedMinRetryTimeMs * expectedDelayMultiplier) + 10;
        retryManager.setClock(Clock.fixed(Instant.ofEpochMilli(newFixedTime), ZoneId.of("UTC")));

        // Now tupleMessageId1 should expire next,
        nextMessageIdToBeRetried = retryManager.nextFailedMessageToRetry();
        assertNotNull("result should not be null", nextMessageIdToBeRetried);

        // Validate state.
        validateTupleNotInFailedSetButIsInFlight(retryManager, tupleMessageId1);
        validateTupleNotInFailedSetButIsInFlight(retryManager, tupleMessageId2);

        // call ack, validate its no longer tracked
        retryManager.acked(tupleMessageId1);
        validateTupleIsNotBeingTracked(retryManager, tupleMessageId1);
        validateTupleNotInFailedSetButIsInFlight(retryManager, tupleMessageId2);

        // Calculate time for 3rd fail, against new fixed time
        final long thirdRetryTime = newFixedTime + (long) (3 * expectedMinRetryTimeMs * expectedDelayMultiplier);

        // Mark tuple2 as having failed
        retryManager.failed(tupleMessageId2);
        validateTupleIsNotBeingTracked(retryManager, tupleMessageId1);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 3, thirdRetryTime, false);
    }

    /**
     * Helper method.
     * @param retryManager
     * @param tupleMessageId
     * @param expectedFailCount
     * @param expectedRetryTime
     * @param expectedToBeInFlight
     */
    private void validateExpectedFailedMessageId(DefaultRetryManager retryManager, TupleMessageId tupleMessageId, int expectedFailCount, long expectedRetryTime, boolean expectedToBeInFlight) {
        // Find its queue
        Queue<TupleMessageId> failQueue = retryManager.getFailedMessageIds().get(expectedRetryTime);
        assertNotNull("Queue should exist for our retry time of " + expectedRetryTime + " has [" + retryManager.getFailedMessageIds().keySet() + "]", failQueue);
        assertTrue("Queue should contain our tuple messageId", failQueue.contains(tupleMessageId));

        // This messageId should have the right number of fails associated with it.
        assertEquals("Should have expected number of fails", (Integer) expectedFailCount, retryManager.getNumberOfTimesFailed().get(tupleMessageId));

        // Should this be marked as in flight?
        assertEquals("Should or should not be in flight", expectedToBeInFlight, retryManager.getRetriesInFlight().contains(tupleMessageId));
    }

    private void validateTupleNotInFailedSetButIsInFlight(DefaultRetryManager retryManager, TupleMessageId tupleMessageId) {
        // Loop through all failed tuples
        for (Long key : retryManager.getFailedMessageIds().keySet()) {
            Queue queue = retryManager.getFailedMessageIds().get(key);
            assertFalse("Should not contain our messageId", queue.contains(tupleMessageId));
        }
        assertTrue("Should be tracked as in flight", retryManager.getRetriesInFlight().contains(tupleMessageId));
    }

    private void validateTupleIsNotBeingTracked(DefaultRetryManager retryManager, TupleMessageId tupleMessageId) {
        // Loop through all failed tuples
        for (Long key : retryManager.getFailedMessageIds().keySet()) {
            Queue queue = retryManager.getFailedMessageIds().get(key);
            assertFalse("Should not contain our messageId", queue.contains(tupleMessageId));
        }
        assertFalse("Should not be tracked as in flight", retryManager.getRetriesInFlight().contains(tupleMessageId));
        assertFalse("Should not have a fail count", retryManager.getNumberOfTimesFailed().containsKey(tupleMessageId));
    }

    // Helper method
    private Map getDefaultConfig(Integer maxRetries, Long minRetryTimeMs, Double delayMultiplier, Long expectedMaxDelayMS) {
        Map stormConfig = Maps.newHashMap();
        if (maxRetries != null) {
            stormConfig.put(SidelineSpoutConfig.RETRY_MANAGER_RETRY_LIMIT, maxRetries);
        }
        if (minRetryTimeMs != null) {
            stormConfig.put(SidelineSpoutConfig.RETRY_MANAGER_INITIAL_DELAY_MS, minRetryTimeMs);
        }
        if (delayMultiplier != null) {
            stormConfig.put(SidelineSpoutConfig.RETRY_MANAGER_DELAY_MULTIPLIER, delayMultiplier);
        }
        if (expectedMaxDelayMS != null) {
            stormConfig.put(SidelineSpoutConfig.RETRY_MANAGER_MAX_DELAY_MS, expectedMaxDelayMS);
        }
        return stormConfig;
    }
}