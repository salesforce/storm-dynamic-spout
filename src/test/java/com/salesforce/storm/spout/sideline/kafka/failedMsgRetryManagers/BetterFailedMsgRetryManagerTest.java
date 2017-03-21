package com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.Queue;

import static org.junit.Assert.*;

/**
 *
 */
public class BetterFailedMsgRetryManagerTest {
    private static final Logger logger = LoggerFactory.getLogger(BetterFailedMsgRetryManagerTest.class);

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

        // Build config.
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs);

        // Create instance and call open.
        BetterFailedMsgRetryManager retryManager = new BetterFailedMsgRetryManager();
        retryManager.open(stormConfig);

        assertEquals("Wrong max retrys", expectedMaxRetries, retryManager.getMaxRetries());
        assertEquals("Wrong retry time", expectedMinRetryTimeMs, retryManager.getMinRetryTimeMs());
    }

    /**
     * Tests that the open() uses default values if not configured.
     */
    @Test
    public void testOpenWithNoConfigUsesDefaults() {
        // Build config.
        Map stormConfig = getDefaultConfig(null, null);

        // Create instance and call open.
        BetterFailedMsgRetryManager retryManager = new BetterFailedMsgRetryManager();
        retryManager.open(stormConfig);

        assertEquals("Wrong max retrys", 25, retryManager.getMaxRetries());
        assertEquals("Wrong retry time", 1000, retryManager.getMinRetryTimeMs());
    }

    /**
     * Tests tracking a new failed messageIds.
     */
    @Test
    public void testFailedSimpleCase() {
        // construct manager
        final int expectedMaxRetries = 10;
        final long expectedMinRetryTimeMs = 1000;

        // Build config.
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs);

        // Create instance, inject our mock clock,  and call open.
        BetterFailedMsgRetryManager retryManager = new BetterFailedMsgRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        // Define our tuple message id
        final TupleMessageId tupleMessageId1 = new TupleMessageId("MyTopic", 0, 101L, "MyConsumerId");
        final TupleMessageId tupleMessageId2 = new TupleMessageId("MyTopic", 0, 102L, "MyConsumerId");
        final TupleMessageId tupleMessageId3 = new TupleMessageId("MyTopic", 0, 103L, "MyConsumerId");

        // Mark first as having failed
        retryManager.failed(tupleMessageId1);

        // Validate it has failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);

        // Mark second as having failed
        retryManager.failed(tupleMessageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);

        // Mark 3rd as having failed
        retryManager.failed(tupleMessageId3);

        // Validate it has all three as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId3, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);
    }

    /**
     * Tests tracking a new failed messageId.
     */
    @Test
    public void testFailedMultipleFails() {
        // construct manager
        final int expectedMaxRetries = 10;
        final long expectedMinRetryTimeMs = 1000;

        // Build config.
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs);

        // Create instance, inject our mock clock,  and call open.
        BetterFailedMsgRetryManager retryManager = new BetterFailedMsgRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        // Define our tuple message id
        final TupleMessageId tupleMessageId1 = new TupleMessageId("MyTopic", 0, 101L, "MyConsumerId");
        final TupleMessageId tupleMessageId2 = new TupleMessageId("MyTopic", 0, 102L, "MyConsumerId");

        // Mark first as having failed
        retryManager.failed(tupleMessageId1);

        // Validate it has failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);

        // Mark second as having failed
        retryManager.failed(tupleMessageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);

        // Now fail messageId1 a second time.
        retryManager.failed(tupleMessageId1);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 2, (FIXED_TIME + (2 * expectedMinRetryTimeMs)), false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);

        // Now fail messageId1 a 3rd time.
        retryManager.failed(tupleMessageId1);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 3, (FIXED_TIME + (3 * expectedMinRetryTimeMs)), false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);

        // Now fail messageId2 a 2nd time.
        retryManager.failed(tupleMessageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 3, (FIXED_TIME + (3 * expectedMinRetryTimeMs)), false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 2, (FIXED_TIME + (2 * expectedMinRetryTimeMs)), false);
    }

    /**
     * Tests that all previously untracked messageIds should be retried.
     */
    @Test
    public void testRetryFurtherForUntrackedMessageId() {
        // construct manager
        final int expectedMaxRetries = 2;
        final long expectedMinRetryTimeMs = 1000;

        // Build config.
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs);

        // Create instance, inject our mock clock,  and call open.
        BetterFailedMsgRetryManager retryManager = new BetterFailedMsgRetryManager();
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
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs);

        // Create instance, inject our mock clock,  and call open.
        BetterFailedMsgRetryManager retryManager = new BetterFailedMsgRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        // Define our tuple message id
        final TupleMessageId tupleMessageId = new TupleMessageId("MyTopic", 0, 100L, "MyConsumerId");

        assertFalse("Should always be false because we are configured to never retry", retryManager.retryFurther(tupleMessageId));
        assertFalse("Should always be false because we are configured to never retry", retryManager.retryFurther(tupleMessageId));
        assertFalse("Should always be false because we are configured to never retry", retryManager.retryFurther(tupleMessageId));
    }

    /**
     * Tests what happens if a tuple fails more than our max fail limit.
     */
    @Test
    public void testRetryFurtherWhenMessageExceedsRetryLimit() {
        // construct manager
        final int expectedMaxRetries = 3;
        final long expectedMinRetryTimeMs = 1000;

        // Build config.
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs);

        // Create instance, inject our mock clock,  and call open.
        BetterFailedMsgRetryManager retryManager = new BetterFailedMsgRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        // Define our tuple message id
        final TupleMessageId tupleMessageId1 = new TupleMessageId("MyTopic", 0, 101L, "MyConsumerId");
        final TupleMessageId tupleMessageId2 = new TupleMessageId("MyTopic", 0, 102L, "MyConsumerId");

        // Mark first as having failed
        retryManager.failed(tupleMessageId1);

        // Validate it has failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);

        // Mark second as having failed
        retryManager.failed(tupleMessageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);

        // Validate that we can retry both of these
        assertTrue("Should be able to retry", retryManager.retryFurther(tupleMessageId1));
        assertTrue("Should be able to retry", retryManager.retryFurther(tupleMessageId2));

        // Fail messageId1 a 2nd time
        retryManager.failed(tupleMessageId1);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 2, (FIXED_TIME + (2 * expectedMinRetryTimeMs)), false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);

        // Validate that we can retry both of these
        assertTrue("Should be able to retry", retryManager.retryFurther(tupleMessageId1));
        assertTrue("Should be able to retry", retryManager.retryFurther(tupleMessageId2));

        // Fail messageId1 a 3rd time
        retryManager.failed(tupleMessageId1);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 3, (FIXED_TIME + (3 * expectedMinRetryTimeMs)), false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);

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

        // Build config.
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs);

        // Create instance, inject our mock clock,  and call open.
        BetterFailedMsgRetryManager retryManager = new BetterFailedMsgRetryManager();
        retryManager.setClock(mockClock);
        retryManager.open(stormConfig);

        // Define our tuple message id
        final TupleMessageId tupleMessageId1 = new TupleMessageId("MyTopic", 0, 101L, "MyConsumerId");
        final TupleMessageId tupleMessageId2 = new TupleMessageId("MyTopic", 0, 102L, "MyConsumerId");

        // Mark both as having been failed.
        retryManager.failed(tupleMessageId1);
        retryManager.failed(tupleMessageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);

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

        // Build config.
        Map stormConfig = getDefaultConfig(expectedMaxRetries, expectedMinRetryTimeMs);

        // Create instance, inject our mock clock,  and call open.
        BetterFailedMsgRetryManager retryManager = new BetterFailedMsgRetryManager();
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

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, 1, (FIXED_TIME + expectedMinRetryTimeMs), false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 2, (FIXED_TIME + (2 * expectedMinRetryTimeMs)), false);

        // Ask for the next tuple to retry, should be empty
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());

        // Now advance time by exactly expectedMinRetryTimeMs milliseconds
        retryManager.setClock(Clock.fixed(Instant.ofEpochMilli(FIXED_TIME + expectedMinRetryTimeMs), ZoneId.of("UTC")));

        // Now tupleMessageId1 should expire next,
        TupleMessageId nextMessageIdToBeRetried = retryManager.nextFailedMessageToRetry();
        assertNotNull("result should not be null", nextMessageIdToBeRetried);
        assertEquals("Should be our tupleMessageId1", tupleMessageId1, nextMessageIdToBeRetried);

        // Validate the internal state.
        validateTupleNotInFailedSetButIsInFlight(retryManager, tupleMessageId1);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 2, (FIXED_TIME + (2 * expectedMinRetryTimeMs)), false);

        // Calling nextFailedMessageToRetry should result in null.
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());
        assertNull("Should be null", retryManager.nextFailedMessageToRetry());

        // Advance time again, by 2x expected retry time, plus a few MS
        final long newFixedTime = FIXED_TIME + (2 * expectedMinRetryTimeMs) + 10;
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

        // Mark tuple2 as having failed
        retryManager.failed(tupleMessageId2);
        validateTupleIsNotBeingTracked(retryManager, tupleMessageId1);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, 3, (newFixedTime + (3 * expectedMinRetryTimeMs)), false);
    }

    /**
     * Helper method.
     * @param retryManager
     * @param tupleMessageId
     * @param expectedFailCount
     * @param expectedRetryTime
     * @param expectedToBeInFlight
     */
    private void validateExpectedFailedMessageId(BetterFailedMsgRetryManager retryManager, TupleMessageId tupleMessageId, int expectedFailCount, long expectedRetryTime, boolean expectedToBeInFlight) {
        // Find its queue
        Queue<TupleMessageId> failQueue = retryManager.getFailedMessageIds().get(expectedRetryTime);
        assertNotNull("Queue should exist for our retry time of " + expectedRetryTime, failQueue);
        assertTrue("Queue should contain our tuple messageId", failQueue.contains(tupleMessageId));

        // This messageId should have the right number of fails assocaited with it.
        assertEquals("Should have expected number of fails", (Integer) expectedFailCount, (Integer) retryManager.getNumberOfTimesFailed().get(tupleMessageId));

        // Should this be marked as in flight?
        assertEquals("Should or should not be in flight", expectedToBeInFlight, retryManager.getRetriesInFlight().contains(tupleMessageId));
    }

    private void validateTupleNotInFailedSetButIsInFlight(BetterFailedMsgRetryManager retryManager, TupleMessageId tupleMessageId) {
        // Loop thru all failed tuples
        for (Long key : retryManager.getFailedMessageIds().keySet()) {
            Queue queue = retryManager.getFailedMessageIds().get(key);
            assertFalse("Should not contain our messageId", queue.contains(tupleMessageId));
        }
        assertTrue("Should be tracked as in flight", retryManager.getRetriesInFlight().contains(tupleMessageId));
    }

    private void validateTupleIsNotBeingTracked(BetterFailedMsgRetryManager retryManager, TupleMessageId tupleMessageId) {
        // Loop thru all failed tuples
        for (Long key : retryManager.getFailedMessageIds().keySet()) {
            Queue queue = retryManager.getFailedMessageIds().get(key);
            assertFalse("Should not contain our messageId", queue.contains(tupleMessageId));
        }
        assertFalse("Should not be tracked as in flight", retryManager.getRetriesInFlight().contains(tupleMessageId));
        assertFalse("Should not have a fail count", retryManager.getNumberOfTimesFailed().containsKey(tupleMessageId));
    }

    // Helper method
    private Map getDefaultConfig(Integer maxRetries, Long minRetryTimeMs) {
        Map stormConfig = Maps.newHashMap();
        if (maxRetries != null) {
            stormConfig.put(SidelineSpoutConfig.FAILED_MSG_RETRY_MANAGER_MAX_RETRIES, maxRetries);
        }
        if (minRetryTimeMs != null) {
            stormConfig.put(SidelineSpoutConfig.FAILED_MSG_RETRY_MANAGER_MIN_RETRY_TIME_MS, minRetryTimeMs);
        }
        return stormConfig;
    }
}