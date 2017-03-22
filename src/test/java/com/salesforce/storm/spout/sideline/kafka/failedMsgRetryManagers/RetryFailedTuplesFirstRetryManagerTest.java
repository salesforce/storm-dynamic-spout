package com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import org.junit.Test;

import java.util.Map;
import java.util.Queue;

import static org.junit.Assert.*;

/**
 *
 */
public class RetryFailedTuplesFirstRetryManagerTest {

    /**
     * Tests tracking a new failed messageIds.
     */
    @Test
    public void testFailedSimpleCase() {
        // construct manager and call open
        RetryFailedTuplesFirstRetryManager retryManager = new RetryFailedTuplesFirstRetryManager();
        retryManager.open(Maps.newHashMap());

        // Define our tuple message id
        final TupleMessageId tupleMessageId1 = new TupleMessageId("MyTopic", 0, 101L, "MyConsumerId");
        final TupleMessageId tupleMessageId2 = new TupleMessageId("MyTopic", 0, 102L, "MyConsumerId");
        final TupleMessageId tupleMessageId3 = new TupleMessageId("MyTopic", 0, 103L, "MyConsumerId");

        // Mark first as having failed
        retryManager.failed(tupleMessageId1);

        // Validate it has failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, false);

        // Mark second as having failed
        retryManager.failed(tupleMessageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, false);

        // Mark 3rd as having failed
        retryManager.failed(tupleMessageId3);

        // Validate it has all three as failed
        validateExpectedFailedMessageId(retryManager, tupleMessageId1, false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId2, false);
        validateExpectedFailedMessageId(retryManager, tupleMessageId3, false);

        // Now try to get them
        // Get first
        final TupleMessageId firstRetry = retryManager.nextFailedMessageToRetry();
        assertNotNull("Should be not null", firstRetry);
        assertEquals("Should be our first messageId", tupleMessageId1, firstRetry);
        validateTupleNotInFailedSetButIsInFlight(retryManager, firstRetry);

        // Get 2nd
        final TupleMessageId secondRetry = retryManager.nextFailedMessageToRetry();
        assertNotNull("Should be not null", secondRetry);
        assertEquals("Should be our first messageId", tupleMessageId2, secondRetry);
        validateTupleNotInFailedSetButIsInFlight(retryManager, secondRetry);

        // Get 3rd
        final TupleMessageId thirdRetry = retryManager.nextFailedMessageToRetry();
        assertNotNull("Should be not null", thirdRetry);
        assertEquals("Should be our first messageId", tupleMessageId3, thirdRetry);
        validateTupleNotInFailedSetButIsInFlight(retryManager, thirdRetry);

        // Call next failed 3 times, should be null cuz all are in flight
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());

        // Mark 2nd as acked
        retryManager.acked(tupleMessageId2);
        validateTupleIsNotBeingTracked(retryManager, tupleMessageId2);

        // Mark 3rd as failed
        retryManager.failed(tupleMessageId3);
        validateExpectedFailedMessageId(retryManager, tupleMessageId3, false);

        // Mark 1st as acked
        retryManager.acked(tupleMessageId1);
        validateTupleIsNotBeingTracked(retryManager, tupleMessageId1);

        // Call next failed tuple, should be tuple id 3
        final TupleMessageId finalRetry = retryManager.nextFailedMessageToRetry();
        assertNotNull("Should be not null", finalRetry);
        assertEquals("Should be our first messageId", tupleMessageId3, finalRetry);
        validateTupleNotInFailedSetButIsInFlight(retryManager, finalRetry);

        // Call next failed 3 times, should be null cuz all are in flight
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());

        // Ack last remaining
        validateTupleNotInFailedSetButIsInFlight(retryManager, tupleMessageId3);
        retryManager.acked(tupleMessageId3);
        validateTupleIsNotBeingTracked(retryManager, tupleMessageId1);
        validateTupleIsNotBeingTracked(retryManager, tupleMessageId2);
        validateTupleIsNotBeingTracked(retryManager, tupleMessageId3);

        // Call next failed 3 times, should be null because nothing is left!
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());

        // And we always retry further.
        assertTrue("Should always be true regardless of input", retryManager.retryFurther(null));
        assertTrue("Should always be true regardless of input", retryManager.retryFurther(tupleMessageId1));
        assertTrue("Should always be true regardless of input", retryManager.retryFurther(tupleMessageId2));
        assertTrue("Should always be true regardless of input", retryManager.retryFurther(tupleMessageId3));
    }

    /**
     * Helper method.
     * @param retryManager
     * @param tupleMessageId
     * @param expectedToBeInFlight
     */
    private void validateExpectedFailedMessageId(RetryFailedTuplesFirstRetryManager retryManager, TupleMessageId tupleMessageId, boolean expectedToBeInFlight) {
        // Find its queue
        assertTrue("Queue should contain our tuple messageId", retryManager.getFailedMessageIds().contains(tupleMessageId));

        // Should this be marked as in flight?
        assertEquals("Should or should not be in flight", expectedToBeInFlight, retryManager.getMessageIdsInFlight().contains(tupleMessageId));
    }

    private void validateTupleNotInFailedSetButIsInFlight(RetryFailedTuplesFirstRetryManager retryManager, TupleMessageId tupleMessageId) {
        assertFalse("Should not contain our messageId", retryManager.getFailedMessageIds().contains(tupleMessageId));
        assertTrue("Should be tracked as in flight", retryManager.getMessageIdsInFlight().contains(tupleMessageId));
    }

    private void validateTupleIsNotBeingTracked(RetryFailedTuplesFirstRetryManager retryManager, TupleMessageId tupleMessageId) {
        assertFalse("Should not contain our messageId", retryManager.getFailedMessageIds().contains(tupleMessageId));
        assertFalse("Should not be tracked as in flight", retryManager.getMessageIdsInFlight().contains(tupleMessageId));
    }
}