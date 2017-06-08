package com.salesforce.storm.spout.sideline.retry;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.MessageId;
import com.salesforce.storm.spout.sideline.DefaultVirtualSpoutIdentifier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class FailedTuplesFirstRetryManagerTest {

    /**
     * Tests tracking a new failed messageIds.
     */
    @Test
    public void testFailedSimpleCase() {
        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumerId");

        // construct manager and call open
        FailedTuplesFirstRetryManager retryManager = new FailedTuplesFirstRetryManager();
        retryManager.open(Maps.newHashMap());

        // Define our tuple message id
        final MessageId messageId1 = new MessageId("MyTopic", 0, 101L, consumerId);
        final MessageId messageId2 = new MessageId("MyTopic", 0, 102L, consumerId);
        final MessageId messageId3 = new MessageId("MyTopic", 0, 103L, consumerId);

        // Mark first as having failed
        retryManager.failed(messageId1);

        // Validate it has failed
        validateExpectedFailedMessageId(retryManager, messageId1, false);

        // Mark second as having failed
        retryManager.failed(messageId2);

        // Validate it has first two as failed
        validateExpectedFailedMessageId(retryManager, messageId1, false);
        validateExpectedFailedMessageId(retryManager, messageId2, false);

        // Mark 3rd as having failed
        retryManager.failed(messageId3);

        // Validate it has all three as failed
        validateExpectedFailedMessageId(retryManager, messageId1, false);
        validateExpectedFailedMessageId(retryManager, messageId2, false);
        validateExpectedFailedMessageId(retryManager, messageId3, false);

        // Now try to get them
        // Get first
        final MessageId firstRetry = retryManager.nextFailedMessageToRetry();
        assertNotNull("Should be not null", firstRetry);
        assertEquals("Should be our first messageId", messageId1, firstRetry);
        validateTupleNotInFailedSetButIsInFlight(retryManager, firstRetry);

        // Get 2nd
        final MessageId secondRetry = retryManager.nextFailedMessageToRetry();
        assertNotNull("Should be not null", secondRetry);
        assertEquals("Should be our first messageId", messageId2, secondRetry);
        validateTupleNotInFailedSetButIsInFlight(retryManager, secondRetry);

        // Get 3rd
        final MessageId thirdRetry = retryManager.nextFailedMessageToRetry();
        assertNotNull("Should be not null", thirdRetry);
        assertEquals("Should be our first messageId", messageId3, thirdRetry);
        validateTupleNotInFailedSetButIsInFlight(retryManager, thirdRetry);

        // Call next failed 3 times, should be null cuz all are in flight
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());

        // Mark 2nd as acked
        retryManager.acked(messageId2);
        validateTupleIsNotBeingTracked(retryManager, messageId2);

        // Mark 3rd as failed
        retryManager.failed(messageId3);
        validateExpectedFailedMessageId(retryManager, messageId3, false);

        // Mark 1st as acked
        retryManager.acked(messageId1);
        validateTupleIsNotBeingTracked(retryManager, messageId1);

        // Call next failed tuple, should be tuple id 3
        final MessageId finalRetry = retryManager.nextFailedMessageToRetry();
        assertNotNull("Should be not null", finalRetry);
        assertEquals("Should be our first messageId", messageId3, finalRetry);
        validateTupleNotInFailedSetButIsInFlight(retryManager, finalRetry);

        // Call next failed 3 times, should be null cuz all are in flight
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());

        // Ack last remaining
        validateTupleNotInFailedSetButIsInFlight(retryManager, messageId3);
        retryManager.acked(messageId3);
        validateTupleIsNotBeingTracked(retryManager, messageId1);
        validateTupleIsNotBeingTracked(retryManager, messageId2);
        validateTupleIsNotBeingTracked(retryManager, messageId3);

        // Call next failed 3 times, should be null because nothing is left!
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());

        // And we always retry further.
        assertTrue("Should always be true regardless of input", retryManager.retryFurther(null));
        assertTrue("Should always be true regardless of input", retryManager.retryFurther(messageId1));
        assertTrue("Should always be true regardless of input", retryManager.retryFurther(messageId2));
        assertTrue("Should always be true regardless of input", retryManager.retryFurther(messageId3));
    }

    /**
     * Helper method.
     * @param retryManager
     * @param messageId
     * @param expectedToBeInFlight
     */
    private void validateExpectedFailedMessageId(FailedTuplesFirstRetryManager retryManager, MessageId messageId, boolean expectedToBeInFlight) {
        // Find its queue
        assertTrue("Queue should contain our tuple messageId", retryManager.getFailedMessageIds().contains(messageId));

        // Should this be marked as in flight?
        assertEquals("Should or should not be in flight", expectedToBeInFlight, retryManager.getMessageIdsInFlight().contains(messageId));
    }

    private void validateTupleNotInFailedSetButIsInFlight(FailedTuplesFirstRetryManager retryManager, MessageId messageId) {
        assertFalse("Should not contain our messageId", retryManager.getFailedMessageIds().contains(messageId));
        assertTrue("Should be tracked as in flight", retryManager.getMessageIdsInFlight().contains(messageId));
    }

    private void validateTupleIsNotBeingTracked(FailedTuplesFirstRetryManager retryManager, MessageId messageId) {
        assertFalse("Should not contain our messageId", retryManager.getFailedMessageIds().contains(messageId));
        assertFalse("Should not be tracked as in flight", retryManager.getMessageIdsInFlight().contains(messageId));
    }
}