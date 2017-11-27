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

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test that failed tuples are retried as soon as they can be.
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
        retryManager.open(new AbstractConfig(new ConfigDefinition(), new HashMap<>()));

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
     * @param retryManager retry manager instance.
     * @param messageId message id.
     * @param expectedToBeInFlight  whether or not the message is expected to be in flight.
     */
    private void validateExpectedFailedMessageId(
        FailedTuplesFirstRetryManager retryManager,
        MessageId messageId,
        boolean expectedToBeInFlight
    ) {
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