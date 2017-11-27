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

import com.google.common.collect.Sets;
import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * This implementation will always retry failed tuples at the earliest chance it can.
 * No back-off strategy, no maximum times a tuple can fail.
 */
public class FailedTuplesFirstRetryManager implements RetryManager {
    /**
     * This Set holds which Tuples are in flight.
     */
    private Set<MessageId> messageIdsInFlight;

    /**
     * Our FIFO queue of failed messageIds.
     */
    private Queue<MessageId> failedMessageIds;

    @Override
    public void open(final AbstractConfig spoutConfig) {
        messageIdsInFlight = Sets.newHashSet();
        failedMessageIds = new LinkedList<>();
    }

    @Override
    public void failed(MessageId messageId) {
        messageIdsInFlight.remove(messageId);
        failedMessageIds.add(messageId);
    }

    @Override
    public void acked(MessageId messageId) {
        messageIdsInFlight.remove(messageId);
        failedMessageIds.remove(messageId);
    }

    @Override
    public MessageId nextFailedMessageToRetry() {
        final MessageId nextMessageId = failedMessageIds.poll();
        if (nextMessageId == null) {
            return null;
        }
        messageIdsInFlight.add(nextMessageId);
        return nextMessageId;
    }

    @Override
    public boolean retryFurther(MessageId messageId) {
        // We always retry.
        return true;
    }

    /**
     * @return - the messageIds currently in flight.
     */
    Set<MessageId> getMessageIdsInFlight() {
        return messageIdsInFlight;
    }

    /**
     * @return - the messageIds currently marked as having failed, excluding those in flight.
     */
    Queue<MessageId> getFailedMessageIds() {
        return failedMessageIds;
    }
}
