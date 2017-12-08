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

package com.salesforce.storm.spout.dynamic;

import org.apache.storm.tuple.Values;

/**
 * Represents an abstracted view over MessageId and the Tuple values.
 */
public class Message {

    /**
     * MessageId contains information about what Topic, Partition, Offset, and Consumer this
     * message originated from.
     */
    private final MessageId messageId;

    /**
     * Values contains the values that will be emitted out to the Storm Topology.
     */
    private final Values values;

    /**
     * Determines if this message should be considered 'Permanently Failed.'
     * We're defining "permanently failed" meaning that the topology attempted to process the tuple at least once and the RetryManager
     * implementation has determined that the tuple should never be retried. When this occurs, the tuple will be emitted un-anchored out
     * a "failed" stream. Bolts within the topology can subscribe to this "failed" stream and do its own error handling.
     */
    private final boolean isPermanentlyFailed;

    /**
     * Constructor.
     * @param messageId contains information about what Topic, Partition, Offset, and Consumer this
     * @param values contains the values that will be emitted out to the Storm Topology.
     */
    public Message(final MessageId messageId, final Values values) {
        this(messageId, values, false);
    }

    /**
     * Private constructor.  Allows for creating permanently failed messages.
     * Use the Factory method to create permanently failed messages.
     *
     * @param messageId contains information about what Topic, Partition, Offset, and Consumer this
     * @param values contains the values that will be emitted out to the Storm Topology.
     * @param hasPermanentlyFailed True if it has failed permanently, false if not.
     */
    private Message(final MessageId messageId, final Values values, final boolean hasPermanentlyFailed) {
        this.messageId = messageId;
        this.values = values;
        this.isPermanentlyFailed = hasPermanentlyFailed;
    }

    public MessageId getMessageId() {
        return messageId;
    }

    public String getNamespace() {
        return getMessageId().getNamespace();
    }

    public int getPartition() {
        return getMessageId().getPartition();
    }

    public long getOffset() {
        return getMessageId().getOffset();
    }

    public Values getValues() {
        return values;
    }

    public boolean isPermanentlyFailed() {
        return isPermanentlyFailed;
    }

    @Override
    public String toString() {
        return "Message{"
            + "messageId=" + messageId
            + ", values=" + values
            + ", isPermanentlyFailed=" + isPermanentlyFailed
            + '}';
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        Message that = (Message) other;

        if (!getMessageId().equals(that.getMessageId())) {
            return false;
        }
        if (isPermanentlyFailed() != that.isPermanentlyFailed()) {
            return false;
        }
        return getValues().equals(that.getValues());
    }

    @Override
    public int hashCode() {
        int result = getMessageId().hashCode();
        result = 31 * result + getValues().hashCode();
        return result;
    }

    /**
     * Factory method to create a permanently failed message from an existing message.
     * @param message Message to make permanently failed.
     * @return A new Message object that is marked as permanently failed.
     */
    public static Message createPermanentlyFailedMessage(final Message message) {
        return new Message(message.getMessageId(), message.getValues(), true);
    }
}
