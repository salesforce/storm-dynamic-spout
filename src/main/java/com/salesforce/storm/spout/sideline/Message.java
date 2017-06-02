package com.salesforce.storm.spout.sideline;

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
     * Constructor.
     * @param messageId - contains information about what Topic, Partition, Offset, and Consumer this
     * @param values - contains the values that will be emitted out to the Storm Topology.
     */
    public Message(MessageId messageId, Values values) {
        this.messageId = messageId;
        this.values = values;
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

    @Override
    public String toString() {
        return "Message{"
                + "messageId=" + messageId
                + ", values=" + values
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
        return getValues().equals(that.getValues());
    }

    @Override
    public int hashCode() {
        int result = getMessageId().hashCode();
        result = 31 * result + getValues().hashCode();
        return result;
    }
}
