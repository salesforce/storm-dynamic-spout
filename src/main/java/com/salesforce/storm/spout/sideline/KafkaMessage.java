package com.salesforce.storm.spout.sideline;

import org.apache.storm.tuple.Values;

/**
 * Represents an abstracted view over MessageId and the Tuple values.
 */
public class KafkaMessage {

    /**
     * TupleMessageId contains information about what Topic, Partition, Offset, and Consumer this
     * message originated from.
     */
    private final TupleMessageId tupleMessageId;

    /**
     * Values contains the values that will be emitted out to the Storm Topology.
     */
    private final Values values;

    /**
     * Constructor.
     * @param tupleMessageId - contains information about what Topic, Partition, Offset, and Consumer this
     * @param values - contains the values that will be emitted out to the Storm Topology.
     */
    public KafkaMessage(TupleMessageId tupleMessageId, Values values) {
        this.tupleMessageId = tupleMessageId;
        this.values = values;
    }

    public TupleMessageId getTupleMessageId() {
        return tupleMessageId;
    }

    public String getTopic() {
        return getTupleMessageId().getTopic();
    }

    public int getPartition() {
        return getTupleMessageId().getPartition();
    }

    public long getOffset() {
        return getTupleMessageId().getOffset();
    }

    public Values getValues() {
        return values;
    }

    @Override
    public String toString() {
        return "KafkaMessage{"
                + "tupleMessageId=" + tupleMessageId
                + ", values=" + values
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KafkaMessage that = (KafkaMessage) o;

        if (!getTupleMessageId().equals(that.getTupleMessageId())) {
            return false;
        }
        return getValues().equals(that.getValues());
    }

    @Override
    public int hashCode() {
        int result = getTupleMessageId().hashCode();
        result = 31 * result + getValues().hashCode();
        return result;
    }
}
