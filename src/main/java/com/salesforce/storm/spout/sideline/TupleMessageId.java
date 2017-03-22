package com.salesforce.storm.spout.sideline;

import org.apache.kafka.common.TopicPartition;

/**
 * This object is used as the MessageId for Tuples emitted to the Storm topology.
 */
public class TupleMessageId {
    private final String topic;
    private final int partition;
    private final long offset;
    private final String srcVirtualSpoutId;

    /**
     * Constructor.
     * @param topic - the topic this tuple came from.
     * @param partition - the partition this tuple came from.
     * @param offset - the offset this tuple came from.
     * @param srcVirtualSpoutId - the VirtualSpout's identifier this tuple came from.
     */
    public TupleMessageId(final String topic, final int partition, final long offset, final String srcVirtualSpoutId) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.srcVirtualSpoutId = srcVirtualSpoutId;
    }
    
    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public String getSrcVirtualSpoutId() {
        return srcVirtualSpoutId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TupleMessageId that = (TupleMessageId) o;

        if (getPartition() != that.getPartition()) {
            return false;
        }
        if (getOffset() != that.getOffset()) {
            return false;
        }
        if (!getTopic().equals(that.getTopic())) {
            return false;
        }
        return getSrcVirtualSpoutId().equals(that.getSrcVirtualSpoutId());
    }

    @Override
    public int hashCode() {
        int result = getTopic().hashCode();
        result = 31 * result + getPartition();
        result = 31 * result + (int) (getOffset() ^ (getOffset() >>> 32));
        result = 31 * result + getSrcVirtualSpoutId().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "TupleMessageId{"
                + "topic='" + topic + '\''
                + ", partition=" + partition
                + ", offset=" + offset
                + ", srcVirtualSpoutId='" + srcVirtualSpoutId + '\''
                + '}';
    }

    /**
     * Helper method.
     * @return TopicPartition object for this tuple message.
     */
    public TopicPartition getTopicPartition() {
        return new TopicPartition(getTopic(), getPartition());
    }
}
