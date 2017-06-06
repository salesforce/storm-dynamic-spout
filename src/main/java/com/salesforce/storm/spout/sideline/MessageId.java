package com.salesforce.storm.spout.sideline;

import java.time.Clock;

/**
 * This object is used as the MessageId for Tuples emitted to the Storm topology.
 */
public class MessageId {
    private final String namespace;
    private final int partition;
    private final long offset;
    private final VirtualSpoutIdentifier srcVirtualSpoutId;
    private final long timestamp;

    /**
     * Constructor.
     * @param namespace - the namespace this tuple came from.
     * @param partition - the partition this tuple came from.
     * @param offset - the offset this tuple came from.
     * @param srcVirtualSpoutId - the VirtualSpout's identifier this tuple came from.
     */
    public MessageId(final String namespace, final int partition, final long offset, final VirtualSpoutIdentifier srcVirtualSpoutId) {
        this.namespace = namespace;
        this.partition = partition;
        this.offset = offset;
        this.srcVirtualSpoutId = srcVirtualSpoutId;
        this.timestamp = Clock.systemUTC().millis();
    }
    
    public String getNamespace() {
        return namespace;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public VirtualSpoutIdentifier getSrcVirtualSpoutId() {
        return srcVirtualSpoutId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        MessageId that = (MessageId) other;

        if (getPartition() != that.getPartition()) {
            return false;
        }
        if (getOffset() != that.getOffset()) {
            return false;
        }
        if (!getNamespace().equals(that.getNamespace())) {
            return false;
        }
        return getSrcVirtualSpoutId().equals(that.getSrcVirtualSpoutId());
    }

    @Override
    public int hashCode() {
        int result = getNamespace().hashCode();
        result = 31 * result + getPartition();
        result = 31 * result + (int) (getOffset() ^ (getOffset() >>> 32));
        result = 31 * result + getSrcVirtualSpoutId().hashCode();
        return result;
    }

    @Override
    public String toString() {
        final long diff = Clock.systemUTC().millis() - timestamp;

        return "MessageId{"
                + "namespace='" + namespace + '\''
                + ", partition=" + partition
                + ", offset=" + offset
                + ", srcVirtualSpoutId='" + srcVirtualSpoutId + '\''
                + ", timestamp='" + timestamp + " (" + diff + ")" + '\''
                + '}';
    }
}
