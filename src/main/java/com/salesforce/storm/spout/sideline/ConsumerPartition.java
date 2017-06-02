package com.salesforce.storm.spout.sideline;

/**
 * Namespace and partition number DTO object.
 */
public class ConsumerPartition {
    private int hash = 0;
    private final String namespace;
    private final int partition;

    /**
     * Constructor.
     * @param namespace Set the namespace for the Consumer.
     * @param partition Set the partition within the namespace.
     */
    public ConsumerPartition(final String namespace, final int partition) {
        this.namespace = namespace;
        this.partition = partition;
    }

    public int partition() {
        return partition;
    }

    public String namespace() {
        return namespace;
    }

    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        final int prime = 31;
        int result = 1;
        result = prime * result + partition;
        result = prime * result + ((namespace == null) ? 0 : namespace.hashCode());
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ConsumerPartition other = (ConsumerPartition) obj;
        if (partition != other.partition) {
            return false;
        }
        if (namespace == null) {
            if (other.namespace != null) {
                return false;
            }
        } else if (!namespace.equals(other.namespace)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return namespace + "-" + partition;
    }
}
