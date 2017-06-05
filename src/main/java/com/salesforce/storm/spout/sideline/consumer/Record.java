package com.salesforce.storm.spout.sideline.consumer;


import org.apache.storm.tuple.Values;

public class Record {
    private final String namespace;
    private final int partition;
    private final long offset;
    private final Values values;

    public Record(String namespace, int partition, long offset, Values values) {
        this.namespace = namespace;
        this.partition = partition;
        this.offset = offset;
        this.values = values;
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

    public Values getValues() {
        return values;
    }
}
