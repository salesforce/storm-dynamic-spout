package com.salesforce.storm.spout.sideline;

public class VirtualSpoutIdentifier {

    private final String id;

    public VirtualSpoutIdentifier(final String id) {
        this.id = id;
    }

    public String toString() {
        return id;
    }
}
