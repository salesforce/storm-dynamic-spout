package com.salesforce.storm.spout.sideline.trigger;

public class StopRequest {

    final public SidelineIdentifier id;

    public StopRequest(final SidelineIdentifier id) {
        this.id = id;
    }
}
