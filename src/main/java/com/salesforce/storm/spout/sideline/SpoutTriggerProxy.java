package com.salesforce.storm.spout.sideline;

import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;

public class SpoutTriggerProxy {

    final SidelineSpout spout;

    public SpoutTriggerProxy(final SidelineSpout spout) {
        this.spout = spout;
    }

    public void startSidelining(final SidelineRequest request) {
        this.spout.startSidelining(request);
    }

    public void stopSidelining(final SidelineRequest request) {
        this.spout.stopSidelining(request);
    }
}
