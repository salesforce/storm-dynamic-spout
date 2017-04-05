package com.salesforce.storm.spout.sideline;

import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;

public class SpoutTriggerProxy {

    final SidelineSpout spout;

    public SpoutTriggerProxy(final SidelineSpout spout) {
        this.spout = spout;
    }

    public SidelineRequestIdentifier startSidelining(final SidelineRequest request) {
        return this.spout.startSidelining(request);
    }

    public void stopSidelining(final SidelineRequest request) {
        this.spout.stopSidelining(request);
    }
}
