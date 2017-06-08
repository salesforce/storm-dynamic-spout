package com.salesforce.storm.spout.sideline;

import com.salesforce.storm.spout.sideline.handler.SidelineSpoutHandler;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;

public class SpoutTriggerProxy {

    final SidelineSpoutHandler spoutHandler;

    public SpoutTriggerProxy(final SidelineSpoutHandler spoutHandler) {
        this.spoutHandler = spoutHandler;
    }

    public SidelineRequestIdentifier startSidelining(final SidelineRequest request) {
        return this.spoutHandler.startSidelining(request);
    }

    public void stopSidelining(final SidelineRequest request) {
        this.spoutHandler.stopSidelining(request);
    }
}
