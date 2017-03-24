package com.salesforce.storm.spout.sideline.trigger;

import com.salesforce.storm.spout.sideline.SpoutTriggerProxy;

import java.util.Map;

public class StaticTrigger implements StartingTrigger, StoppingTrigger {

    private SpoutTriggerProxy sidelineSpout;
    private SidelineRequestIdentifier currentSidelineRequestIdentifier;

    @Override
    public void open(Map config) {

    }

    @Override
    public void close() {

    }

    public void start(SidelineRequestIdentifier sidelineRequestIdentifier) {
        this.currentSidelineRequestIdentifier = sidelineRequestIdentifier;
    }

    public void stop() {

    }

    public void sendStartRequest(SidelineRequest request) {
        this.sidelineSpout.startSidelining(request);
    }

    public void sendStopRequest(SidelineRequest request) {
        this.sidelineSpout.stopSidelining(request);
    }

    public void setSidelineSpout(SpoutTriggerProxy sidelineSpout) {
        this.sidelineSpout = sidelineSpout;
    }

    public SidelineRequestIdentifier getCurrentSidelineRequestIdentifier() {
        return this.currentSidelineRequestIdentifier;
    }
}
