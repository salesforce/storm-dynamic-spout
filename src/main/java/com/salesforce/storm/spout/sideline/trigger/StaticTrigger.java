package com.salesforce.storm.spout.sideline.trigger;

import com.salesforce.storm.spout.sideline.SidelineSpout;

import java.util.Map;

public class StaticTrigger implements StartingTrigger, StoppingTrigger {

    private SidelineSpout sidelineSpout;
    private SidelineIdentifier currentSidelineIdentifier;

    @Override
    public void open(Map config) {

    }

    @Override
    public void close() {

    }

    public void start(SidelineIdentifier sidelineIdentifier) {
        this.currentSidelineIdentifier = sidelineIdentifier;
    }

    public void stop() {

    }

    public void sendStartRequest(StartRequest request) {
        this.sidelineSpout.startSidelining(request);
    }

    public void sendStopRequest(StopRequest request) {
        this.sidelineSpout.stopSidelining(request);
    }

    public void setSidelineSpout(SidelineSpout sidelineSpout) {
        this.sidelineSpout = sidelineSpout;
    }

    public SidelineIdentifier getCurrentSidelineIdentifier() {
        return this.currentSidelineIdentifier;
    }
}
