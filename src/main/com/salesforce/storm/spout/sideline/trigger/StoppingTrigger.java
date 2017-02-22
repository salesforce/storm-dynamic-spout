package com.salesforce.storm.spout.sideline.trigger;

import com.salesforce.storm.spout.sideline.SidelineSpout;

public interface StoppingTrigger {

    void stop();

    void setSidelineSpout(SidelineSpout sidelineSpout);
}
