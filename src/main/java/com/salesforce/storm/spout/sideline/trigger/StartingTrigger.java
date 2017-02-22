package com.salesforce.storm.spout.sideline.trigger;

import com.salesforce.storm.spout.sideline.SidelineSpout;

public interface StartingTrigger {

    void start(SidelineIdentifier id);

    void setSidelineSpout(SidelineSpout sidelineSpout);
}
