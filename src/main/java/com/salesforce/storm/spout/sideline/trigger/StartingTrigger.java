package com.salesforce.storm.spout.sideline.trigger;

import com.salesforce.storm.spout.sideline.SidelineSpout;

public interface StartingTrigger extends Trigger {

    void start(SidelineIdentifier id);

    void setSidelineSpout(SidelineSpout sidelineSpout);
}
