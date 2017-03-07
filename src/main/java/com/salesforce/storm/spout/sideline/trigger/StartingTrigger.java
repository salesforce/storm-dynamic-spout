package com.salesforce.storm.spout.sideline.trigger;

import com.salesforce.storm.spout.sideline.SpoutTriggerProxy;

public interface StartingTrigger extends Trigger {

    void setSidelineSpout(SpoutTriggerProxy spout);
}
