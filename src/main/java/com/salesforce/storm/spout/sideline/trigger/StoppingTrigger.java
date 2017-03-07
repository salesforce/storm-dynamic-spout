package com.salesforce.storm.spout.sideline.trigger;

import com.salesforce.storm.spout.sideline.SpoutTriggerProxy;

public interface StoppingTrigger extends Trigger {

    void setSidelineSpout(SpoutTriggerProxy spout);
}
