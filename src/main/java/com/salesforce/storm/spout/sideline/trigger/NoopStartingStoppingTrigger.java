package com.salesforce.storm.spout.sideline.trigger;

import com.salesforce.storm.spout.sideline.SpoutTriggerProxy;

/**
 * Noop starting/stopping trigger
 */
public class NoopStartingStoppingTrigger implements StartingTrigger, StoppingTrigger {

    public void setSidelineSpout(SpoutTriggerProxy spout) {
        // Noop
    }
}
