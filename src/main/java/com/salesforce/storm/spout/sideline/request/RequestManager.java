package com.salesforce.storm.spout.sideline.request;

import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import com.salesforce.storm.spout.sideline.trigger.SidelineIdentifier;

public interface RequestManager {

    ConsumerState get(SidelineIdentifier id);

    void set(SidelineIdentifier id, ConsumerState state);
}
