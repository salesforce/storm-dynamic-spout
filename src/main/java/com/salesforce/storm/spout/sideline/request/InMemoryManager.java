package com.salesforce.storm.spout.sideline.request;

import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import com.salesforce.storm.spout.sideline.trigger.SidelineIdentifier;

import java.util.HashMap;
import java.util.Map;

// TODO: Need an implementation that uses a persistence state manager
public class InMemoryManager implements RequestManager {

    private Map<SidelineIdentifier, ConsumerState> state = new HashMap<>();

    public ConsumerState get(SidelineIdentifier id) {
        return this.state.get(id);
    }

    public void set(SidelineIdentifier id, ConsumerState state) {
        this.state.put(id, state);
    }
}
