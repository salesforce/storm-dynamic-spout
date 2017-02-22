package com.salesforce.storm.spout.sideline.trigger;

import com.salesforce.storm.spout.sideline.filter.FilterChainStep;

import java.util.List;

public class StartRequest {

    final public List<FilterChainStep> steps;

    public StartRequest(final List<FilterChainStep> steps) {
        this.steps = steps;
    }
}
