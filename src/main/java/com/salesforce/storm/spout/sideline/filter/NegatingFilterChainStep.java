package com.salesforce.storm.spout.sideline.filter;

import com.salesforce.storm.spout.sideline.Message;

public class NegatingFilterChainStep implements FilterChainStep {

    private final FilterChainStep step;

    public NegatingFilterChainStep(FilterChainStep step) {
        this.step = step;
    }

    public boolean filter(Message message) {
        return !this.step.filter(message);
    }
}
