package com.salesforce.storm.spout.sideline.filter;

import com.salesforce.storm.spout.sideline.KafkaMessage;

public class NegatingFilterChainStep implements FilterChainStep {

    final private FilterChainStep step;

    public NegatingFilterChainStep(FilterChainStep step) {
        this.step = step;
    }

    public boolean filter(KafkaMessage message) {
        return !this.step.filter(message);
    }
}
