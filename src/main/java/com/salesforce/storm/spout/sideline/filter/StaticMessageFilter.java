package com.salesforce.storm.spout.sideline.filter;

import com.salesforce.storm.spout.sideline.KafkaMessage;

/**
 * We use this filter in tests because it allows us an easy way to define
 * how a filter behaves.
 */
public class StaticMessageFilter implements FilterChainStep {

    private final boolean shouldFilter;

    public StaticMessageFilter(boolean shouldFilter) {
        this.shouldFilter = shouldFilter;
    }

    public boolean isShouldFilter() {
        return shouldFilter;
    }

    public boolean filter(KafkaMessage message) {
        return shouldFilter;
    }
}
