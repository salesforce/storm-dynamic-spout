package com.salesforce.storm.spout.sideline.filter;

import com.salesforce.storm.spout.sideline.KafkaMessage;

public class StaticMessageFilter implements FilterChainStep {

    private boolean shouldFilter;

    public StaticMessageFilter() {
        this(false);
    }

    public StaticMessageFilter(boolean shouldFilter) {
        this.shouldFilter = shouldFilter;
    }

    public boolean isShouldFilter() {
        return shouldFilter;
    }

    public void setShouldFilter(boolean shouldFilter) {
        this.shouldFilter = shouldFilter;
    }

    public boolean filter(KafkaMessage message) {
        return !shouldFilter;
    }
}
