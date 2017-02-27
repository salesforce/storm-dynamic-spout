package com.salesforce.storm.spout.sideline.filter;

import com.salesforce.storm.spout.sideline.KafkaMessage;

/**
 * We use this filter in tests because it allows us an easy way to adjust
 * how a filter behaves.
 */
public class StaticMessageFilter implements FilterChainStep {

    /**
     * We mark this variable as volatile because in testing scenarios we may
     * adjust its stored value in the main/test thread, and another thread running
     * a consumer reads it.
     */
    private volatile boolean shouldFilter;

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
