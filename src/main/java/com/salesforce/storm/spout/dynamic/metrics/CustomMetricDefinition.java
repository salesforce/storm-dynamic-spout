package com.salesforce.storm.spout.dynamic.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class CustomMetricDefinition implements MetricDefinition {
    private final String context;
    private final String key;

    public CustomMetricDefinition(final String context, final String key) {
        this.context = context;
        this.key = key;
    }

    @Override
    public String getContext() {
        return context;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "CustomMetricDefinition{"
            + "context='" + context + '\''
            + ", key='" + key + '\''
            + '}';
    }
}
