package com.salesforce.storm.spout.dynamic.metrics;

/**
 *
 */
public interface MetricDefinition {
    String getContext();
    String getKey();
}
