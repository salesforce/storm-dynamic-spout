package com.salesforce.storm.spout.sideline.mocks;

import org.apache.storm.metric.api.IMetric;
import org.apache.storm.task.TopologyContext;

import java.util.HashMap;
import java.util.Map;

public class MockTopologyContext extends TopologyContext {

    public Map<String, IMetric> mockRegisteredMetrics = new HashMap<>();
    public int taskId;

    public MockTopologyContext() {
        super(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    @Override
    public <T extends IMetric> T registerMetric(String name, T metric, int timeBucketSizeInSecs) {
        mockRegisteredMetrics.put(name, metric);
        return metric;
    }

    @Override
    public IMetric getRegisteredMetricByName(String name) {
        return mockRegisteredMetrics.get(name);
    }

    @Override
    public int getThisTaskId() {
        return taskId;
    }

}