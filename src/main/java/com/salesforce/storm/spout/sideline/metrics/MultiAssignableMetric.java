package com.salesforce.storm.spout.sideline.metrics;

import com.google.common.collect.Maps;
import org.apache.storm.metric.api.AssignableMetric;
import org.apache.storm.metric.api.IMetric;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple implementation of a MultiAssignableMetric.
 */
public class MultiAssignableMetric implements IMetric {
    private final Map<String, AssignableMetric> values = Maps.newHashMap();

    public MultiAssignableMetric() {
    }

    public AssignableMetric scope(String key) {
        return this.values.computeIfAbsent(key, k -> new AssignableMetric(null));
    }

    /**
     * @return - Returns values stored and resets internal values.
     */
    public Object getValueAndReset() {
        HashMap ret = Maps.newHashMap();

        for (Map.Entry<String, AssignableMetric> entry : this.values.entrySet()) {
            ret.put(entry.getKey(), entry.getValue().getValueAndReset());
        }

        return ret;
    }
}
