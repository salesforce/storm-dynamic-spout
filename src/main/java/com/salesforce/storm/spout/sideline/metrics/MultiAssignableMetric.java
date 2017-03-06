package com.salesforce.storm.spout.sideline.metrics;

import org.apache.storm.metric.api.AssignableMetric;
import org.apache.storm.metric.api.IMetric;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class MultiAssignableMetric implements IMetric {
    Map<String, AssignableMetric> values = new HashMap();

    public MultiAssignableMetric() {
    }

    public AssignableMetric scope(String key) {
        AssignableMetric val = this.values.get(key);
        if(val == null) {
            this.values.put(key, val = new AssignableMetric(null));
        }

        return val;
    }

    public Object getValueAndReset() {
        HashMap ret = new HashMap();
        Iterator var2 = this.values.entrySet().iterator();

        while(var2.hasNext()) {
            Map.Entry e = (Map.Entry)var2.next();
            ret.put(e.getKey(), ((AssignableMetric)e.getValue()).getValueAndReset());
        }

        return ret;
    }
}
