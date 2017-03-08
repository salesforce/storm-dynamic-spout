package com.salesforce.storm.spout.sideline.trigger;

import com.salesforce.storm.spout.sideline.filter.FilterChainStep;

import java.util.Collections;
import java.util.List;

public class SidelineRequest {

    final public List<FilterChainStep> steps;

    public SidelineRequest(final List<FilterChainStep> steps) {
        this.steps = steps;
    }

    public SidelineRequest(FilterChainStep step) {
        this.steps = Collections.singletonList(step);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SidelineRequest that = (SidelineRequest) o;

        return steps != null ? steps.equals(that.steps) : that.steps == null;
    }

    @Override
    public int hashCode() {
        return steps != null ? steps.hashCode() : 0;
    }
}
