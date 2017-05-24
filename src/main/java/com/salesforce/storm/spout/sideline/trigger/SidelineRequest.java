package com.salesforce.storm.spout.sideline.trigger;

import com.salesforce.storm.spout.sideline.filter.FilterChainStep;

public class SidelineRequest {

    public final SidelineRequestIdentifier id;
    public final FilterChainStep step;

    public SidelineRequest(final SidelineRequestIdentifier id, final FilterChainStep step) {
        this.id = id;
        this.step = step;
    }

    @Deprecated
    public SidelineRequest(final FilterChainStep step) {
        this(new SidelineRequestIdentifier(), step);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SidelineRequest that = (SidelineRequest) o;

        return step != null ? step.equals(that.step) : that.step == null;
    }

    @Override
    public int hashCode() {
        return step != null ? step.hashCode() : 0;
    }
}
