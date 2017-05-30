package com.salesforce.storm.spout.sideline.filter;

import com.salesforce.storm.spout.sideline.Message;

import java.util.UUID;

/**
 * We use this filter in tests because it allows us an easy way to define
 * how a filter behaves.
 */
public class StaticMessageFilter implements FilterChainStep {

    /**
     * We need a way to make this instance unique from others, so we use a UUID.
     */
    private final UUID uniqueId;

    public StaticMessageFilter() {
        this.uniqueId = UUID.randomUUID();
    }

    @Override
    public boolean filter(Message message) {
        return true;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        StaticMessageFilter that = (StaticMessageFilter) other;

        return uniqueId.equals(that.uniqueId);
    }

    @Override
    public int hashCode() {
        return uniqueId.hashCode();
    }

    @Override
    public String toString() {
        return "StaticMessageFilter{"
            + "uniqueId=" + uniqueId
            + '}';
    }
}
