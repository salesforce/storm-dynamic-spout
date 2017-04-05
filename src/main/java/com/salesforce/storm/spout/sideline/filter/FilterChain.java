package com.salesforce.storm.spout.sideline.filter;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;

import java.util.Map;

/**
 * Process a filter through a chain of steps, giving the next step the result of the previous one.
 */
public class FilterChain {

    final Map<SidelineRequestIdentifier, FilterChainStep> steps = Maps.newConcurrentMap();

    /**
     * Fluent method for adding steps to the chain (must be done in order).
     *
     * @param step A step for processing in the chain
     * @return The chain instance
     */
    public FilterChain addStep(final SidelineRequestIdentifier id, final FilterChainStep step) {
        this.steps.put(id, step);
        return this;
    }

    public FilterChainStep removeSteps(final SidelineRequestIdentifier id) {
        return this.steps.remove(id);
    }

    /**
     * Process a filter through the chain, get the resulting filter.
     *
     * @param message The filter to be processed by this step of the chain
     * @return Should this message be filtered out? True means yes.
     */
    public boolean filter(KafkaMessage message) {
        // No steps = nothing to filter by
        if (steps.values().isEmpty()) {
            return false;
        }

        for (FilterChainStep step : steps.values()) {
            if (step.filter(message)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Find the identifier for a set of steps.
     *
     * @param seek The list of steps to find
     * @return Identifier for the steps in the chain
     */
    public SidelineRequestIdentifier findStep(FilterChainStep seek) {
        for (Map.Entry<SidelineRequestIdentifier, FilterChainStep> entry : steps.entrySet()) {
            FilterChainStep step = entry.getValue();

            if (step.equals(seek)) {
                return entry.getKey();
            }
        }

        return null;
    }

    public Map<SidelineRequestIdentifier,FilterChainStep> getSteps() {
        return steps;
    }

    @Override
    public String toString() {
        return "FilterChain{"
            + "steps=" + steps
            + '}';
    }
}
