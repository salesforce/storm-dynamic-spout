package com.salesforce.storm.spout.sideline.filter;

import com.google.common.collect.Lists;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Process a filter through a chain of steps, giving the next step the result of the previous one.
 */
public class FilterChain {

    private final Map<SidelineRequestIdentifier,List<FilterChainStep>> steps = new HashMap<>();

    public FilterChain() {
    }

    public boolean hasSteps(final SidelineRequestIdentifier id) {
        return this.steps.containsKey(id);
    }

    /**
     * Fluent method for adding steps to the chain (must be done in order).
     *
     * @param step A step for processing in the chain
     * @return The chain instance
     */
    public FilterChain addStep(final SidelineRequestIdentifier id, final FilterChainStep step) {
        return addSteps(id, Lists.newArrayList(step));
    }

    public FilterChain addSteps(final SidelineRequestIdentifier id, final List<FilterChainStep> steps) {
        this.steps.put(id, steps);
        return this;
    }

    public List<FilterChainStep> removeSteps(final SidelineRequestIdentifier id) {
        return this.steps.remove(id);
    }

    /**
     * Process a filter through the chain, get the resulting filter.
     *
     * @param message The filter to be processed by this step of the chain
     * @return The resulting filter after being processed
     */
    public boolean filter(KafkaMessage message) {
        // No steps = nothing to filter by
        if (steps.values().isEmpty()) {
            return false;
        }

        for (List<FilterChainStep> listOfSteps : steps.values()) {
            for (FilterChainStep step : listOfSteps) {
                if (!step.filter(message)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Find the identifier for a set of steps.
     *
     * @param seek The list of steps to find
     * @return Identifier for the steps in the chain
     */
    public SidelineRequestIdentifier findSteps(List<FilterChainStep> seek) {
        for (Map.Entry<SidelineRequestIdentifier, List<FilterChainStep>> entry : steps.entrySet()) {
            List<FilterChainStep> listOfSteps = entry.getValue();

            if (listOfSteps.equals(seek)) {
                return entry.getKey();
            }
        }

        return null;
    }

    /**
     * Find the identifier for a set that only had one step.
     *
     * @param seek The step to find
     * @return Identifier for the steps in the chain
     */
    public SidelineRequestIdentifier findStep(FilterChainStep seek) {
        return findSteps(Collections.singletonList(seek));
    }

    public Map<SidelineRequestIdentifier,List<FilterChainStep>> getSteps() {
        return steps;
    }

    @Override
    public String toString() {
        return "FilterChain{"
            + "steps=" + steps
            + '}';
    }
}
