package com.salesforce.storm.spout.sideline.filter;

import com.google.common.collect.Lists;
import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.trigger.SidelineIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Process a filter through a chain of steps, giving the next step the result of the previous one
 */
public class FilterChain {

    private static final Logger logger = LoggerFactory.getLogger(FilterChain.class);
    private final Map<SidelineIdentifier,List<FilterChainStep>> steps = new HashMap<>();

    public FilterChain() {
    }

    public boolean hasSteps(final SidelineIdentifier id) {
        return this.steps.containsKey(id);
    }

    /**
     * Fluent method for adding steps to the chain (must be done in order)
     *
     * @param step A step for processing in the chain
     * @return The chain instance
     */
    public FilterChain addStep(final SidelineIdentifier id, final FilterChainStep step) {
        return addSteps(id, Lists.newArrayList(step));
    }

    public FilterChain addSteps(final SidelineIdentifier id, final List<FilterChainStep> steps) {
        this.steps.put(id, steps);
        return this;
    }

    public List<FilterChainStep> removeSteps(final SidelineIdentifier id) {
        return this.steps.remove(id);
    }

    /**
     * Process a filter through the chain, get the resulting filter
     *
     * @param message The filter to be processed by this step of the chain
     * @return The resulting filter after being processed
     */
    public boolean filter(KafkaMessage message) {
        for (List<FilterChainStep> listOfSteps : steps.values()) {
            for (FilterChainStep step : listOfSteps) {
                if (!step.filter(message)) {
                    return false;
                }
            }
        }

        return true;
    }
}
