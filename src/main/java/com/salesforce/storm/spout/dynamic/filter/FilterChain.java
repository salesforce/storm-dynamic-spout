/*
 * Copyright (c) 2018, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 *   disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * * Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or promote products
 *   derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.salesforce.storm.spout.dynamic.filter;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.Message;

import java.util.Map;

/**
 * Process a filter through a chain of steps, giving the next step the result of the previous one.
 */
public class FilterChain {

    private final Map<FilterChainStepIdentifier, FilterChainStep> steps = Maps.newConcurrentMap();

    /**
     * Add a step to the filter chain.
     *
     * @param id is of the filter chain step to add the step for.
     * @param step step for processing in the chain.
     * @return filter chain, for fluent a interface.
     */
    public FilterChain addStep(final FilterChainStepIdentifier id, final FilterChainStep step) {
        Preconditions.checkNotNull(id, "FilterChainStepIdentifier is required to add a step.");
        Preconditions.checkNotNull(step, "Cannot add a null FilterChainStep to the FilterChain.");
        this.steps.put(id, step);
        return this;
    }

    /**
     * Remove a step to the filter chain.
     *
     * @param id is of the filter chain step to add the step for.
     * @return filter chain, for fluent a interface.
     */
    public FilterChainStep removeStep(final FilterChainStepIdentifier id) {
        Preconditions.checkNotNull(id, "FilterChainStepIdentifier is required to remove a step.");
        return this.steps.remove(id);
    }

    /**
     * Process a filter through the chain, get the resulting filter.
     *
     * @param message The filter to be processed by this step of the chain
     * @return Should this message be filtered out? True means yes.
     */
    public boolean filter(final Message message) {
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
     * @param seek filter chain step to find.
     * @return Identifier for the steps in the chain
     */
    public FilterChainStepIdentifier findStep(final FilterChainStep seek) {
        for (Map.Entry<FilterChainStepIdentifier, FilterChainStep> entry : steps.entrySet()) {
            FilterChainStep step = entry.getValue();

            if (step.equals(seek)) {
                return entry.getKey();
            }
        }

        return null;
    }

    /**
     * Get a map of the filter chain steps by filter chain step identifier.
     * @return map of the filter chain steps by filter chain step identifier.
     */
    public Map<FilterChainStepIdentifier,FilterChainStep> getSteps() {
        return steps;
    }

    /**
     * Determines if the current filter chain step has a filter chain step.
     * @param filterChainStepIdentifier filter chain step identifier.
     * @return true, the filter chain step exists, false it does not.
     */
    public boolean hasStep(final FilterChainStepIdentifier filterChainStepIdentifier) {
        return steps.containsKey(filterChainStepIdentifier);
    }

    /**
     * Get the filter chain step for the given filter chain step identifier.
     * @param filterChainStepIdentifier filter chain step identifier.
     * @return corresponding filter chain step.
     */
    public FilterChainStep getStep(final FilterChainStepIdentifier filterChainStepIdentifier) {
        return steps.get(filterChainStepIdentifier);
    }

    @Override
    public String toString() {
        return "FilterChain{"
            + "steps=" + steps
            + '}';
    }
}
