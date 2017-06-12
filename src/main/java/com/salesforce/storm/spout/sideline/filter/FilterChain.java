/**
 * Copyright (c) 2017, Salesforce.com, Inc.
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
package com.salesforce.storm.spout.sideline.filter;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.Message;
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
    public boolean filter(Message message) {
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
