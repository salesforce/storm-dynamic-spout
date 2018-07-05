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

package com.salesforce.storm.spout.dynamic.metrics;

import com.google.common.collect.Maps;
import org.apache.storm.metric.api.AssignableMetric;
import org.apache.storm.metric.api.IMetric;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple implementation of a MultiAssignableMetric.
 */
public class MultiAssignableMetric implements IMetric {
    private final Map<String, AssignableMetric> values = Maps.newConcurrentMap();

    public MultiAssignableMetric() {
    }

    public AssignableMetric scope(String key) {
        return this.values.computeIfAbsent(key, k -> new AssignableMetric(null));
    }

    /**
     * Get the values stored and resets internal values.
     * @return values stored and resets internal values
     */
    public Object getValueAndReset() {
        HashMap ret = Maps.newHashMap();

        for (Map.Entry<String, AssignableMetric> entry : this.values.entrySet()) {
            ret.put(entry.getKey(), entry.getValue().getValueAndReset());
        }

        return ret;
    }
}
