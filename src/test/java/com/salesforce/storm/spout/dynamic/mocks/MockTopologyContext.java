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

package com.salesforce.storm.spout.dynamic.mocks;

import com.google.common.collect.Maps;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.task.TopologyContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Mock for Storm's TopologyContext..
 */
public class MockTopologyContext extends TopologyContext {

    public Map<String, IMetric> mockRegisteredMetrics = Maps.newHashMap();
    public int taskId = 0;
    public int taskIndex = 0;
    public List<Integer> componentTasks = Collections.singletonList(1);

    /**
     * Mock for Storm's TopologyContext..
     */
    public MockTopologyContext() {
        super(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    @Override
    public <T extends IMetric> T registerMetric(String name, T metric, int timeBucketSizeInSecs) {
        mockRegisteredMetrics.put(name, metric);
        return metric;
    }

    @Override
    public IMetric getRegisteredMetricByName(String name) {
        return mockRegisteredMetrics.get(name);
    }

    @Override
    public int getThisTaskId() {
        return taskId;
    }

    public String getThisComponentId() {
        return "Mock";
    }

    public List<Integer> getComponentTasks(String componentId) {
        return componentTasks;
    }

    public int getThisTaskIndex() {
        return taskIndex;
    }
}
