/*
 * Copyright (c) 2017, 2018, Salesforce.com, Inc.
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import org.apache.storm.task.TopologyContext;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@link MetricsRecorder} implementation for Storm 1.2+ based off DropwizardMetrics.
 *
 * This implementation will NOT work with Storm 1.1 or older.  If you are running a pre-1.2 version please checkout {@link StormRecorder}.
 */
public class DropwizardRecorder implements MetricsRecorder {

    private Map<String, Object> spoutConfig;
    private TopologyContext topologyContext;

    private KeyBuilder keyBuilder;
    private TimerManager timerManager = new TimerManager();

    private final Map<MetricDefinition, Timer> timers = new ConcurrentHashMap<>();
    private final Map<MetricDefinition, Counter> counters = new ConcurrentHashMap<>();
    private final Map<MetricDefinition, Histogram> histograms = new ConcurrentHashMap<>();

    @Override
    public void open(final Map<String, Object> spoutConfig, final TopologyContext topologyContext) {
        // Store the spout config for later use
        this.spoutConfig = spoutConfig;
        // Store the TopologyContext for later use when we need to register metrics.
        this.topologyContext = topologyContext;

        this.keyBuilder = new KeyBuilder(null);

    }

    @Override
    public void countBy(MetricDefinition metric, long incrementBy, Object... metricParameters) {
        if (!counters.containsKey(metric)) {
            counters.put(metric, topologyContext.registerCounter(keyBuilder.build(metric, metricParameters)));
        }

        counters.get(metric).inc(incrementBy);
    }

    /**
     * Assign an arbitrary value.
     *
     * This implementation uses a {@link Histogram} because we felt it was closest to the arbitrary value assignment previously done with
     * the {@link MultiAssignableMetric}.
     *
     * This particularly method should be considered experimental at this point as we may revise it before finalizing the release.
     *
     * @param metric metric definition.
     * @param value value to be assigned.
     * @param metricParameters when a {@link MetricDefinition} supports interpolation on it's key, for example "foo.{}.bar" the {}
     */
    @Override
    public void assignValue(MetricDefinition metric, Object value, Object... metricParameters) {
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("Supplied value must be an instance of Number.");
        }

        if (!histograms.containsKey(metric)) {
            histograms.put(metric, topologyContext.registerHistogram(keyBuilder.build(metric, metricParameters)));
        }

        histograms.get(metric).update(
            ((Number) value).longValue()
        );
    }

    @Override
    public void startTimer(MetricDefinition metric, Object... metricParameters) {
        timerManager.start(keyBuilder.build(metric, metricParameters));
    }

    @Override
    public long stopTimer(MetricDefinition metric, Object... metricParameters) {
        final long elapseMs = timerManager.stop(keyBuilder.build(metric, metricParameters));
        recordTimer(metric, elapseMs, metricParameters);
        return elapseMs;
    }

    @Override
    public void recordTimer(MetricDefinition metric, long elapsedTimeMs, Object... metricParameters) {
        if (!timers.containsKey(metric)) {
            timers.put(metric, getTopologyContext().registerTimer(keyBuilder.build(metric, metricParameters)));
        }

        timers.get(metric).update(elapsedTimeMs, TimeUnit.MILLISECONDS);
    }

    private TopologyContext getTopologyContext() {
        if (topologyContext == null) {
            throw new IllegalStateException(
                "TopologyContext is null on the MetricsRecord, "
                    + "which most likely means you've attempted to record a metric before the MetricsRecord has been opened."
            );
        }
        return topologyContext;
    }
}
