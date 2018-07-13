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

import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A wrapper for recording metrics in Storm
 *
 * ONLY USE WITH STORM 1.1 or lower.
 *
 * If you are running Storm 1.2 or higher you should use {@link DropwizardRecorder}
 *
 * Learn more about Storm metrics here: http://storm.apache.org/releases/1.0.1/Metrics.html
 *
 * Use this as an instance variable on your bolt, make sure to create it inside of prepareBolt()
 * and pass it down stream to any classes that need to track metrics in your application.
 *
 * This will report metrics in the following format:
 *
 * Gauge Values: GAUGES.[className].[metricPrefix].[metricName]
 * Timed Values: TIMERS.[className].[metricPrefix].[metricName]
 * Counter Values: COUNTERS.[className].[metricPrefix].[metricName]
 */
@Deprecated
public class StormRecorder implements MetricsRecorder {
    private static final Logger logger = LoggerFactory.getLogger(StormRecorder.class);

    /**
     * Constructs metric keys from a {@link MetricDefinition}.
     */
    private KeyBuilder keyBuilder;

    /**
     * Contains a map of Assigned Metrics, which are used to set a metric to a specific value.
     */
    private MultiAssignableMetric assignedValues;

    /**
     * Contains a map of Reduced Metrics, which are used to calculate timings of something over time.
     */
    private MultiReducedMetric timers;

    /**
     * Contains a map of Counter metrics, which are used to count how often something happens,
     * always increasing.
     */
    private MultiCountMetric counters;

    /**
     * Tracks timers start points and provides elapsed time when stopping.
     */
    private final TimerManager timerManager = new TimerManager();

    /**
     * Allow configuring a prefix for metric keys.
     */
    private String metricPrefix = "";

    @Override
    public void open(final Map<String, Object> spoutConfig, final TopologyContext topologyContext) {
        // Load configuration items.

        // Determine our time bucket window, in seconds, defaulted to 60.
        int timeBucketSeconds = 60;
        if (spoutConfig.containsKey(SpoutConfig.METRICS_RECORDER_TIME_BUCKET)) {
            final Object timeBucketCfgValue = spoutConfig.get(SpoutConfig.METRICS_RECORDER_TIME_BUCKET);
            if (timeBucketCfgValue instanceof Number) {
                timeBucketSeconds = ((Number) timeBucketCfgValue).intValue();
            }
        }

        // Conditionally enable prefixing with taskId
        if (spoutConfig.containsKey(SpoutConfig.METRICS_RECORDER_ENABLE_TASK_ID_PREFIX)) {
            final Object taskIdCfgValue = spoutConfig.get(SpoutConfig.METRICS_RECORDER_ENABLE_TASK_ID_PREFIX);
            if (taskIdCfgValue instanceof Boolean && (Boolean) taskIdCfgValue) {
                this.metricPrefix = "task-" + topologyContext.getThisTaskIndex();
            }
        }

        this.keyBuilder = new KeyBuilder(this.metricPrefix);

        // Log how we got configured.
        logger.info("Configured with time window of {} seconds and using taskId prefixes?: {}",
            timeBucketSeconds, Boolean.toString(metricPrefix.isEmpty()));

        // Register the top level metrics.
        assignedValues = topologyContext.registerMetric("GAUGES", new MultiAssignableMetric(), timeBucketSeconds);
        timers = topologyContext.registerMetric("TIMERS", new MultiReducedMetric(new MeanReducer()), timeBucketSeconds);
        counters = topologyContext.registerMetric("COUNTERS", new MultiCountMetric(), timeBucketSeconds);
    }

    @Override
    public void countBy(final MetricDefinition metric, final long incrementBy, final Object... metricParameters) {
        final String key = generateKey(metric, metricParameters);
        counters.scope(key).incrBy(incrementBy);
    }

    @Override
    public void assignValue(final MetricDefinition metric, final Object value, final Object... metricParameters) {
        final String key = generateKey(metric, metricParameters);
        assignedValues.scope(key).setValue(value);
    }

    @Override
    public void startTimer(final MetricDefinition metric, final Object... metricParameters) {
        final String key = generateKey(metric, metricParameters);
        timerManager.start(key);
    }

    @Override
    public long stopTimer(final MetricDefinition metric, final Object... metricParameters) {
        // Build key from the metric
        final String key = generateKey(metric, metricParameters);

        final long elapsedMs = timerManager.stop(key);

        recordTimer(key, elapsedMs);

        return elapsedMs;
    }

    @Override
    public void recordTimer(final MetricDefinition metric, final long elapsedTimeMs, final Object... metricParameters) {
        final String key = generateKey(metric, metricParameters);
        recordTimer(key, elapsedTimeMs);
    }

    /**
     * Internal helper to record the value of a timer.
     * @param key String representation of the key to record the timer under
     * @param elapsedTimeMs How long the timer ran for, in milliseconds.
     */
    private void recordTimer(final String key, final long elapsedTimeMs) {
        // Update averaged timer key
        timers.scope(key).update(elapsedTimeMs);

        // Increment total time counter, this keeps a running count of total time spent in this timer
        counters.scope(key + "_totalTimeMs").incrBy(elapsedTimeMs);
    }

    /**
     * Internal utility class to help generate metric keys.
     *
     * @return in format of: "className.metricPrefix.metricName"
     */
    private String generateKey(final MetricDefinition metric, final Object[] parameters) {
        return keyBuilder.build(metric, parameters);
    }

    /**
     * Package protected getter.
     * @return Configured metric prefix.
     */
    String getMetricPrefix() {
        return metricPrefix;
    }
}
