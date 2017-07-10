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
package com.salesforce.storm.spout.sideline.metrics;

import com.google.common.collect.Maps;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * A wrapper for recording metrics in Storm
 *
 * Learn more about Storm metrics here: http://storm.apache.org/releases/1.0.1/Metrics.html
 *
 * Use this as an instance variable on your bolt, make sure to create it inside of prepareBolt()
 * and pass it down stream to any classes that need to track metrics in your application.
 *
 * This will report metrics in the following format:
 *
 * Averaged Values: AVERAGES.[className].[metricName]
 * Gauge Values: GAUGES.[className].[metricName]
 * Timed Values: TIMERS.[className].[metricName]
 * Counter Values: COUNTERS.[className].[metricName]
 *
 */
public class StormRecorder implements MetricsRecorder {

    private static final Logger logger = LoggerFactory.getLogger(StormRecorder.class);

    private MultiReducedMetric averagedValues;
    private MultiAssignableMetric assignedValues;
    private MultiReducedMetric timers;
    private MultiCountMetric counters;

    // For storing timer start values
    private final Map<String, Long> timerStartValues = Maps.newConcurrentMap();

    @Override
    public void open(final Map spoutConfig, final TopologyContext topologyContext) {
        // Configuration items, hardcoded for now.
        final int timeBucket = 60;

        // Register the top level metrics.
        averagedValues = topologyContext.registerMetric("AVERAGES", new MultiReducedMetric(new MeanReducer()), timeBucket);
        assignedValues = topologyContext.registerMetric("GAUGES", new MultiAssignableMetric(), timeBucket);
        timers = topologyContext.registerMetric("TIMERS", new MultiReducedMetric(new MeanReducer()), timeBucket);
        counters = topologyContext.registerMetric("COUNTERS", new MultiCountMetric(), timeBucket);
    }

    @Override
    public void close() {
        // Noop
    }

    @Override
    public void count(Class sourceClass, String metricName) {
        count(sourceClass, metricName, 1);
    }

    @Override
    public void count(Class sourceClass, String metricName, long incrementBy) {
        // Generate key
        final String key = generateKey(sourceClass, metricName);

        counters.scope(key).incrBy(incrementBy);
    }

    @Override
    public void averageValue(Class sourceClass, String metricName, Object value) {
        final String key = generateKey(sourceClass, metricName);

        averagedValues.scope(key).update(value);
    }

    @Override
    public void assignValue(Class sourceClass, String metricName, Object value) {
        final String key = generateKey(sourceClass, metricName);
        assignedValues.scope(key).setValue(value);
    }

    /**
     * Gauge the execution time, given a name and scope, for the Callable code (you should use a lambda!).
     */
    public <T> T timer(Class sourceClass, final String metricName, Callable<T> callable) throws Exception {
        // Wrap in timing
        final long start = Clock.systemUTC().millis();
        T result = callable.call();
        final long end = Clock.systemUTC().millis();

        // Update
        timer(sourceClass, metricName, (end - start));

        // return result.
        return result;
    }

    public void timer(Class sourceClass, String metricName, long timeInMs) {
        final String key = generateKey(sourceClass, metricName);
        timers.scope(key).update(timeInMs);
    }

    @Override
    public void startTimer(Class sourceClass, String metricName) {
        final String key = generateKey(sourceClass, metricName);
        timerStartValues.put(key, Clock.systemUTC().millis());
    }

    @Override
    public void stopTimer(Class sourceClass, String metricName) {
        final long stopTime = Clock.systemUTC().millis();

        final String key = generateKey(sourceClass, metricName);
        final Long startTime = timerStartValues.get(key);

        if (startTime == null) {
            logger.warn("Could not find timer key {}", key);
            return;
        }
        timer(sourceClass, metricName, stopTime - startTime);
    }
    
    private String generateKey(Class sourceClass, String metricName) {
        return sourceClass.getSimpleName() + "." + metricName;
    }
}
