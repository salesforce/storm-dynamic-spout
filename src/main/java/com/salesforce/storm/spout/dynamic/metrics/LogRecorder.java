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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metrics recorder that dumps metrics to logs.
 *
 * This is useful in your development and test environments.
 */
public class LogRecorder implements MetricsRecorder {

    private static final Logger logger = LoggerFactory.getLogger(LogRecorder.class);

    private final KeyBuilder keyBuilder = new KeyBuilder(null);

    private final Map<String, Long> counters = new ConcurrentHashMap<>();
    private final Map<String, Object> assignedValues = new ConcurrentHashMap<>();

    private final TimerManager timerManager = new TimerManager();

    @Override
    public void countBy(final MetricDefinition metric, final long incrementBy, final Object... metricParameters) {
        final String key = generateKey(metric, metricParameters);
        synchronized (counters) {
            final long newValue = counters.getOrDefault(key, 0L) + incrementBy;
            counters.put(key, newValue);
            logger.debug("[COUNTER] {} = {}", key, newValue);
        }
    }

    @Override
    public void assignValue(final MetricDefinition metric, final Object value, final Object... metricParameters) {
        final String key = generateKey(metric, metricParameters);
        assignedValues.put(key, value);
        logger.debug("[ASSIGNED] {} => {}", key, value);
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

        logger.debug("[TIMER] {} + {}ms", key, elapsedMs);

        return elapsedMs;
    }

    @Override
    public void recordTimer(final MetricDefinition metric, final long elapsedTimeMs, final Object... metricParameters) {
        final String key = generateKey(metric, metricParameters);
        logger.debug("[TIMER] {} + {}ms", key, elapsedTimeMs);
    }

    private String generateKey(final MetricDefinition metric, final Object[] parameters) {
        return keyBuilder.build(metric, parameters);
    }
}
