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

package com.salesforce.storm.spout.dynamic.metrics;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.time.Clock;
import java.util.Map;

/**
 * Metrics recorder that dumps metrics to logs.
 *
 * This is useful in your development and test environments.
 */
public class LogRecorder implements MetricsRecorder {

    private static final Logger logger = LoggerFactory.getLogger(LogRecorder.class);
    private final Map<String, Long> counters = Maps.newConcurrentMap();
    private final Map<String, CircularFifoBuffer> averages = Maps.newConcurrentMap();
    private final Map<String, Object> assignedValues = Maps.newConcurrentMap();

    // For storing timer start values
    private final Map<String, Long> timerStartValues = Maps.newConcurrentMap();

    @Override
    public void open(SpoutConfig spoutConfig, TopologyContext topologyContext) {
    }

    @Override
    public void close() {
        // Noop
    }

    @Override
    public void count(final MetricDefinition metric) {
        countBy(metric, 1L, new Object[0]);
    }

    @Override
    public void count(final MetricDefinition metric, final Object... metricParameters) {
        countBy(metric, 1L, metricParameters);

    }

    @Override
    public void countBy(final MetricDefinition metric, final long incrementBy) {
        countBy(metric, incrementBy, new Object[0]);
    }

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
    public void assignValue(final MetricDefinition metric, final Object value) {
        assignValue(metric, value, new Object[0]);
    }

    @Override
    public void startTimer(final MetricDefinition metric, final Object... metricParameters) {
        final String key = generateKey(metric, metricParameters);
        timerStartValues.put(key, Clock.systemUTC().millis());
    }

    @Override
    public void startTimer(final MetricDefinition metric) {
        startTimer(metric, new Object[0]);
    }

    @Override
    public void stopTimer(final MetricDefinition metric, final Object... metricParameters) {
        final long stopTime = Clock.systemUTC().millis();

        final String key = generateKey(metric, metricParameters);
        final Long startTime = timerStartValues.get(key);

        if (startTime == null) {
            logger.warn("Could not find timer key {}", key);
            return;
        }
        logger.debug("[TIMER] {} + {}", key, stopTime - startTime);
    }

    @Override
    public void stopTimer(final MetricDefinition metric) {
        stopTimer(metric, new Object[0]);
    }

    private String generateKey(final MetricDefinition metric, final Object[] parameters) {
        return MessageFormatter.format(metric.getKey(), parameters).getMessage();
    }
}
