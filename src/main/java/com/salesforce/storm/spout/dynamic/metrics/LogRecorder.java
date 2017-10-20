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
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.Callable;

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
    public void open(Map spoutConfig, TopologyContext topologyContext) {
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
        final String key = generateKey(sourceClass, metricName);
        synchronized (counters) {
            final long newValue = counters.getOrDefault(key, 0L) + incrementBy;
            counters.put(key, newValue);
            logger.debug("[COUNTER] {} = {}", key, newValue);
        }
    }

    @Override
    public void averageValue(Class sourceClass, String metricName, Object value) {
        if (!(value instanceof Number)) {
            // Dunno what to do?
            return;
        }
        final String key = generateKey(sourceClass, metricName);

        synchronized (averages) {
            if (!averages.containsKey(key)) {
                averages.put(key, new CircularFifoBuffer(64));
            }
            averages.get(key).add(value);

            // TODO - make this work, for now assume double values?
            // Now calculate average using the ring buffer.
            Number total = 0;
            for (Object entry: averages.get(key)) {
                total = total.doubleValue() + ((Number)entry).doubleValue();
            }
            logger.debug("[AVERAGE] {} => {}", key, (total.doubleValue() / averages.get(key).size()));
        }
    }

    @Override
    public void assignValue(Class sourceClass, String metricName, Object value) {
        final String key = generateKey(sourceClass, metricName);
        assignedValues.put(key, value);
        logger.debug("[ASSIGNED] {} => {}", key, value);
    }

    @Override
    public <T> T timer(Class sourceClass, String metricName, Callable<T> callable) throws Exception {
        // Wrap in timing
        final long start = Clock.systemUTC().millis();
        T result = callable.call();
        final long end = Clock.systemUTC().millis();

        // Update
        timer(sourceClass, metricName, (end - start));

        // return result.
        return result;
    }

    @Override
    public void timer(Class sourceClass, String metricName, long timeInMs) {
        logger.debug("[TIMER] {} + {}", generateKey(sourceClass, metricName), timeInMs);
    }

    @Override
    public void startTimer(Class sourceClass, String metricName) {
        final String key = generateKey(sourceClass, metricName);
        timerStartValues.put(key, Clock.systemUTC().millis());
    }

    @Override
    public long stopTimer(Class sourceClass, String metricName) {
        final long stopTime = Clock.systemUTC().millis();

        final String key = generateKey(sourceClass, metricName);
        final Long startTime = timerStartValues.get(key);

        if (startTime == null) {
            logger.warn("Could not find timer key {}", key);
            return -1;
        }
        final long totalTime = stopTime - startTime;
        timer(sourceClass, metricName, totalTime);
        return totalTime;
    }

    private String generateKey(Class sourceClass, String metricName) {
        return sourceClass.getSimpleName() + "." + metricName;
    }
}
