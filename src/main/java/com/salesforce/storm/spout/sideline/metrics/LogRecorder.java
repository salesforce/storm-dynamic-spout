package com.salesforce.storm.spout.sideline.metrics;

import com.google.common.collect.Maps;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.storm.shade.org.apache.http.annotation.ThreadSafe;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.Callable;

@ThreadSafe
public class LogRecorder implements MetricsRecorder {

    private static final Logger logger = LoggerFactory.getLogger(LogRecorder.class);
    private Map<String, Long> counters;
    private Map<String, CircularFifoBuffer> averages;
    private Map<String, Object> assignedValues;

    @Override
    public void open(Map topologyConfig, TopologyContext topologyContext) {
        // Create concurrent maps
        counters = Maps.newConcurrentMap();
        assignedValues = Maps.newConcurrentMap();
        averages = Maps.newConcurrentMap();
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
        return (T) result;
    }

    @Override
    public void timer(Class sourceClass, String metricName, long timeInMs) {
        logger.debug("[TIMER] {} + {}", generateKey(sourceClass, metricName), timeInMs);
    }

    private String generateKey(Class sourceClass, String metricName) {
        return new StringBuilder(sourceClass.getSimpleName())
                .append(".")
                .append(metricName)
                .toString();
    }
}
