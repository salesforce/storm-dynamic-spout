package com.salesforce.storm.spout.sideline.metrics;

import com.salesforce.storm.spout.sideline.SidelineSpout;
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
 */
public class StormRecorder implements MetricsRecorder {

    private static final Logger logger = LoggerFactory.getLogger(StormRecorder.class);

    private MultiReducedMetric averagedValues;
    private MultiAssignableMetric assignedValues;
    private MultiReducedMetric timers;
    private MultiCountMetric counters;


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


    private String generateKey(Class sourceClass, String metricName) {
        return sourceClass.getSimpleName() + "." + metricName;
    }
}
