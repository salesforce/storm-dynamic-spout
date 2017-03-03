package com.salesforce.storm.spout.sideline.metrics;

import com.google.common.base.CaseFormat;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.metric.api.MeanReducer;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.metric.api.ReducedMetric;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
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

    final private Map<Gauge,ReducedMetric> gauges = new HashMap<>();
    final private Map<Gauge,MultiReducedMetric> scopedGauges = new HashMap<>();
    final private Map<Counter,CountMetric> counters = new HashMap<>();
    final private Map<Counter,MultiCountMetric> scopedCounters = new HashMap<>();

    /**
     * Sets up all of the known Counter's and Gauge's in the Topology
     *
     * A counter is additive over the time window
     * A gauge will average all supplied value over the time window
     *
     * @param topologyContext Storm topology context, where all metrics will be registered
     */
    public StormRecorder(final TopologyContext topologyContext) {
        for (Gauge gauge : Gauge.values()) {
            try {
                final String name = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, gauge.toString());
                final boolean isScoped = gauge.getClass().getField(gauge.name()).isAnnotationPresent(Scoped.class);

                logger.debug("Gauge {} isScoped = {}", name, isScoped);

                if (isScoped) {
                    final MultiReducedMetric metric = topologyContext.registerMetric(name, new MultiReducedMetric(new MeanReducer()), 60);
                    scopedGauges.put(gauge, metric);
                } else {
                    final ReducedMetric metric = topologyContext.registerMetric(name, new ReducedMetric(new MeanReducer()), 60);
                    gauges.put(gauge, metric);
                }
            } catch (NoSuchFieldException ex) {
                logger.error("Gauge enum is not setup correctly {}", ex);
            }
        }

        for (Counter counter : Counter.values()) {
            try {
                final String name = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, counter.toString());
                final boolean isScoped = counter.getClass().getField(counter.name()).isAnnotationPresent(Scoped.class);

                logger.debug("Counter {} isScoped = {}", name, isScoped);

                if (isScoped) {
                    final MultiCountMetric metric = topologyContext.registerMetric(name, new MultiCountMetric(), 60);
                    scopedCounters.put(counter, metric);
                } else {
                    final CountMetric metric = topologyContext.registerMetric(name, new CountMetric(), 60);
                    counters.put(counter, metric);
                }
            } catch (NoSuchFieldException ex) {
                logger.error("Counter enum is not setup correctly {}", ex);
            }
        }
    }

    /**
     * Count a metric, given a name, increments it by 1
     *
     * @param metric Name of the metric
     */
    public void count(Counter metric) {
        count(metric, 1L);
    }

    /**
     * Count a metric, given a name and increment it by the supplied value
     *
     * @param metric Name of the metric
     * @param value The value to increment by
     */
    public void count(Counter metric, long value) {
        counters.get(metric).incrBy(value);
    }

    /**
     * Count a metric, given a name and a scope, increment it by the supplied value
     *
     * A scope is a secondary keyspace, so Foo.Bar as a metric name.
     *
     * @param metric Name of the metric
     * @param scope The scope, or the second part of the namespace for the metric
     * @param value The value to increment by
     */
    public void count(Counter metric, final String scope, long value) {
        scopedCounters.get(metric).scope(scope).incrBy(value);
    }

    /**
     * Count a metric, given a name and a scope, increment it by the supplied value
     *
     * This is a short hand for supplying a long as the scope, this would most commonly be an account id
     *
     * @param metric Name of the metric
     * @param scope The scope, or the second part of the namespace for the metric
     * @param value The value to increment by
     */
    public void count(Counter metric, final long scope, long value) {
        count(metric, String.valueOf(scope), value);
    }


    /**
     * Count a metric, given a name and a scope, increments it by 1
     *
     * A scope is a secondary keyspace, so Foo.Bar as a metric name.
     *
     * @param metric Name of the metric
     * @param scope The scope, or the second part of the namespace for the metric
     */
    public void count(Counter metric, final String scope) {
        count(metric, scope, 1L);
    }

    /**
     * Gauge a metric, given a name, by a specify value
     *
     * @param metric Name of the metric
     * @param value The value to include in the average
     */
    public void gauge(Gauge metric, Object value) {
        gauges.get(metric).update(value);
    }

    /**
     * Gauge a metric, given a name and a scope by a specify value
     *
     * A scope is a secondary keyspace, so Foo.Bar as a metric name.
     *
     * @param metric Name of the metric
     * @param scope The scope, or the second part of the namespace for the metric
     * @param value The value to include in the average
     */
    public void gauge(final Gauge metric, final String scope, final Object value) {
        scopedGauges.get(metric).scope(scope).update(value);
    }

    /**
     * Gauge a metric, given a name and a scope by a specify value
     *
     * This is a short hand for supplying a long as the scope, this would most commonly be an account id
     *
     * @param metric Name of the metric
     * @param scope The scope, or the second part of the namespace for the metric
     * @param value The value to include in the average
     */
    public void gauge(final Gauge metric, final long scope, final Object value) {
        gauge(metric, String.valueOf(scope), value);
    }

    /**
     * Gauge the execution time, given a name, for the Callable code (you should use a lambda!)
     *
     * @param metric Name of the metric
     * @param callable Some code that you want to time when it runs
     * @return The result of the Callable, whatever they might be
     * @throws Exception Hopefully whatever went wrong in your callable
     */
    public <T> T timer(Gauge metric, Callable<T> callable) throws Exception {
        final long start = System.currentTimeMillis();
        T result = callable.call();
        final long end = System.currentTimeMillis();
        gauges.get(metric).update(end - start);
        return (T) result;
    }

    /**
     * Gauge the execution time, given a name and scope, for the Callable code (you should use a lambda!)
     *
     * A scope is a secondary keyspace, so Foo.Bar as a metric name.
     *
     * @param metric Name of the metric
     * @param scope The scope, or the second part of the namespace for the metric
     * @param callable Some code that you want to time when it runs
     * @return The result of the Callable, whatever they might be
     * @throws Exception Hopefully whatever went wrong in your callable
     */
    public <T> T timer(Gauge metric, final String scope, Callable<T> callable) throws Exception {
        final long start = System.currentTimeMillis();
        T result = callable.call();
        final long end = System.currentTimeMillis();
        scopedGauges.get(metric).scope(scope).update(end - start);
        return (T) result;
    }

    /**
     * Gauge the execution time, given a name and scope, for the Callable code (you should use a lambda!)
     *
     * This is a short hand for supplying a long as the scope, this would most commonly be an account id
     *
     * @param metric Name of the metric
     * @param scope The scope, or the second part of the namespace for the metric
     * @param callable Some code that you want to time when it runs
     * @return The result of the Callable, whatever they might be
     * @throws Exception Hopefully whatever went wrong in your callable
     */
    public <T> T timer(Gauge metric, final long scope, Callable<T> callable) throws Exception {
        return timer(metric, String.valueOf(scope), callable);
    }
}
