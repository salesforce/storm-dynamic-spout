package com.salesforce.storm.spout.sideline.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class LogRecorder implements MetricsRecorder {

    private static final Logger logger = LoggerFactory.getLogger(LogRecorder.class);

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
        logger.debug("[COUNTER] {} = {}", metric, value);
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
        logger.debug("[COUNTER] {} {} = {}", metric, scope, value);
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
        logger.debug("[GAUGE] {} = {}", metric, value);
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
        logger.debug("[GAUGE] {} {} = {}", metric, scope, value);
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
        logger.debug("[GAUGE] {} = {}", metric, end - start);
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
        logger.debug("[GAUGE] {} {} = {}", metric, scope, end - start);
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
