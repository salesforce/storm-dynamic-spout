package com.salesforce.storm.spout.sideline.metrics;

import java.util.concurrent.Callable;

public interface MetricsRecorder {

    /**
     * Count a metric, given a name, increments it by 1
     *
     * @param metric Name of the metric
     */
    void count(Counter metric);

    /**
     * Count a metric, given a name and increment it by the supplied value
     *
     * @param metric Name of the metric
     * @param value The value to increment by
     */
    void count(Counter metric, long value);

    /**
     * Count a metric, given a name and a scope, increment it by the supplied value
     *
     * A scope is a secondary keyspace, so Foo.Bar as a metric name.
     *
     * @param metric Name of the metric
     * @param scope The scope, or the second part of the namespace for the metric
     * @param value The value to increment by
     */
    void count(Counter metric, final String scope, long value);

    /**
     * Count a metric, given a name and a scope, increment it by the supplied value
     *
     * This is a short hand for supplying a long as the scope, this would most commonly be an account id
     *
     * @param metric Name of the metric
     * @param scope The scope, or the second part of the namespace for the metric
     * @param value The value to increment by
     */
    void count(Counter metric, final long scope, long value);


    /**
     * Count a metric, given a name and a scope, increments it by 1
     *
     * A scope is a secondary keyspace, so Foo.Bar as a metric name.
     *
     * @param metric Name of the metric
     * @param scope The scope, or the second part of the namespace for the metric
     */
    void count(Counter metric, final String scope);

    /**
     * Gauge a metric, given a name, by a specify value
     *
     * @param metric Name of the metric
     * @param value The value to include in the average
     */
    void gauge(Gauge metric, Object value);

    /**
     * Gauge a metric, given a name and a scope by a specify value
     *
     * A scope is a secondary keyspace, so Foo.Bar as a metric name.
     *
     * @param metric Name of the metric
     * @param scope The scope, or the second part of the namespace for the metric
     * @param value The value to include in the average
     */
    void gauge(final Gauge metric, final String scope, final Object value);

    /**
     * Gauge a metric, given a name and a scope by a specify value
     *
     * This is a short hand for supplying a long as the scope, this would most commonly be an account id
     *
     * @param metric Name of the metric
     * @param scope The scope, or the second part of the namespace for the metric
     * @param value The value to include in the average
     */
    void gauge(final Gauge metric, final long scope, final Object value);

    /**
     * Gauge the execution time, given a name, for the Callable code (you should use a lambda!)
     *
     * @param metric Name of the metric
     * @param callable Some code that you want to time when it runs
     * @return The result of the Callable, whatever they might be
     * @throws Exception Hopefully whatever went wrong in your callable
     */
    <T> T timer(Gauge metric, Callable<T> callable) throws Exception;

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
    <T> T timer(Gauge metric, final String scope, Callable<T> callable) throws Exception;

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
    <T> T timer(Gauge metric, final long scope, Callable<T> callable) throws Exception;
}
