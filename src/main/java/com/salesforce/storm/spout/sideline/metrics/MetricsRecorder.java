package com.salesforce.storm.spout.sideline.metrics;


import java.util.concurrent.Callable;

public interface MetricsRecorder {

    /**
     * Count a metric, given a name, increments it by 1.
     */
    void count(Class sourceClass, String metricName);

    /**
     * Count a metric, given a name, increments it by value.
     */
    void count(Class sourceClass, String metricName, long incrementBy);


    /**
     * Gauge a metric, given a name, by a specify value.
     */
    void average(Class sourceClass, String metricName, Object value);

    /**
     * Gauge the execution time, given a name and scope, for the Callable code (you should use a lambda!)
     *
     * A scope is a secondary keyspace, so Foo.Bar as a metric name.
     *
     * @param callable Some code that you want to time when it runs
     * @return The result of the Callable, whatever they might be
     * @throws Exception Hopefully whatever went wrong in your callable
     */
    <T> T timer(Class sourceClass, final String metricName, Callable<T> callable) throws Exception;

    /**
     * Gauge the execution time, given a name and scope, for the Callable code (you should use a lambda!)
     *
     * A scope is a secondary keyspace, so Foo.Bar as a metric name.
     *
     * @throws Exception Hopefully whatever went wrong in your callable
     */
    void timer(Class sourceClass, String metricName, long timeInMs) throws Exception;

}
