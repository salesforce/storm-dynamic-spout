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

import org.apache.storm.task.TopologyContext;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Any implementation of this should be written to be thread safe.  This instance
 * is definitely shared across multiple threads.
 */
public interface MetricsRecorder {

    /**
     * Performs any required initialization/connection/setup required for
     * the implementation.  By contract, this will be called once prior to calling
     * collecting any metrics.
     * @param spoutConfig spout configuration.
     * @param topologyContext topology context.
     */
    void open(final Map<String, Object> spoutConfig, final TopologyContext topologyContext);

    /**
     * Perform any cleanup.
     */
    void close();

    /**
     * Count a metric, given a name, increments it by 1.
     * @param sourceClass class the metric originates from.
     * @param metricName name of the metric.
     */
    void count(final Class sourceClass, final String metricName);

    /**
     * Count a metric, given a name, increments it by value.
     * @param sourceClass class the metric originates from.
     * @param metricName name of the metric.
     * @param incrementBy amount to increment the metric by.
     */
    void count(final Class sourceClass, final String metricName, final long incrementBy);


    /**
     * Gauge a metric, given a name, by a specify value.
     * @param sourceClass class the metric originates from.
     * @param metricName name of the metric.
     * @param value value of the metric.
     */
    void averageValue(final Class sourceClass, final String metricName, final Object value);

    /**
     * Assign a value to metric.
     * @param sourceClass class the metric originates from.
     * @param metricName name of the metric.
     * @param value value of the metric.
     */
    void assignValue(final Class sourceClass, final String metricName, final Object value);

    /**
     * Gauge the execution time, given a name and scope, for the Callable code (you should use a lambda!)
     *
     * A scope is a secondary key space, so Foo.Bar as a metric name.
     *
     * @param sourceClass class the metric originates from.
     * @param metricName name of the metric.
     * @param callable Some code that you want to time when it runs
     * @param <T> return type of the callable.
     * @return The result of the Callable, whatever they might be
     * @throws Exception Hopefully whatever went wrong in your callable
     */
    <T> T timer(final Class sourceClass, final String metricName, final Callable<T> callable) throws Exception;

    /**
     * Record the execution time, given a name and scope.
     * @param sourceClass class the metric originates from.
     * @param metricName name of the metric.
     * @param timeInMs time to capture for the metric.
     */
    void timer(final Class sourceClass, final String metricName, final long timeInMs);

    /**
     * Starts a timer for the given sourceClass and metricName.
     * @param sourceClass class the metric originates from.
     * @param metricName name of the metric.
     */
    void startTimer(final Class sourceClass, final String metricName);

    /**
     * Stops and records a timer for the given sourceClass and metricName.
     * @param sourceClass class the metric originates from.
     * @param metricName name of the metric.
     * @return The total time that was recorded for the timer, in milliseconds.
     */
    long stopTimer(final Class sourceClass, final String metricName);
}
