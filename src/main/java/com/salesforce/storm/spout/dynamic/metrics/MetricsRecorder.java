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

import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

/**
 * Any implementation of this should be written to be thread safe.  This instance
 * is definitely shared across multiple threads.
 */
public interface MetricsRecorder {

    /**
     * Performs any required initialization/connection/setup required for
     * the implementation.  By contract, this will be called once prior to calling
     * collecting any metrics.
     * @param config spout configuration.
     * @param topologyContext topology context.
     */
    void open(final AbstractConfig config, final TopologyContext topologyContext);

    /**
     * Perform any cleanup.
     */
    void close();

    /**
     * Count a metric, given a name, increments it by 1.
     * @param metric metric definition.
     */
    void count(final MetricDefinition metric);

    /**
     * Count a metric, given a name, increments it by 1.
     * @param metric metric definition.
     * @param metricParameters when a {@link MetricDefinition} supports interpolation on it's key, for example "foo.{}.bar" the {}
     *                         can be replace with the supplied parameters.
     */
    void count(final MetricDefinition metric, final Object... metricParameters);

    /**
     * Count a metric, given a name, increments it by value.
     * @param metric metric definition.
     * @param incrementBy amount to increment the metric by.
     */
    void countBy(final MetricDefinition metric, final long incrementBy);

    /**
     * Count a metric, given a name and increments by a specific amount.
     * @param metric metric definition.
     * @param incrementBy amount to increment the metric by.
     * @param metricParameters when a {@link MetricDefinition} supports interpolation on it's key, for example "foo.{}.bar" the {}
     *                         can be replace with the supplied parameters.
     */
    void countBy(final MetricDefinition metric, final long incrementBy, final Object... metricParameters);

    /**
     * Assign a value to metric.
     * @param metric metric definition.
     * @param value value to be assigned.
     * @param metricParameters when a {@link MetricDefinition} supports interpolation on it's key, for example "foo.{}.bar" the {}
     *                         can be replace with the supplied parameters.
     */
    void assignValue(final MetricDefinition metric, final Object value, final Object... metricParameters);

    /**
     * Assign a value to metric.
     * @param metric metric definition.
     * @param value value to be assigned.
     */
    void assignValue(final MetricDefinition metric, final Object value);

    /**
     * Starts a timer for the given sourceClass and metricName.
     * @param metric metric definition.
     * @param metricParameters when a {@link MetricDefinition} supports interpolation on it's key, for example "foo.{}.bar" the {}
     *                         can be replace with the supplied parameters.
     */
    void startTimer(final MetricDefinition metric, final Object... metricParameters);

    /**
     * Starts a timer for the given sourceClass and metricName.
     * @param metric metric definition.
     */
    void startTimer(final MetricDefinition metric);

    /**
     * Stops and records a timer for the given sourceClass and metricName.
     * @param metric metric definition.
     * @param metricParameters when a {@link MetricDefinition} supports interpolation on it's key, for example "foo.{}.bar" the {}
     *                         can be replace with the supplied parameters.
     */
    void stopTimer(final MetricDefinition metric, final Object... metricParameters);

    /**
     * Stops and records a timer for the given sourceClass and metricName.
     * @param metric metric definition.
     */
    void stopTimer(final MetricDefinition metric);
}
