/*
 * Copyright (c) 2017, 2018, Salesforce.com, Inc.
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

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test that {@link KeyBuilder} properly formats {@link MetricDefinition} objects into Strings.
 */
class KeyBuilderTest {

    /**
     * Test that a {@link KeyBuilder} with a prefix is prepended with that prefix.
     */
    @Test
    void testWithPrefix() {
        final String prefix = "prefix";
        final String metricName = "custom_metric";
        final MetricDefinition metricDefinition = new CustomMetric(metricName);

        final KeyBuilder keyBuilder = new KeyBuilder(prefix);

        assertThat(
            "Prefix is properly placed before the metric name",
            prefix + "." + metricName,
            equalTo(
                keyBuilder.build(metricDefinition, new Object[0])
            )
        );
    }

    /**
     * Test that a {@link KeyBuilder} without a prefix is formatted correctly.
     */
    @Test
    void testWithOutPrefix() {
        final String metricName = "custom_metric";
        final MetricDefinition metricDefinition = new CustomMetric(metricName);

        final KeyBuilder keyBuilder = new KeyBuilder(null);

        assertThat(
            "Metric without a prefix comes out correctly",
            metricName,
            equalTo(
                keyBuilder.build(metricDefinition, new Object[0])
            )
        );
    }

    /**
     * Test that a {@link KeyBuilder} with parameters have them properly formatted.
     */
    @Test
    void testWithParameters() {
        final String metricName = "custom_metric.{}.{}";
        final MetricDefinition metricDefinition = new CustomMetric(metricName);
        final String param1 = "foo";
        final String param2 = "bar";

        final KeyBuilder keyBuilder = new KeyBuilder(null);

        assertThat(
            "Metric with parameters are formatted properly",
            metricName.substring(0, metricName.length() - 6) + "." + param1 + "." + param2,
            equalTo(
                keyBuilder.build(metricDefinition, new String[]{ param1, param2 })
            )
        );
    }
}