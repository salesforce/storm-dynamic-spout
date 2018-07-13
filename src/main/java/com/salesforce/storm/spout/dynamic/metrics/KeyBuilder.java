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

import org.slf4j.helpers.MessageFormatter;

/**
 * Takes a {@link MetricDefinition} and generates a String that can be used as a metric key.
 */
class KeyBuilder {

    private final String keyPrefix;

    /**
     * Create a new {@link KeyBuilder} with the specified prefix.
     *
     * Prefix is optional, if you don't need it simply pass in null to the builder.
     *
     * @param keyPrefix prefix to use when building keys from {@link MetricDefinition} instances
     */
    KeyBuilder(final String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    /**
     * Build metric keys from {@link MetricDefinition} instances with the optionally supplied parameters.
     *
     * Strings are formatted using {@link MessageFormatter}, so if you have parameters "foo" and "bar" for a metric with the name of "test"
     * than you could use a {@link CustomMetric} with "test.{}.{}" in the constructor and pass parameters "foo" and "bar" to make the full
     * metric key of "test.foo.bar".
     *
     * @return format string like: "prefix.metric_name.param1.param2"
     */
    String build(final MetricDefinition metric, final Object[] parameters) {
        final StringBuilder keyBuilder = new StringBuilder();

        // Conditionally add key prefix.
        if (getKeyPrefix() != null && !getKeyPrefix().isEmpty()) {
            keyBuilder
                .append(getKeyPrefix())
                .append(".");
        }

        // Our default implementation should include the simple class name in the key
        keyBuilder.append(
            MessageFormatter.arrayFormat(metric.getKey(), parameters).getMessage()
        );
        return keyBuilder.toString();
    }

    /**
     * Get key prefix.
     * @return key prefix
     */
    private String getKeyPrefix() {
        return keyPrefix;
    }
}
