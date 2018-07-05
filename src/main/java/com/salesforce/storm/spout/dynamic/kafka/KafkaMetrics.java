/*
 * Copyright (c) 2018, Salesforce.com, Inc.
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

package com.salesforce.storm.spout.dynamic.kafka;

import com.salesforce.storm.spout.dynamic.metrics.ClassMetric;
import com.salesforce.storm.spout.dynamic.metrics.MetricDefinition;
import com.salesforce.storm.spout.documentation.MetricDocumentation;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Metrics for the {@link com.salesforce.storm.spout.dynamic.kafka.Consumer} and friends.
 */
public final class KafkaMetrics {
    @MetricDocumentation(
        type = MetricDocumentation.Type.GAUGE,
        unit = MetricDocumentation.Unit.NUMBER,
        category = MetricDocumentation.Category.KAFKA,
        description = "Offset consumer has processed.",
        dynamicValues = { "topic", "partition" }
    )
    public static final MetricDefinition KAFKA_CURRENT_OFFSET =
        new ClassMetric(KafkaConsumer.class, "topic.{}.partition.{}.currentOffset");

    @MetricDocumentation(
        type = MetricDocumentation.Type.GAUGE,
        unit = MetricDocumentation.Unit.NUMBER,
        category = MetricDocumentation.Category.KAFKA,
        description = "Offset for TAIL position in the partition.",
        dynamicValues = { "topic", "partition" }
    )
    public static final MetricDefinition KAFKA_END_OFFSET =
        new ClassMetric(KafkaConsumer.class, "topic.{}.partition.{}.endOffset");

    @MetricDocumentation(
        type = MetricDocumentation.Type.GAUGE,
        unit = MetricDocumentation.Unit.NUMBER,
        category = MetricDocumentation.Category.KAFKA,
        description = "Difference between endOffset and currentOffset metrics.",
        dynamicValues = { "topic", "partition" }
    )
    public static final MetricDefinition KAFKA_LAG = new ClassMetric(KafkaConsumer.class, "topic.{}.partition.{}.lag");
}
