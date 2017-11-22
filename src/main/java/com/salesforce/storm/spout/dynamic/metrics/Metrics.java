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

import com.salesforce.storm.spout.dynamic.VirtualSpout;
import com.salesforce.storm.spout.dynamic.coordinator.SpoutMonitor;
import com.salesforce.storm.spout.dynamic.metrics.annotation.Documentation;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Enumeration of all available metrics.
 */
public enum Metrics implements MetricDefinition {

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Size of internal MessageBuffer."
    )
    SPOUT_MONITOR_BUFFER_SIZE(SpoutMonitor.class, "bufferSize"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The number of running VirtualSpout instances."
    )
    SPOUT_MONITOR_RUNNING(SpoutMonitor.class, "running"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The number of queued VirtualSpout instances."
    )
    SPOUT_MONITOR_QUEUED(SpoutMonitor.class, "queued"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The number of errored VirtualSpout instances."
    )
    SPOUT_MONITOR_ERRORED(SpoutMonitor.class, "errored"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The number of completed VirtualSpout instances."
    )
    SPOUT_MONITOR_COMPLETED(SpoutMonitor.class, "completed"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The max number of VirtualSpout instances that will be run concurrently."
    )
    SPOUT_MONITOR_POOL_SIZE(SpoutMonitor.class, "poolSize"),

    @Documentation(
        type = Documentation.Type.COUNTER,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Tuple ack count per VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    VIRTUAL_SPOUT_ACK(VirtualSpout.class, "{}.ack"),

    @Documentation(
        type = Documentation.Type.COUNTER,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Messages who have exceeded the maximum configured retry count per VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    VIRTUAL_SPOUT_EXCEEDED_RETRY_LIMIT(VirtualSpout.class, "{}.exceededRetryLimit"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "How many Filters are being applied against the VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    VIRTUAL_SPOUT_NUMBER_FILTERS_APPLIED(VirtualSpout.class, "{}.numberFiltersApplied"),

    @Documentation(
        type = Documentation.Type.COUNTER,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Tuple emit count per VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    VIRTUAL_SPOUT_EMIT(VirtualSpout.class, "{}.emit"),

    @Documentation(
        type = Documentation.Type.COUNTER,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Tuple fail count per VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    VIRTUAL_SPOUT_FAIL(VirtualSpout.class, "{}.fail"),

    @Documentation(
        type = Documentation.Type.COUNTER,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Filtered messages per VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    VIRTUAL_SPOUT_FILTERED(VirtualSpout.class, "{}.filtered"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Total number of messages to be processed by the VirtualSpout for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    VIRTUAL_SPOUT_PARTITION_TOTAL_MESSAGES(VirtualSpout.class, "{}.partition.{}.totalMessages"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Number of messages processed by the VirtualSpout instance for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    VIRTUAL_SPOUT_PARTITION_TOTAL_PROCESSED(VirtualSpout.class, "{}.partition.{}.totalProcessed"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Number of messages remaining to be processed by the VirtualSpout instance for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    VIRTUAL_SPOUT_PARTITION_TOTAL_UNPROCESSED(VirtualSpout.class, "{}.partition.{}.totalUnprocessed"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Percentage of messages processed out of the total for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    VIRTUAL_SPOUT_PARTITION_PERCENT_COMPLETE(VirtualSpout.class, "{}.partition.{}.percentComplete"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.PERCENT,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The starting offset position for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    VIRTUAL_SPOUT_PARTITION_STARTING_OFFSET(VirtualSpout.class, "{}.partition.{}.startingOffset"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The offset currently being processed for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    VIRTUAL_SPOUT_PARTITION_CURRENT_OFFSET(VirtualSpout.class, "{}.partition.{}.currentOffset"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The ending offset for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    VIRTUAL_SPOUT_PARTITION_ENDING_OFFSET(VirtualSpout.class, "{}.partition.{}.endingOffset"),

    /*
     * Kafka Consumer Metrics.
     */

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.KAFKA,
        description = "Offset consumer has processed.",
        dynamicValues = { "topic", "partition" }
    )
    KAFKA_CURRENT_OFFSET(KafkaConsumer.class, "topic.{}.partition.{}.currentOffset"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.KAFKA,
        description = "Offset for TAIL position in the partition.",
        dynamicValues = { "topic", "partition" }
    )
    KAFKA_END_OFFSET(KafkaConsumer.class, "topic.{}.partition.{}.endOffset"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.KAFKA,
        description = "Difference between endOffset and currentOffset metrics.",
        dynamicValues = { "topic", "partition" }
    )
    KAFKA_LAG(KafkaConsumer.class, "topic.{}.partition.{}.lag");

    /**
     * Context of the metric.
     */
    private final Class context;

    /**
     * Name/Key of the metric.
     */
    private final String key;

    /**
     * Constructor.
     * @param context Context of the metric.
     * @param key Name of the metric.
     */
    Metrics(final Class context, final String key) {
        this.context = context;
        this.key = key;
    }

    @Override
    public String getKey() {
        return this.context.getSimpleName().concat(".").concat(key);
    }

    @Override
    public String toString() {
        return "Metrics{"
            + "context='" + context + '\''
            + ", key='" + key + '\''
            + '}';
    }
}
