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
import com.salesforce.storm.spout.dynamic.coordinator.SpoutCoordinator;
import com.salesforce.storm.spout.dynamic.metrics.annotation.Documentation;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Metrics for the {@link com.salesforce.storm.spout.dynamic.DynamicSpout} and friends.
 */
public final class Metrics {

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Size of internal MessageBuffer."
    )
    public static final MetricDefinition SPOUT_COORDINATOR_BUFFER_SIZE = new ClassMetric(SpoutCoordinator.class, "bufferSize");

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The number of running VirtualSpout instances."
    )
    public static final MetricDefinition SPOUT_COORDINATOR_RUNNING = new ClassMetric(SpoutCoordinator.class, "running");

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The number of queued VirtualSpout instances."
    )
    public static final MetricDefinition SPOUT_COORDINATOR_QUEUED = new ClassMetric(SpoutCoordinator.class, "queued");

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The number of errored VirtualSpout instances."
    )
    public static final MetricDefinition SPOUT_COORDINATOR_ERRORED = new ClassMetric(SpoutCoordinator.class, "errored");

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The number of completed VirtualSpout instances."
    )
    public static final MetricDefinition SPOUT_COORDINATOR_COMPLETED = new ClassMetric(SpoutCoordinator.class, "completed");

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The max number of VirtualSpout instances that will be run concurrently."
    )
    public static final MetricDefinition SPOUT_COORDINATOR_POOL_SIZE = new ClassMetric(SpoutCoordinator.class, "poolSize");

    @Documentation(
        type = Documentation.Type.COUNTER,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Tuple ack count per VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    public static final MetricDefinition VIRTUAL_SPOUT_ACK = new ClassMetric(VirtualSpout.class, "{}.ack");

    @Documentation(
        type = Documentation.Type.COUNTER,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Messages who have exceeded the maximum configured retry count per VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    public static final MetricDefinition VIRTUAL_SPOUT_EXCEEDED_RETRY_LIMIT = new ClassMetric(VirtualSpout.class, "{}.exceededRetryLimit");

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "How many Filters are being applied against the VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    public static final MetricDefinition VIRTUAL_SPOUT_NUMBER_FILTERS_APPLIED =
        new ClassMetric(VirtualSpout.class, "{}.numberFiltersApplied");

    @Documentation(
        type = Documentation.Type.COUNTER,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Tuple emit count per VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    public static final MetricDefinition VIRTUAL_SPOUT_EMIT = new ClassMetric(VirtualSpout.class, "{}.emit");

    @Documentation(
        type = Documentation.Type.COUNTER,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Tuple fail count per VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    public static final MetricDefinition VIRTUAL_SPOUT_FAIL = new ClassMetric(VirtualSpout.class, "{}.fail");

    @Documentation(
        type = Documentation.Type.COUNTER,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Filtered messages per VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    public static final MetricDefinition VIRTUAL_SPOUT_FILTERED = new ClassMetric(VirtualSpout.class, "{}.filtered");

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Total number of messages to be processed by the VirtualSpout for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    public static final MetricDefinition VIRTUAL_SPOUT_PARTITION_TOTAL_MESSAGES =
        new ClassMetric(VirtualSpout.class, "{}.partition.{}.totalMessages");

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Number of messages processed by the VirtualSpout instance for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    public static final MetricDefinition VIRTUAL_SPOUT_PARTITION_TOTAL_PROCESSED =
        new ClassMetric(VirtualSpout.class, "{}.partition.{}.totalProcessed");

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Number of messages remaining to be processed by the VirtualSpout instance for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    public static final MetricDefinition VIRTUAL_SPOUT_PARTITION_TOTAL_UNPROCESSED =
        new ClassMetric(VirtualSpout.class, "{}.partition.{}.totalUnprocessed");

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "Percentage of messages processed out of the total for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    public static final MetricDefinition VIRTUAL_SPOUT_PARTITION_PERCENT_COMPLETE =
        new ClassMetric(VirtualSpout.class, "{}.partition.{}.percentComplete");

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.PERCENT,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The starting offset position for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    public static final MetricDefinition VIRTUAL_SPOUT_PARTITION_STARTING_OFFSET =
        new ClassMetric(VirtualSpout.class, "{}.partition.{}.startingOffset");

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The offset currently being processed for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    public static final MetricDefinition VIRTUAL_SPOUT_PARTITION_CURRENT_OFFSET =
        new ClassMetric(VirtualSpout.class, "{}.partition.{}.currentOffset");

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.DYNAMIC_SPOUT,
        description = "The ending offset for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    public static final MetricDefinition VIRTUAL_SPOUT_PARTITION_ENDING_OFFSET =
        new ClassMetric(VirtualSpout.class, "{}.partition.{}.endingOffset");
}
