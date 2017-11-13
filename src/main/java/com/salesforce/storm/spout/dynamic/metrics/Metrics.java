package com.salesforce.storm.spout.dynamic.metrics;

import com.salesforce.storm.spout.dynamic.metrics.annotation.Documentation;
import com.sun.scenario.effect.Offset;
import org.apache.storm.shade.com.codahale.metrics.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enumeration of all available metrics.
 */
public enum Metrics implements MetricDefinition {

    /*
     * Spout Monitor Metrics.
     */
    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.SPOUT_MONITOR,
        description = "Size of internal MessageBuffer."
    )
    spoutMonitor_bufferSize("SpoutMonitor", "bufferSize"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.SPOUT_MONITOR,
        description = "The number of running VirtualSpout instances."
    )
    spoutMonitor_running("SpoutMonitor", "running"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.SPOUT_MONITOR,
        description = "The number of queued VirtualSpout instances."
    )
    spoutMonitor_queued("SpoutMonitor", "queued"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.SPOUT_MONITOR,
        description = "The number of errored VirtualSpout instances."
    )
    spoutMonitor_errored("SpoutMonitor", "errored"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.SPOUT_MONITOR,
        description = "The number of completed VirtualSpout instances."
    )
    spoutMonitor_completed("SpoutMonitor", "completed"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.SPOUT_MONITOR,
        description = "The max number of VirtualSpout instances that will be run concurrently."
    )
    spoutMonitor_poolSize("SpoutMonitor", "poolSize"),

    /*
     * Virtual Spout Metrics.
     */

    @Documentation(
        type = Documentation.Type.COUNTER,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.VIRTUAL_SPOUT,
        description = "Tuple ack count per VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    virtualSpout_ack("VirtualSpout", "{}.ack"),

    @Documentation(
        type = Documentation.Type.COUNTER,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.VIRTUAL_SPOUT,
        description = "Messages who have exceeded the maximum configured retry count per VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    virtualSpout_exceededRetryLimit("VirtualSpout", "{}.exceededRetryLimit"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.VIRTUAL_SPOUT,
        description = "How many Filters are being applied against the VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    virtualSpout_numberFiltersApplied("VirtualSpout", "{}.numberFiltersApplied"),

    @Documentation(
        type = Documentation.Type.COUNTER,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.VIRTUAL_SPOUT,
        description = "Tuple emit count per VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    virtualSpout_emit("VirtualSpout", "{}.emit"),

    @Documentation(
        type = Documentation.Type.COUNTER,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.VIRTUAL_SPOUT,
        description = "Tuple fail count per VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    virtualSpout_fail("VirtualSpout", "{}.fail"),

    @Documentation(
        type = Documentation.Type.COUNTER,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.VIRTUAL_SPOUT,
        description = "Filtered messages per VirtualSpout instance.",
        dynamicValues = { "virtualSpoutIdentifier" }
    )
    virtualSpout_filtered("VirtualSpout", "{}.filtered"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.VIRTUAL_SPOUT,
        description = "Total number of messages to be processed by the VirtualSpout for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    virtualSpout_partition_totalMessages("VirtualSpout", "{}.partition.{}.totalMessages"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.VIRTUAL_SPOUT,
        description = "Number of messages processed by the VirtualSpout instance for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    virtualSpout_partition_totalProcessed("VirtualSpout", "{}.partition.{}.totalProcessed"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.VIRTUAL_SPOUT,
        description = "Number of messages remaining to be processed by the VirtualSpout instance for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    virtualSpout_partition_totalUnprocessed("VirtualSpout", "{}.partition.{}.totalUnprocessed"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.VIRTUAL_SPOUT,
        description = "Percentage of messages processed out of the total for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    virtualSpout_partition_percentComplete("VirtualSpout", "{}.partition.{}.percentComplete"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.PERCENT,
        category = Documentation.Category.VIRTUAL_SPOUT,
        description = "The starting offset position for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    virtualSpout_partition_startingOffset("VirtualSpout", "{}.partition.{}.startingOffset"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.VIRTUAL_SPOUT,
        description = "The offset currently being processed for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    virtualSpout_partition_currentOffset("VirtualSpout", "{}.partition.{}.currentOffset"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.VIRTUAL_SPOUT,
        description = "The ending offset for the given partition.",
        dynamicValues = { "virtualSpoutIdentifier", "partition" }
    )
    virtualSpout_partition_endingOffset("VirtualSpout", "{}.partition.{}.endingOffset"),

    /*
     * Kafka Consumer Metrics.
     */

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.KAFKA_CONSUMER,
        description = "Offset consumer has processed.",
        dynamicValues = { "topic", "partition" }
    )
    kafkaConsumer_currentOffset("KafkaConsumer", "topic.{}.partition.{}.currentOffset"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.KAFKA_CONSUMER,
        description = "Offset for TAIL position in the partition.",
        dynamicValues = { "topic", "partition" }
    )
    kafkaConsumer_endOffset("KafkaConsumer", "topic.{}.partition.{}.endOffset"),

    @Documentation(
        type = Documentation.Type.GAUGE,
        unit = Documentation.Unit.NUMBER,
        category = Documentation.Category.KAFKA_CONSUMER,
        description = "Difference between endOffset and currentOffset metrics.",
        dynamicValues = { "topic", "partition" }
    )
    kafkaConsumer_lag("KafkaConsumer", "topic.{}.partition.{}.lag");

    /**
     * Context of the metric.
     */
    private final String context;

    /**
     * Name/Key of the metric.
     */
    private final String key;

    /**
     * Constructor.
     * @param context Context of the metric.
     * @param key Name of the metric.
     */
    Metrics(final String context, final String key) {
        this.context = context;
        this.key = key;
    }

    public String getContext() {
        return context;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "Metrics{"
            + "context='" + context + '\''
            + ", key='" + key + '\''
            + '}';
    }
}
