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

package com.salesforce.storm.spout.dynamic.coordinator;

import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerState;
import com.salesforce.storm.spout.dynamic.DelegateSpout;
import com.salesforce.storm.spout.dynamic.metrics.SpoutMetrics;
import com.salesforce.storm.spout.dynamic.metrics.MetricsRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reports the progress of the spout's consumer, partition by partition.
 */
class SpoutPartitionProgressMonitor {

    private static final Logger logger = LoggerFactory.getLogger(SpoutPartitionProgressMonitor.class);

    /**
     * Metrics recorder for various partition specific stats.
     */
    private final MetricsRecorder metricsRecorder;

    /**
     * Constructor.
     */
    SpoutPartitionProgressMonitor(final MetricsRecorder metricsRecorder) {
        this.metricsRecorder = metricsRecorder;
    }

    /**
     * Reports the progress of the spout's consumer, partition by partition.
     * @param spout spout to calculate the progress for.
     */
    void reportStatus(final DelegateSpout spout) {
        final VirtualSpoutIdentifier virtualSpoutId = spout.getVirtualSpoutId();

        logger.debug("Reporting status for {}", virtualSpoutId);

        final ConsumerState currentState = spout.getCurrentState();
        final ConsumerState startingState = spout.getStartingState();
        final ConsumerState endingState = spout.getEndingState();

        if (currentState == null) {
            logger.warn("No current state for {}, if the spout didn't just open there's probably a bug in your consumer!", virtualSpoutId);
            return;
        }

        // We can only track progress for partitions the consumer is currently subscribed to, so let's loop
        // over those.  It's possible there were more partitions in startingState, but we can't deal with
        // those if we are not currently getting state reported for them.
        for (final ConsumerPartition consumerPartition : currentState.getConsumerPartitions()) {
            // Find the current state for the partition we're looking at
            Long currentOffset = currentState.getOffsetForNamespaceAndPartition(consumerPartition);
            if (currentOffset == null) {
                // This most likely happens when the partition has been completed, else it could be an error
                logger.error("Could not find CURRENT offset for {} on virtual spout {}", consumerPartition, virtualSpoutId);
                continue;
            }

            Long startingOffset = null;
            if (startingState != null) {
                startingOffset = startingState.getOffsetForNamespaceAndPartition(consumerPartition);
            } else {
                logger.warn("Could not find the STARTING offset, we likely didn't start with one {} {}", consumerPartition, virtualSpoutId);
            }

            Long endingOffset = null;
            if (endingState != null) {
                endingOffset = endingState.getOffsetForNamespaceAndPartition(consumerPartition);
            } else {
                logger.debug("Could not find the ENDING offset, we likely didn't start with one {} {}", consumerPartition, virtualSpoutId);
            }

            // Use our spout id + the partition for our key
            final String metricKey = spout.getVirtualSpoutId() + ".partition" + consumerPartition.partition();

            Long totalProcessed = null;
            if (startingOffset != null) {
                totalProcessed = currentOffset - startingOffset;
            }

            Long totalUnprocessed = null;
            if (endingOffset != null) {
                totalUnprocessed = endingOffset - currentOffset;
            }

            Long totalMessages = null;
            if (startingOffset != null && endingOffset != null) {
                // Note that this value should not change during reports
                totalMessages = endingOffset - startingOffset;
            }

            Long percentComplete = null;
            // Need both stats, and to make sure we don't divide by 0!
            if (totalProcessed != null && totalMessages != null && totalMessages > 0) {
                percentComplete = (totalProcessed / totalMessages);
            }

            // Capture our metrics...
            metricsRecorder.assignValue(
                SpoutMetrics.VIRTUAL_SPOUT_PARTITION_CURRENT_OFFSET,
                currentOffset,
                spout.getVirtualSpoutId().toString(),
                consumerPartition.partition()
            );

            if (totalProcessed != null) {
                metricsRecorder.assignValue(
                    SpoutMetrics.VIRTUAL_SPOUT_PARTITION_TOTAL_PROCESSED,
                    totalProcessed,
                    spout.getVirtualSpoutId().toString(),
                    consumerPartition.partition()
                );
            }

            if (totalUnprocessed != null) {
                metricsRecorder.assignValue(
                    SpoutMetrics.VIRTUAL_SPOUT_PARTITION_TOTAL_UNPROCESSED,
                    totalUnprocessed,
                    spout.getVirtualSpoutId().toString(),
                    consumerPartition.partition()
                );
            }

            if (totalMessages != null) {
                metricsRecorder.assignValue(
                    SpoutMetrics.VIRTUAL_SPOUT_PARTITION_TOTAL_MESSAGES,
                    totalMessages,
                    spout.getVirtualSpoutId().toString(),
                    consumerPartition.partition()
                );
            }

            if (percentComplete != null) {
                metricsRecorder.assignValue(
                    SpoutMetrics.VIRTUAL_SPOUT_PARTITION_PERCENT_COMPLETE,
                    percentComplete,
                    spout.getVirtualSpoutId().toString(),
                    consumerPartition.partition()
                );
            }

            if (startingOffset != null) {
                metricsRecorder.assignValue(
                    SpoutMetrics.VIRTUAL_SPOUT_PARTITION_STARTING_OFFSET,
                    startingOffset,
                    spout.getVirtualSpoutId().toString(),
                    consumerPartition.partition()
                );
            }

            if (endingOffset != null) {
                metricsRecorder.assignValue(
                    SpoutMetrics.VIRTUAL_SPOUT_PARTITION_ENDING_OFFSET,
                    endingOffset,
                    spout.getVirtualSpoutId().toString(),
                    consumerPartition.partition()
                );
            }

            // Log our metrics...

            if (endingOffset != null) {
                // This is when our consumer was given a specific stopping point
                logger.info(
                    "Progress for {} on partition {}: {} processed, {} remaining ({}% complete)",
                    spout.getVirtualSpoutId(),
                    consumerPartition,
                    totalProcessed,
                    totalUnprocessed,
                    percentComplete
                );
            } else {
                // TODO: If we have no endingOffset and our totalProcessed = 0, should we even bother writing this log
                logger.info(
                    "Progress for {} on partition {}: {} processed",
                    spout.getVirtualSpoutId(),
                    consumerPartition,
                    totalProcessed
                );
            }
        }
    }
}
