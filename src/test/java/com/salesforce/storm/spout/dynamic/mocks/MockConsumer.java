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

package com.salesforce.storm.spout.dynamic.mocks;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.consumer.Consumer;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerPeerContext;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerState;
import com.salesforce.storm.spout.dynamic.consumer.Record;
import com.salesforce.storm.spout.dynamic.metrics.MetricsRecorder;
import com.salesforce.storm.spout.dynamic.persistence.PersistenceAdapter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Mock consumer implementation, used within tests.
 */
public class MockConsumer implements Consumer {
    private static final Map<VirtualSpoutIdentifier,BlockingQueue<Record>> recordHolder = Maps.newConcurrentMap();
    private static final Map<VirtualSpoutIdentifier, List<CommittedState>> committedStateHolder = Maps.newConcurrentMap();

    public static String topic = "MyTopic";
    public static List<Integer> partitions = Collections.singletonList(1);

    private PersistenceAdapter persistenceAdapter;
    private VirtualSpoutIdentifier activeVirtualSpoutIdentifier;

    @Override
    public void open(
        final SpoutConfig spoutConfig,
        final VirtualSpoutIdentifier virtualSpoutIdentifier,
        final ConsumerPeerContext consumerPeerContext,
        final PersistenceAdapter persistenceAdapter,
        final MetricsRecorder metricsRecorder,
        final ConsumerState startingState
    ) {
        this.persistenceAdapter = persistenceAdapter;
        this.activeVirtualSpoutIdentifier = virtualSpoutIdentifier;

        // Create empty queue.
        injectRecords(virtualSpoutIdentifier, new ArrayList<>());
    }

    @Override
    public void close() {
        synchronized (MockConsumer.class) {
            recordHolder.remove(this.activeVirtualSpoutIdentifier);
            committedStateHolder.remove(this.activeVirtualSpoutIdentifier);
        }

        this.activeVirtualSpoutIdentifier = null;
    }

    @Override
    public Record nextRecord() {
        synchronized (MockConsumer.class) {
            if (!recordHolder.containsKey(activeVirtualSpoutIdentifier) || recordHolder.get(activeVirtualSpoutIdentifier).isEmpty()) {
                return null;
            }

            return recordHolder.get(activeVirtualSpoutIdentifier).poll();
        }
    }

    @Override
    public void commitOffset(String namespace, int partition, long offset) {
        final CommittedState committedState = new CommittedState(namespace, partition, offset);
        synchronized (MockConsumer.class) {
            committedStateHolder.get(this.activeVirtualSpoutIdentifier).add(committedState);
        }
    }

    @Override
    public ConsumerState getCurrentState() {
        return buildConsumerState(partitions);
    }

    @Override
    public ConsumerState flushConsumerState() {
        return null;
    }

    @Override
    public void removeConsumerState() {

    }
    
    @Override
    public boolean unsubscribeConsumerPartition(ConsumerPartition consumerPartitionToUnsubscribe) {
        return false;
    }

    /**
     * Build consumer state for a set of partitions.
     * @param partitions list of partition ids.
     * @return consumer state instance for the provided partition ids.
     */
    static ConsumerState buildConsumerState(List<Integer> partitions) {
        ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();

        for (Integer partition : partitions) {
            builder.withPartition(topic, partition, 1L);
        }

        return builder.build();
    }

    /**
     * Quick and easy way to reset state.
     */
    public static void reset() {
        synchronized (MockConsumer.class) {
            recordHolder.clear();
            committedStateHolder.clear();
        }
    }

    /**
     * Allows for injecting recordHolder into the MockConsumer.
     * @param virtualSpoutIdentifier Identifier to inject recordHolder for.
     * @param injectedRecords Records to inject.
     */
    public static void injectRecords(
        final VirtualSpoutIdentifier virtualSpoutIdentifier,
        final Collection<Record> injectedRecords) {
        synchronized (MockConsumer.class) {
            if (!recordHolder.containsKey(virtualSpoutIdentifier)) {
                recordHolder.put(virtualSpoutIdentifier, new LinkedBlockingQueue<>(10_000));
            }
            recordHolder.get(virtualSpoutIdentifier).addAll(injectedRecords);

            if (!committedStateHolder.containsKey(virtualSpoutIdentifier)) {
                committedStateHolder.put(virtualSpoutIdentifier, new ArrayList<>());
            }
        }
    }

    /**
     * Return all Namespace/Offsets that have been committed for a given VirtualSpoutIdentifier.
     * @param virtualSpoutIdentifier The VirtualSpout to get committed offsets for.
     * @return List of Committed Offsets.
     */
    public static List<CommittedState> getCommitted(final VirtualSpoutIdentifier virtualSpoutIdentifier) {
        synchronized (MockConsumer.class) {
            if (!committedStateHolder.containsKey(virtualSpoutIdentifier)) {
                return new ArrayList<>();
            }
            return Collections.unmodifiableList(committedStateHolder.get(virtualSpoutIdentifier));
        }
    }

    /**
     * Value object containing information about Committed Offsets.
     */
    public static class CommittedState {
        private final String namespace;
        private final int partition;
        private final long offset;

        private CommittedState(final String namespace, final int partition, final long offset) {
            this.namespace = namespace;
            this.partition = partition;
            this.offset = offset;
        }

        public String getNamespace() {
            return namespace;
        }

        public int getPartition() {
            return partition;
        }

        public long getOffset() {
            return offset;
        }

        @Override
        public String toString() {
            return "CommittedState{"
                + "namespace='" + namespace + '\''
                + ", partition=" + partition
                + ", offset=" + offset
                + '}';
        }
    }
}
