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

package com.salesforce.storm.spout.dynamic.consumer;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.metrics.MetricsRecorder;
import com.salesforce.storm.spout.dynamic.persistence.InMemoryPersistenceAdapter;
import com.salesforce.storm.spout.dynamic.persistence.PersistenceAdapter;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Mock consumer instance.
 */
public class MockConsumer implements Consumer {

    public static Map<VirtualSpoutIdentifier,BlockingQueue<Record>> records = Maps.newConcurrentMap();

    public static PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
    public static String topic = "MyTopic";
    public static List<Integer> partitions = Collections.singletonList(1);

    private VirtualSpoutIdentifier activeVirtualSpoutIdentifier;

    @Override
    public void open(
        Map<String, Object> spoutConfig,
        VirtualSpoutIdentifier virtualSpoutIdentifier,
        ConsumerPeerContext consumerPeerContext,
        PersistenceAdapter persistenceAdapter,
        MetricsRecorder metricsRecorder,
        ConsumerState startingState
    ) {
        this.activeVirtualSpoutIdentifier = virtualSpoutIdentifier;

        records.put(virtualSpoutIdentifier, new LinkedBlockingQueue<>(10_000));
    }

    @Override
    public void close() {
        records.remove(this.activeVirtualSpoutIdentifier);

        this.activeVirtualSpoutIdentifier = null;
    }

    @Override
    public Record nextRecord() {
        if (records.get(activeVirtualSpoutIdentifier).isEmpty()) {
            return null;
        }

        return records.get(activeVirtualSpoutIdentifier).poll();
    }

    @Override
    public void commitOffset(String namespace, int partition, long offset) {

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
    public PersistenceAdapter getPersistenceAdapter() {
        return persistenceAdapter;
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
    public static ConsumerState buildConsumerState(List<Integer> partitions) {
        ConsumerState.ConsumerStateBuilder builder = ConsumerState.builder();

        for (Integer partition : partitions) {
            builder.withPartition(topic, partition, 1L);
        }

        return builder.build();
    }
}
