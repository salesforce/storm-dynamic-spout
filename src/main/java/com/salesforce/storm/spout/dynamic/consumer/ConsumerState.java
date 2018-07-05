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

package com.salesforce.storm.spout.dynamic.consumer;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import com.salesforce.storm.spout.dynamic.Tools;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * This represents the State of a Consumer.  Immutable, use Builder to construct an instance.
 */
public final class ConsumerState implements Map<ConsumerPartition, Long> {
    private final Map<ConsumerPartition, Long> state;

    /**
     * Private constructor.  Create an instance via builder().
     * @param state State that backs the consumer state.
     */
    private ConsumerState(Map<ConsumerPartition, Long> state) {
        this.state = Tools.immutableCopy(state);
    }

    /**
     * Get an instance of a consumer state builder.
     * @return instance of a consumer state builder.
     */
    public static ConsumerStateBuilder builder() {
        return new ConsumerStateBuilder();
    }

    /**
     * Return the current offset for the given TopicPartition.
     * @param consumerPartition The TopicPartition to get the offset for.
     * @return The current offset, or null if none is available.
     */
    public Long getOffsetForNamespaceAndPartition(ConsumerPartition consumerPartition) {
        return state.get(consumerPartition);
    }

    /**
     * Return the current offset for the given TopicPartition.
     * @param namespace Namespace to retrieve offset for
     * @param partition Partition to retrieve offset for
     * @return The current offset, or null if none is available.
     */
    public Long getOffsetForNamespaceAndPartition(String namespace, int partition) {
        return getOffsetForNamespaceAndPartition(new ConsumerPartition(namespace, partition));
    }

    /**
     * Get a set of the ConsumerPartitions represented by the state.
     * @return set of the ConsumerPartitions represented by the state.
     */
    public Set<ConsumerPartition> getConsumerPartitions() {
        return state.keySet();
    }

    @Override
    public boolean isEmpty() {
        return state == null || state.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return state.containsKey(key);
    }

    @Override
    public Long get(Object key) {
        return state.get(key);
    }

    @Override
    public Set<ConsumerPartition> keySet() {
        return getConsumerPartitions();
    }

    @Override
    public Collection<Long> values() {
        return state.values();
    }

    @Override
    public Set<Entry<ConsumerPartition, Long>> entrySet() {
        return state.entrySet();
    }

    @Override
    public int size() {
        return state.size();
    }

    @Override
    public boolean containsValue(Object value) {
        return state.containsValue(value);
    }

    @Override
    public Long put(ConsumerPartition key, Long value) {
        throw new UnsupportedOperationException("Immutable map");
    }

    @Override
    public Long remove(Object key) {
        throw new UnsupportedOperationException("Immutable map");
    }

    @Override
    public String toString() {
        return "ConsumerState{"
            + state
            + '}';
    }

    /**
     * Unsupported operation for this implementation.
     */
    @Override
    public void putAll(Map<? extends ConsumerPartition, ? extends Long> map) {
        throw new UnsupportedOperationException("Immutable map");
    }

    /**
     * Unsupported operation for this implementation.
     */
    @Override
    public void clear() {
        throw new UnsupportedOperationException("Immutable map");
    }

    /**
     * Builder for ConsumerState.
     */
    public static final class ConsumerStateBuilder {
        private final Map<ConsumerPartition, Long> state = Maps.newHashMap();

        /**
         * Default constructor.
         */
        public ConsumerStateBuilder() {
        }

        /**
         * Add a new entry into the builder.
         * @param topicPartition TopicPartition for the entry.
         * @param offset Offset for the given TopicPartition
         * @return Builder instance for chaining.
         */
        public ConsumerStateBuilder withPartition(ConsumerPartition topicPartition, long offset) {
            state.put(topicPartition, offset);
            return this;
        }

        /**
         * Add a new entry into the builder.
         * @param topic Topic for the entry
         * @param partition Partition for the entry
         * @param offset Offset for the given TopicPartition
         * @return Builder instance for chaining.
         */
        public ConsumerStateBuilder withPartition(String topic, int partition, long offset) {
            return withPartition(new ConsumerPartition(topic, partition), offset);
        }

        /**
         * Get a consumer state instance from the state that has been built.
         * @return consumer state instance.
         */
        public ConsumerState build() {
            return new ConsumerState(state);
        }
    }
}
