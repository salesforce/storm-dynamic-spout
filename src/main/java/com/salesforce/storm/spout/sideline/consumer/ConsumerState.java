package com.salesforce.storm.spout.sideline.consumer;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.ConsumerPartition;
import com.salesforce.storm.spout.sideline.Tools;

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
     * @return A new ConsumerStateBuilder instance.
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
     * @return returns all of the ConsumerPartitions represented by the state.
     */
    public Set<ConsumerPartition> getConsumerPartitions() {
        return state.keySet();
    }

// Map Interface methods.

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
         * @return Built ConsumerState instance.
         */
        public ConsumerState build() {
            return new ConsumerState(state);
        }
    }
}
