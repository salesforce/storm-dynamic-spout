package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.Tools;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * This represents the State of a Consumer.  Immutable, use Builder to construct an instance.
 */
public final class ConsumerState implements Map<TopicPartition, Long> {
    private final Map<TopicPartition, Long> state;

    /**
     * Private constructor.  Create an instance via builder().
     * @param state - State that backs the consumer state.
     */
    private ConsumerState(Map<TopicPartition, Long> state) {
        this.state = Tools.immutableCopy(state);
    }

    /**
     * @return - A new ConsumerStateBuilder instance.
     */
    public static ConsumerStateBuilder builder() {
        return new ConsumerStateBuilder();
    }

    /**
     * Return the current offset for the given TopicPartition.
     * @param topicPartition - The TopicPartition to get the offset for.
     * @return - The current offset, or null if none is available.
     */
    public Long getOffsetForTopicAndPartition(TopicPartition topicPartition) {
        return state.get(topicPartition);
    }

    /**
     * @return - returns all of the TopicPartitions represented by the state.
     */
    public Set<TopicPartition> getTopicPartitions() {
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
    public Set<TopicPartition> keySet() {
        return getTopicPartitions();
    }

    @Override
    public Collection<Long> values() {
        return state.values();
    }

    @Override
    public Set<Entry<TopicPartition, Long>> entrySet() {
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
    public Long put(TopicPartition key, Long value) {
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
    public void putAll(Map<? extends TopicPartition, ? extends Long> map) {
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
        private final Map<TopicPartition, Long> state = Maps.newHashMap();

        public ConsumerStateBuilder() {
        }

        /**
         * Add a new entry into the builder.
         * @param topicPartition TopicPartition for the entry.
         * @param offset Offset for the given TopicPartition
         * @return Builder instance for chaining.
         */
        public ConsumerStateBuilder withPartition(TopicPartition topicPartition, long offset) {
            state.put(topicPartition, offset);
            return this;
        }

        /**
         * @return Built ConsumerState instance.
         */
        public ConsumerState build() {
            return new ConsumerState(state);
        }
    }
}
