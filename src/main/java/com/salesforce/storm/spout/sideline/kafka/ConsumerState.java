package com.salesforce.storm.spout.sideline.kafka;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.Tools;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

/**
 * This represents the State of a Consumer.
 */
public class ConsumerState {
    private final Map<TopicPartition, Long> state;

    private ConsumerState(Map<TopicPartition, Long> state) {
        this.state = Tools.immutableCopy(state);
    }

    /**
     * Return the current offset for the given TopicPartition.
     * @param topicPartition - The TopicPartition to get the offset for.
     * @return - The current offset, or null if none is available.
     */
    public Long getOffsetForTopicAndPartition(TopicPartition topicPartition) {
        return getState().get(topicPartition);
    }
    
    /**
     * @return - returns internal hashmap representation.
     */
    public Map<TopicPartition, Long> getState() {
        return state;
    }

    /**
     * @return - returns all of the TopicPartitions represented by the state.
     */
    public Set<TopicPartition> getTopicPartitions() {
        return getState().keySet();
    }

    /**
     * @return - return true if this contains no information.
     */
    public boolean isEmpty() {
        return state == null || state.isEmpty();
    }

    /**
     * @return - the number of entries.
     */
    public int size() {
        return state.size();
    }

    public static ConsumerStateBuilder builder() {
        return new ConsumerStateBuilder();
    }

    @Override
    public String toString() {
        return "ConsumerState{"
                + "state=" + state
                + '}';
    }

    /**
     * WIP builder pattern.
     */
    public static final class ConsumerStateBuilder {
        private Map<TopicPartition, Long> state = Maps.newHashMap();

        public ConsumerStateBuilder() {
        }

        public ConsumerStateBuilder withPartition(TopicPartition topicPartition, long offset) {
            state.put(topicPartition, offset);
            return this;
        }

        public ConsumerState build() {
            return new ConsumerState(state);
        }
    }
}
