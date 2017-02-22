package com.salesforce.storm.spout.sideline.kafka.consumerState;

import com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

/**
 * This represents the State of a Consumer.
 */
public class ConsumerState {
    final Map<TopicPartition, Long> state = Maps.newHashMap();

    public Long getOffsetForTopicAndPartition(TopicPartition topicPartition) {
        return getState().get(topicPartition);
    }

    public void setOffset(TopicPartition topicPartition, Long offset) {
        if (offset == null) {
            getState().remove(topicPartition);
        } else {
            getState().put(topicPartition, offset);
        }
    }

    public Map<TopicPartition, Long> getState() {
        return state;
    }

    public Set<TopicPartition> getTopicPartitions() {
        return getState().keySet();
    }

    @Override
    public String toString() {
        return "ConsumerState{" +
                "state=" + state +
                '}';
    }
}
