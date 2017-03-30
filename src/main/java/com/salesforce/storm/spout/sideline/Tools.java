package com.salesforce.storm.spout.sideline;

import com.google.common.collect.Maps;
import org.apache.commons.collections.map.UnmodifiableMap;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Random collection of utilities.
 */
public class Tools {

    /**
     * Creates a shallow copy of a map and wraps it in an UnmodifiableMap.
     *
     * @param sourceMap - the map we want to shallow clone and make immutable.
     * @return - A shallow cloned map that is immutable.
     */
    public static <K,V> Map<K,V> immutableCopy(Map<K,V> sourceMap) {
        // If we're already dealing with an UnmodifiableMap
        if (sourceMap instanceof UnmodifiableMap) {
            // just return it.
            return sourceMap;
        }

        // Create a new map and add all entries from the source map
        Map<K,V> copy = Maps.newHashMap();
        copy.putAll(sourceMap);

        // Wrap it in an unmodifiable map.
        return Collections.unmodifiableMap(copy);
    }

    /**
     * Returns a pretty formatted string of a duration.
     * @param duration - the Duration to display
     * @return - String representation of the duration
     */
    public static String prettyDuration(Duration duration) {
        return String.valueOf(duration.toDays())
            + " days "
            + String.valueOf(duration.toHours() % 24)
            + " hrs "
            + String.valueOf(duration.toMinutes() % 60)
            + " mins "
            + String.valueOf(duration.getSeconds() % 60)
            + "secs";
    }
}
