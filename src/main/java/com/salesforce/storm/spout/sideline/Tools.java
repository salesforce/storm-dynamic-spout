package com.salesforce.storm.spout.sideline;

import com.google.common.collect.Maps;
import org.apache.commons.collections.map.UnmodifiableMap;

import java.time.Duration;
import java.util.Collections;
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
        return new StringBuilder(String.valueOf(duration.toDays()))
                .append(" days ")
                .append(String.valueOf(duration.toHours() % 24))
                .append(" hrs ")
                .append(String.valueOf(duration.toMinutes() % 60))
                .append(" mins ")
                .append(String.valueOf(duration.getSeconds() % 60))
                .append( "secs")
                .toString();
    }
}
