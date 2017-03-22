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
     * Creates a shallow copy of the source map and wraps it in an UnmodifiedMap wrapper.
     *
     * @param sourceMap - the map we want to shallow clone and make immutable.
     * @return - A shallow cloned map thats immutable.
     */
    public static <K,V> Map<K,V> immutableCopy(Map<K,V> sourceMap) {
        // If we're already dealing with a cloned & immutable map
        if (sourceMap instanceof UnmodifiableMap) {
            // just return it.
            return sourceMap;
        }

        // Create a new map
        Map<K,V> copy = Maps.newHashMap();

        // Add all entries from the source map
        copy.putAll(sourceMap);

        // Wrap it in an unmodified map.
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
