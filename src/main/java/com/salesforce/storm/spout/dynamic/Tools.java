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

package com.salesforce.storm.spout.dynamic;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.map.UnmodifiableMap;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Random collection of utilities.
 */
public class Tools {

    /**
     * Creates a shallow copy of a map and wraps it in an UnmodifiableMap.
     *
     * @param sourceMap map we want to shallow clone and make immutable.
     * @param <K> key of the map.
     * @param <V> value of the map.
     * @return sshallow cloned map that is immutable.
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

    /**
     * Take a map of strings and objects (probably a config object) and strips a prefix out of its keys.
     *
     * If a key does not include the prefix, it gets dropped during the rekeying process.
     *
     * Example:
     * SourceMap:
     *   {prefix.key1: value1, prefix.key2: value2, key3: value3}
     * Result Returned:
     *   {key1: value1, key2: value2}
     *
     * @param prefix prefix to search for and strip out.
     * @param sourceConfig map (likely config) to rekey.
     * @param <V> object representing the configuration value.
     * @return rekeyed map (likely config).
     */
    public static <V> Map<String, V> stripKeyPrefix(
        final String prefix,
        final Map<String, V> sourceConfig
    ) {
        // Create new map instance with initial size equal to the source config map.
        final Map<String, V> clonedConfig = new HashMap<>(sourceConfig.size());

        // Loop over each entry
        for (final Map.Entry<String, V> entry : sourceConfig.entrySet()) {
            final String key = entry.getKey();

            if (key.startsWith(prefix)) {
                clonedConfig.put(
                    key.substring(prefix.length()),
                    entry.getValue()
                );
            }
        }
        return clonedConfig;
    }

    /**
     * Takes an input stream and splits on , returning an array of the values.  Each value is trim()'d
     * and empty values are removed.
     *
     * @param input Input string to split.
     * @return Array of trimmed values.
     */
    public static String[] splitAndTrim(final String input) {
        // Validate non-null input.
        Preconditions.checkNotNull(input);

        // Split on , call trim(), filter empty values.
        return Splitter.on(',')
            .splitToList(input)
            .stream()
            .map(String::trim)
            .filter((inputString) -> !inputString.isEmpty())
            .toArray(String[]::new);
    }
}
