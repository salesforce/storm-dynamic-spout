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

package com.salesforce.storm.spout.dynamic;

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
     * @param prefix prefix to search for and strip out.
     * @param config map (likely config) to rekey.
     * @return rekeyed map (likely config).
     */
    public static Map<String, Object> stripKeyPrefix(
        final String prefix,
        final Map<String, Object> config
    ) {
        Map<String, Object> clonedConfig = Maps.newHashMap();

        for (Map.Entry<String, Object> entry : config.entrySet()) {
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
}
