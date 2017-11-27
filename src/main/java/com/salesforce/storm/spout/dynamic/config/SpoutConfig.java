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

package com.salesforce.storm.spout.dynamic.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Common Spout configuration definitions.
 */
public class SpoutConfig {

    /**
     * Defines the Configuration.
     */
    private final ConfigDefinition configDefinition;

    /**
     * Stores configured values.
     */
    private final Map<String, Object> values;

    public SpoutConfig(final ConfigDefinition configDefinition, final Map<String, Object> values) {
        this.configDefinition = configDefinition;
        this.values = Collections.unmodifiableMap(values);
    }

    /**
     * Return the given configuration name's value.
     * If the name does not exist, this will return null.
     * TODO Should we toss an exception instead?
     *
     * @param name Name of configuration.
     * @return Value of configuration.
     */
    public final Object get(final String name) {
        // If we don't have a value set for this key
        if (!values.containsKey(name)) {
            // Return default value, or null
            return configDefinition.getDefaultValue(name);
        }
        // Otherwise return configured value.
        return values.get(name);
    }

    /**
     * @param name Name of configuration
     * @return Returns value of the configuration casted as a Short.
     */
    public final Short getShort(final String name) {
        return ((Number) get(name)).shortValue();
    }

    /**
     * @param name Name of configuration
     * @return Returns value of the configuration casted as an Integer.
     */
    public final Integer getInt(String name) {
        return ((Number) get(name)).intValue();
    }

    /**
     * @param name Name of configuration
     * @return Returns value of the configuration casted as a Long.
     */
    public final Long getLong(String name) {
        return ((Number) get(name)).longValue();
    }

    /**
     * @param name Name of configuration
     * @return Returns value of the configuration casted as a Double.
     */
    public final Double getDouble(String name) {
        return (Double) get(name);
    }

    /**
     * @param name Name of configuration
     * @return Returns value of the configuration casted as a List.
     */
    @SuppressWarnings("unchecked")
    public final <T> List<T> getList(String name) {
        return (List<T>) get(name);
    }

    /**
     * @param name Name of configuration
     * @return Returns value of the configuration casted as a Boolean.
     */
    public final Boolean getBoolean(String name) {
        return (Boolean) get(name);
    }

    /**
     * @param name Name of configuration
     * @return Returns value of the configuration casted as a String.
     */
    public final String getString(String name) {
        return (String) get(name);
    }

    /**
     * @param name Name of configuration
     * @return Returns value of the configuration casted as a Class.
     */
    public final Class<?> getClass(String name) {
        return (Class<?>) get(name);
    }

    /**
     * @param name Name of configuration
     * @return Returns true if the configuration has a non-null value set.
     */
    public final boolean hasNonNullValue(String name) {
        // If we have a defined key, or our config definition has one.
        return get(name) != null;
    }

    /**
     * Internal utility method that flattens the internal configuration and default values
     * into a single immutable map.
     * @return Merged Configuration values as an immutable map.
     */
    private Map<String, Object> toMap() {
        final Map<String,Object> map = new HashMap<>();
        map.putAll(values);

        // Ensure all the defaults are set.
        for (final ConfigDefinition.ConfigKey configKey : configDefinition.getConfigKeys()) {
            if (!map.containsKey(configKey.name)) {
                map.put(configKey.name, configKey.defaultValue);
            }
        }
        return Collections.unmodifiableMap(map);
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
     * @return rekeyed map (likely config).
     */
    public final Map<String, Object> stripKeyPrefix(final String prefix) {
        // Create new map instance with initial size equal to the source config map.
        final Map<String, Object> strippedConfig = new HashMap<>();

        // Loop over each entry
        for (final Map.Entry<String, Object> entry : toMap().entrySet()) {
            final String name = entry.getKey();

            if (name.startsWith(prefix)) {
                strippedConfig.put(
                    name.substring(prefix.length()),
                    entry.getValue()
                );
            }
        }
        return Collections.unmodifiableMap(strippedConfig);
    }
}
