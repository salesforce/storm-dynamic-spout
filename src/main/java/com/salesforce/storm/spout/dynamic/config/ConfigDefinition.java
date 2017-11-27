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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Defines a Configuration value.
 * Highly inspired by Kafka's ConfigDef class.
 */
public class ConfigDefinition {
    private boolean isLocked = false;
    private final Map<String, ConfigKey> configKeys;

    /**
     * Constructor.
     */
    public ConfigDefinition() {
        configKeys = new LinkedHashMap<>();
    }

    /**
     * Constructor.
     * Create instance using a parent definition as a base.
     * @param parentDefinition base definition to extend.
     */
    public ConfigDefinition(final ConfigDefinition parentDefinition) {
        configKeys = new LinkedHashMap<>(parentDefinition.configKeys);
    }

    /**
     * @return All of the keys used in the configuration.
     */
    public Set<String> getKeys() {
        return Collections.unmodifiableSet(configKeys.keySet());
    }

    /**
     * Define a new ConfigurationKey.
     * @param key The key to define.
     * @return ConfigDefinition instance.
     */
    public ConfigDefinition define(ConfigKey key) {
        if (isLocked) {
            throw new IllegalStateException("Cannot modify after being locked.");
        }
        configKeys.put(key.name, key);
        return this;
    }

    /**
     * Utility to quickly define a new ConfigDef instance.
     * @param name Name of configuration key.
     * @param type Class type of value stored.
     * @param defaultValue Default value, or null.
     * @param importance How important is this value.
     * @param category What category does the field fall under.
     * @param description Human readable description.
     * @return ConfigDefinition instance.
     */
    public ConfigDefinition define(
        String name,
        Class type,
        Object defaultValue,
        Importance importance,
        String category,
        String description) {
        return define(new ConfigKey(name, type, defaultValue, importance, description, category));
    }

    /**
     * Get the configuration keys.
     * @return a map containing all configuration keys
     */
    public Collection<ConfigKey> getConfigKeys() {
        return Collections.unmodifiableCollection(configKeys.values());
    }

    /**
     * Utility method.
     * @param name Name of configuration property.
     * @return Default value stored, or null.
     */
    public Object getDefaultValue(final String name) {
        if (configKeys.containsKey(name)) {
            return configKeys.get(name).defaultValue;
        }
        return null;
    }

    public ConfigDefinition setDefaultValue(final String name, final Object defaultValue) {
        if (isLocked) {
            throw new IllegalStateException("Cannot modify after being locked.");
        }

        if (!configKeys.containsKey(name)) {
            // TODO Handle in some way?
            return this;
        }
        final ConfigKey configKey = configKeys.get(name);

        // Re-define key with new default value.
        define(
            configKey.name,
            configKey.type,
            defaultValue,
            configKey.importance,
            configKey.category,
            configKey.description
        );

        return this;
    }

    /**
     * The importance level for a configuration.
     */
    public enum Importance {
        HIGH, MEDIUM, LOW
    }

    /**
     * Todo later.
     */
    public String toHtmlTable() {
        // todo
        return "";
    }

    public ConfigDefinition lock() {
        isLocked = true;
        return this;
    }

    /**
     * Defines a single Configuration property.
     */
    public static class ConfigKey {
        public final String name;
        public final Class type;
        public final String description;
        public final Object defaultValue;
        public final Importance importance;
        public final String category;

        /**
         * Constructor.
         * @param name Name of configuration key.
         * @param type Class type of value stored.
         * @param defaultValue Default value, or null.
         * @param importance How important is this value.
         * @param category What category does the field fall under.
         * @param description Human readable description.
         */
        public ConfigKey(
            final String name,
            final Class type,
            final Object defaultValue,
            final Importance importance,
            final String category,
            final String description) {
            this.name = name;
            this.type = type;
            this.defaultValue = defaultValue;
            this.importance = importance;
            this.description = description;
            this.category = category;
        }
    }
}
