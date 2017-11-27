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
    private final Map<String, ConfigKey> configKeys;

    public ConfigDefinition() {
        configKeys = new LinkedHashMap<>();
    }

    /**
     * @return All of the keys used in the configuration.
     */
    public Set<String> getKeys() {
        return Collections.unmodifiableSet(configKeys.keySet());
    }

    public ConfigDefinition define(ConfigKey key) {
        if (configKeys.containsKey(key.name)) {
            // TODO handle
            //throw new ConfigException("Configuration " + key.name + " is defined twice.");
        }
        configKeys.put(key.name, key);
        return this;
    }

    /**
     * Utility to quickly define a new ConfigDef instance.
     * @param name
     * @param type
     * @param defaultValue
     * @param importance
     * @param description
     * @param category
     * @return
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

    public Object getDefaultValue(final String name) {
        if (configKeys.containsKey(name)) {
            return configKeys.get(name).defaultValue;
        }
        return null;
    }

    public boolean hasDefinedKey(final String name) {
        return configKeys.containsKey(name);
    }

    /**
     * The importance level for a configuration.
     */
    public enum Importance {
        HIGH, MEDIUM, LOW
    }

    public static class ConfigKey {
        public final String name;
        public final Class type;
        public final String description;
        public final Object defaultValue;
        public final Importance importance;
        public final String category;

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

        public boolean hasDefault() {
            return this.defaultValue != null;
        }
    }

    public String toHtmlTable() {
        // todo
        return "";
    }

    /**
     * Get a list of configs sorted taking the 'group' and 'orderInGroup' into account.
     *
     * If grouping is not specified, the result will reflect "natural" order: listing required fields first, then ordering by importance, and finally by name.
     */
    private List<ConfigKey> sortedConfigs() {
        // todo
        return null;
    }
}
