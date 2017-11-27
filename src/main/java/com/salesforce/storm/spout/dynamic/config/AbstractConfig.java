package com.salesforce.storm.spout.dynamic.config;

import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract/Common configuration definitions.
 */
public class AbstractConfig {
    private static final Logger logger = LoggerFactory.getLogger(AbstractConfig.class);

    /**
     * Defines the Configuration.
     */
    private final ConfigDefinition configDefinition;

    /**
     * Stores configured values.
     */
    private final Map<String, Object> values = new HashMap<>();

    public AbstractConfig(final ConfigDefinition configDefinition, final Map<String, Object> values) {
        this.configDefinition = configDefinition;
        this.values.putAll(values);
    }

    public Object get(final String name) {
        // If we don't have a value set for this key
        if (!values.containsKey(name)) {
            // Return default value, or null
            configDefinition.getDefaultValue(name);
        }
        // Otherwise return configured value.
        return values.get(name);
    }

    public Short getShort(String key) {
        return (Short) get(key);
    }

    public Integer getInt(String key) {
        return (Integer) get(key);
    }

    public Long getLong(String key) {
        return (Long) get(key);
    }

    public Double getDouble(String key) {
        return (Double) get(key);
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getList(String key) {
        return (List<T>) get(key);
    }

    public Boolean getBoolean(String key) {
        return (Boolean) get(key);
    }

    public String getString(String key) {
        return (String) get(key);
    }

    public Class<?> getClass(String key) {
        return (Class<?>) get(key);
    }

    public boolean hasNonNullValue(String name) {
        // If we have a defined key, or our config definition has one.
        return get(name) != null;
    }

//    /**
//     * TODO Should this be immutable?
//     *
//     * @param name Name of setting.
//     * @param value Value to set.
//     * @return Config instance.
//     */
//    public AbstractConfig set(final String name, final Object value) {
//        values.put(name, value);
//        return this;
//    }
//
//    /**
//     * TODO Should this be immutable?
//     * Unsets a configured value.  If a default value is defined, will revert
//     * to using the default value.
//     *
//     * @param name Name of setting to unset.
//     * @return Config instance.
//     */
//    public AbstractConfig unset(final String name) {
//        values.remove(name);
//        return this;
//    }

    // TODO Temp method - remove this after its no longer needed.
    @Deprecated
    public Map<String, Object> toMap() {
        final Map<String,Object> map = new HashMap<>();

        for (final ConfigDefinition.ConfigKey configKey : configDefinition.getConfigKeys()) {
            if (values.containsKey(configKey.name)) {
                map.put(configKey.name, values.get(configKey.name));
            } else {
                map.put(configKey.name, configKey.defaultValue);
            }
        }
        return Collections.unmodifiableMap(map);
    }

}
