/*
 * Copyright (c) 2017, 2018, Salesforce.com, Inc.
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

package com.salesforce.storm.spout.sideline.persistence;

import com.google.common.base.Preconditions;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.salesforce.storm.spout.dynamic.filter.FilterChainStep;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Gson handler for creating FilterChainStep instances.
 * @param <T> object type for serializing and deserializing, should be a FilterChainStep implementation.
 */
public class FilterChainStepSerializer<T extends FilterChainStep> implements JsonDeserializer<T>, JsonSerializer<T> {

    /**
     * Configuration from the spout.
     */
    private final Map<String, Object> config;

    /**
     * Gson handler for creating FilterChainStep instances.
     * @param config configuration from the spout.
     */
    public FilterChainStepSerializer(final Map<String, Object> config) {
        this.config = config;
    }

    @Override
    public JsonElement serialize(T obj, Type typeOfObj, JsonSerializationContext context) {
        // Implement what is essentially the default serialization strategy here.
        // This might seem odd, but before I did this, when FilterChainStep's were nested they did not serialize correctly
        return context.serialize(obj);
    }

    @Override
    public T deserialize(
        final JsonElement jsonElement,
        final Type type,
        final JsonDeserializationContext deserializationContext
    ) throws JsonParseException {
        final JsonObject jsonObject = jsonElement.getAsJsonObject();
        final Class<T> clazz = getClassInstance();
        return deserializationContext.deserialize(jsonObject, clazz);
    }

    @SuppressWarnings("unchecked")
    private Class<T> getClassInstance() {
        @SuppressWarnings("unchecked")
        final String className = (String) config.get(SidelineConfig.FILTER_CHAIN_STEP_CLASS);

        Preconditions.checkArgument(
            className != null && !className.isEmpty(),
            "A valid class name must be specified for " + SidelineConfig.FILTER_CHAIN_STEP_CLASS
        );

        try {
            return (Class<T>) Class.forName(className);
        } catch (ClassNotFoundException cnfe) {
            throw new JsonParseException(cnfe.getMessage());
        }
    }
}
