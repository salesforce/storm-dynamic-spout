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

package com.salesforce.storm.spout.dynamic;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.salesforce.storm.spout.dynamic.filter.FilterChainStep;
import com.salesforce.storm.spout.sideline.persistence.FilterChainStepSerializer;

import java.util.Map;

/**
 * Thin wrapper around JSON parsing.
 *
 * Intended to hide the particular JSON implementation of the day.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JSON {

    /**
     * JSON parser.
     *
     * Includes a special handler for serialized FilterChainSteps, which we hope to now always have to handle this way.
     */
    private final Gson gson;

    /**
     * Create JSON serializer/deserializer instance with default configuration.
     * @param config configuration.
     */
    public JSON(final Map<String, Object> config) {
        this.gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .registerTypeAdapter(FilterChainStep.class, new FilterChainStepSerializer(config))
            .create();
    }

    /**
     * Convert an object to a string of JSON.
     * @param value object to be converted.
     * @return string of JSON.
     */
    public String to(final Object value) {
        return gson.toJson(value);
    }

    /**
     * Convert a string of JSON to an object.
     * @param value string of JSON to be converted.
     * @param clazz class of object to convert the JSON to.
     * @param <T> type to be used for returns.
     * @return object converted from JSON.
     */
    public <T> T from(final String value, final Class<T> clazz) {
        return gson.fromJson(value, clazz);
    }
}
