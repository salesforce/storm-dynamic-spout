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

package com.salesforce.storm.spout.sideline.persistence;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.salesforce.storm.spout.dynamic.filter.FilterChainStep;
import com.salesforce.storm.spout.dynamic.filter.InvalidFilterChainStepException;
import com.salesforce.storm.spout.dynamic.filter.Serializer;

import java.io.IOException;

/**
 * GSON Type Adapter Factory for storing our filter chain steps not as complex objects, but as java serialized and base64'ed strings.
 */
public class FilterChainStepTypeAdapterFactory implements TypeAdapterFactory {

    @SuppressWarnings("unchecked")
    @Override
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
        if (!FilterChainStep.class.isAssignableFrom(type.getRawType())) {
            return null;
        }

        return (TypeAdapter<T>) new FilterChainStepTypeAdapter();
    }

    private static class FilterChainStepTypeAdapter extends TypeAdapter<FilterChainStep> {

        @Override
        public void write(JsonWriter out, FilterChainStep filterChainStep) throws IOException, InvalidFilterChainStepException {
            if (filterChainStep == null) {
                out.nullValue();
            } else {
                final String data = Serializer.serialize(filterChainStep);

                // I know what you're thinking... WTF right?
                // By default GSON is going to convert HTML entities like =, but we're base64'ing this thing so we're going to have
                // equal signs, which makes this type adapter not work. So what do we do?  We set a raw value and handle enclosing
                // it ourselves.  Because of the way we encode this thing we will always have a string, so it'll be safe.
                out.jsonValue("\"" + data + "\"");
            }
        }

        @Override
        public FilterChainStep read(JsonReader in) throws IOException {
            final String data = in.nextString();

            return Serializer.deserialize(data);
        }
    }
}
