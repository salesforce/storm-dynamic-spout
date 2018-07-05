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

package com.salesforce.storm.spout.dynamic.kafka.deserializer.compat;

import com.salesforce.storm.spout.dynamic.kafka.deserializer.Deserializer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Provides a compatibility-like layer to Storm-Kafka Scheme interface.
 */
public abstract class AbstractScheme implements Deserializer {

    /**
     * Define the fields that you will deserialize into.
     * @return the fields that you will deserialize into.
     */
    public abstract Fields getOutputFields();

    /**
     * Implement deserialization logic and return List of objects/tuple values.
     * @param ser byte buffer to deserialize.
     * @return list of objects/tuple values.
     */
    public abstract List<Object> deserialize(ByteBuffer ser);

    /**
     * Provides compatibility layer to 'Storm-Kafka' Scheme-like interface.
     */
    public Values deserialize(String topic, int partition, long offset, byte[] key, byte[] value) {
        List<Object> list = deserialize(ByteBuffer.wrap(value));
        if (list == null) {
            return null;
        }
        Values values = new Values();
        values.addAll(list);
        return values;
    }
}
