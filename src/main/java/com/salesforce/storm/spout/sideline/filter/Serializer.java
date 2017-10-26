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

package com.salesforce.storm.spout.sideline.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

/**
 * Serializer for converting {@link FilterChainStep} to and from a string (which is how they are persisted.)
 */
public class Serializer {

    private static final Logger logger = LoggerFactory.getLogger(Serializer.class);

    /**
     * Deserialize a serialized string value representing a FilterChainStep.
     * @param value String representing a serialized FilterChainStep.
     * @return Hydrated/Deserialized FilterChainStep.
     */
    public static FilterChainStep deserialize(final String value) throws InvalidFilterChainStepException {
        try {
            final byte[] data = Base64.getDecoder().decode(value);
            final ObjectInputStream objectInputStream = new ObjectInputStream(
                new ByteArrayInputStream(data)
            );
            FilterChainStep step = (FilterChainStep) objectInputStream.readObject();
            objectInputStream.close();
            return step;
        } catch (IllegalArgumentException | IOException | ClassNotFoundException ex) {
            throw new InvalidFilterChainStepException(
                "Unable to deserialize FilterChainStep",
                ex
            );
        }
    }

    /**
     * Serialize a FilterChainStep to a String representation.
     * @param step FilterChainStep to serialize.
     * @return Serialized string representation of FilterChainStep.
     */
    public static String serialize(FilterChainStep step) throws InvalidFilterChainStepException {
        try {
            final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            final ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(step);
            objectOutputStream.close();
            return Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());
        } catch (IOException ex) {
            throw new InvalidFilterChainStepException(
                "Unable to serialize FilterChainStep " + step,
                ex
            );
        }
    }
}
