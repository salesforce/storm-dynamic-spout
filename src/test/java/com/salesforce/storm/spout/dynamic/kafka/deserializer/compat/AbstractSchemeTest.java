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

import com.google.common.collect.Lists;
import com.salesforce.storm.spout.dynamic.kafka.deserializer.Deserializer;
import com.google.common.base.Charsets;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Test for compatibility layer to Storm-Kafka scheme.
 */
public class AbstractSchemeTest {

    /**
     * Silly test to go from String to byte[] to ByteBuffer to String.
     * Heh.
     */
    @Test
    public void testDeserialize() {
        @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
        final String value = "This Is My Text\uD83D\uDC7B";
        final byte[] valueBytes = value.getBytes(Charsets.UTF_8);

        // Pull implementation into a Deserializer obj.
        final Deserializer myScheme = new MyScheme();

        // Attempt to deserialize
        final Values myValues = myScheme.deserialize("TopicName", 2, 2222L, "Key".getBytes(Charsets.UTF_8), valueBytes);

        // Validate
        assertNotNull(myValues);
        assertEquals("Should have 1 entry", 1, myValues.size());
        assertEquals("Should be our value", value, myValues.get(0));
    }

    /**
     * Silly test to go from [] to [] to [] to "".
     * Heh.
     */
    @Test
    public void testDeserializeWithEmptyByteArray() {
        final byte[] inputNullBytes = new byte[0];

        // Pull implementation into a Deserializer obj.
        final Deserializer myScheme = new MyScheme();

        // Attempt to deserialize
        final Values myValues = myScheme.deserialize("TopicName", 2, 2222L, "Key".getBytes(Charsets.UTF_8), inputNullBytes);

        // Validate
        assertNotNull(myValues);
        assertEquals("Should have 1 entry", 1, myValues.size());
        assertEquals("Should be our value", "", myValues.get(0));
    }

    /**
     * If we return null, return null.
     */
    @Test
    public void testDeserializeReturnsNull() {
        // Create implementation, and force abstract implementation to return null.
        final MyScheme myScheme = new MyScheme();
        myScheme.setReturnNull(true);

        // Attempt to deserialize
        final Values myValues = myScheme.deserialize(
            "TopicName",
            2,
            2222L,
            "Key".getBytes(Charsets.UTF_8),
            "value".getBytes(Charsets.UTF_8)
        );

        // Validate
        assertNull("Should pass the null through", myValues);
    }

    /**
     * Test Implementation.
     */
    private static class MyScheme extends AbstractScheme {

        private boolean returnNull = false;

        @Override
        public List<Object> deserialize(ByteBuffer ser) {
            // If this flag is true, return null.
            if (returnNull) {
                return null;
            }

            // Probably a better way to do this juggling.
            if (ser == null) {
                return null;
            }
            ser.rewind();
            byte[] bytes = new byte[ser.remaining()];
            ser.get(bytes, 0, bytes.length);

            return Lists.newArrayList(new String(bytes, Charsets.UTF_8));
        }

        @Override
        public Fields getOutputFields() {
            return new Fields("MyFields", "Here", "For", "You");
        }

        public void setReturnNull(boolean returnNull) {
            this.returnNull = returnNull;
        }
    }
}