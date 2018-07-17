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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests that the JSON abstraction converts to and from JSON correctly.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class JSONTest {

    // Why the heck am I double wrapping the map? Because GSON can't handle inner classes...
    private final Map<String, Object> jsonMap = new HashMap<>(new HashMap<String, Object>() {
        {
            put("key1", "value1");
            put("key2", 2.0);
            put("key3", true);
        }
    });

    // Note GSON favors doubles, so if I gave it just "2" I'd still get 2.0 when it converts to JSON
    private final String json = "{\"key1\":\"value1\",\"key2\":2.0,\"key3\":true}";

    /**
     * Test that given a Map we get a valid string of JSON back.
     */
    @Test
    void testTo() {
        assertEquals(
            json,
            JSON.to(jsonMap)
        );
    }

    /**
     * Test that given a string of JSON we get a valid HashMap back.
     */
    @Test
    void testFrom() {
        assertEquals(
            jsonMap,
            JSON.from(json, HashMap.class)
        );
    }
}