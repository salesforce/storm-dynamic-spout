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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Provides test coverage over Tools methods.
 */
public class ToolsTest {

    /**
     * Test that ImmutableCopy makes an immutable copy.
     */
    @Test
    public void testImmutableCopy() {
        // Lets create a map
        final Map<String, String> myMap = new HashMap<>();
        myMap.put("Key1", "Value1");
        myMap.put("Key2", "Value2");
        myMap.put("Key3", "Value3");

        // Now create a copy
        final Map<String, String> immutableMap = Tools.immutableCopy(myMap);

        // Validate
        assertNotNull(immutableMap, "Should not be null");
        assertEquals(3, immutableMap.size(), "Should have 3 keys");
        assertEquals("Value1", immutableMap.get("Key1"), "Check Key1");
        assertEquals("Value2", immutableMap.get("Key2"), "Check Key2");
        assertEquals("Value3", immutableMap.get("Key3"), "Check Key3");

        // Lets modify the original Map
        myMap.put("Key1", "Value4");

        // Validate copied made did not change
        assertEquals("Value1", immutableMap.get("Key1"), "Check Key1");

        // Clear original map again
        myMap.clear();

        // Revalidate copied map
        assertEquals(3, immutableMap.size(), "Should have 3 keys");
        assertEquals("Value1", immutableMap.get("Key1"), "Check Key1");
        assertEquals("Value2", immutableMap.get("Key2"), "Check Key2");
        assertEquals("Value3", immutableMap.get("Key3"), "Check Key3");

        Assertions.assertThrows(UnsupportedOperationException.class, () ->
            // Attempt ot modify the map, expect an exception
            immutableMap.put("Key4", "Value4")
        );
    }

    /**
     * Test that StripKey Prefix copies values from one map into a new map.
     */
    @Test
    public void testStripKeyPrefix() {
        final String prefix = "testPrefix.";
        final Map<String, String> sourceMap = new HashMap<>();
        sourceMap.put(prefix + "key1", "value1");
        sourceMap.put(prefix + "key2", "value2");
        sourceMap.put(prefix + "key3", "value3");
        sourceMap.put("key4", "value4");
        sourceMap.put("key5", "value5");

        // Now lets Strip-er
        final Map<String, String> strippedMap = Tools.stripKeyPrefix(prefix, sourceMap);

        // Validate we now have 3 keys
        assertNotNull(strippedMap, "Not null");
        assertEquals(3, strippedMap.size(), "Has 3 keys");

        // Should have our 3 keys w/o the prefix
        assertTrue(strippedMap.containsKey("key1"), "Has Key key1");
        assertEquals("value1", strippedMap.get("key1"), "Has value for key1");
        assertTrue(strippedMap.containsKey("key2"), "Has Key key2");
        assertEquals("value2", strippedMap.get("key2"), "Has value for key2");
        assertTrue(strippedMap.containsKey("key3"), "Has Key key3");
        assertEquals("value3", strippedMap.get("key3"), "Has value for key3");

        // Original map unchanged
        assertEquals(5, sourceMap.size(), "Has 3 keys");
        assertEquals("value1", sourceMap.get(prefix + "key1"), "Has value for key1");
        assertEquals("value2", sourceMap.get(prefix + "key2"), "Has value for key2");
        assertEquals("value3", sourceMap.get(prefix + "key3"), "Has value for key3");
        assertEquals("value4", sourceMap.get("key4"), "Has value for key4");
        assertEquals("value5", sourceMap.get("key5"), "Has value for key5");
    }

    /**
     * Call split and trim with null input, get null pointer.
     */
    @Test
    public void testSplitAndTrimWithNullInput() {
        Assertions.assertThrows(NullPointerException.class, () ->
            Tools.splitAndTrim(null)
        );
    }

    /**
     * Call split and trim with various input strings, validate we get the expected array of values back.
     */
    @ParameterizedTest
    @MethodSource("provideSplittableStrings")
    public void testSplitAndTrim(final String input, final String[] expectedOutputValues) {
        final String[] output = Tools.splitAndTrim(input);

        // validate
        assertNotNull(output);

        assertEquals(expectedOutputValues.length, output.length, "Should have expected number of results");
        for (int x = 0; x < expectedOutputValues.length; x++) {
            assertEquals(expectedOutputValues[x], output[x], "Has expected value");
        }
    }

    /**
     * Provides various inputs to be split.
     */
    public static Object[][] provideSplittableStrings() throws InstantiationException, IllegalAccessException {
        return new Object[][]{
            { "a,b,c,d", new String[] { "a", "b", "c", "d" } },
            { "my input", new String[] { "my input",} },
            { "my input, your input", new String[] { "my input", "your input"} },
            { "my input       ,         your    input   ", new String[] { "my input", "your    input"} },

            // A couple special cases
            { "a,b,", new String[] { "a","b" } },
            { "a,,b", new String[] { "a","b" } },
            { ",a,b", new String[] { "a","b" } }
        };
    }
}