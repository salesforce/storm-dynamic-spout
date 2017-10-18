package com.salesforce.storm.spout.dynamic;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ToolsTest {

    /**
     * Expect no expections by default.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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
        assertNotNull("Should not be null", immutableMap);
        assertEquals("Should have 3 keys", 3, immutableMap.size());
        assertEquals("Check Key1", "Value1", immutableMap.get("Key1"));
        assertEquals("Check Key2", "Value2", immutableMap.get("Key2"));
        assertEquals("Check Key3", "Value3", immutableMap.get("Key3"));

        // Lets modify the original Map
        myMap.put("Key1", "Value4");

        // Validate copied made did not change
        assertEquals("Check Key1", "Value1", immutableMap.get("Key1"));

        // Clear original map again
        myMap.clear();

        // Revalidate copied map
        assertEquals("Should have 3 keys", 3, immutableMap.size());
        assertEquals("Check Key1", "Value1", immutableMap.get("Key1"));
        assertEquals("Check Key2", "Value2", immutableMap.get("Key2"));
        assertEquals("Check Key3", "Value3", immutableMap.get("Key3"));

        // Attempt ot modify the map, expect an exception
        expectedException.expect(UnsupportedOperationException.class);
        immutableMap.put("Key4", "Value4");
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
        assertNotNull("Not null", strippedMap);
        assertEquals("Has 3 keys", 3, strippedMap.size());

        // Should have our 3 keys w/o the prefix
        assertTrue("Has Key key1", strippedMap.containsKey("key1"));
        assertEquals("Has value for key1", "value1", strippedMap.get("key1"));
        assertTrue("Has Key key2", strippedMap.containsKey("key2"));
        assertEquals("Has value for key2", "value2", strippedMap.get("key2"));
        assertTrue("Has Key key3", strippedMap.containsKey("key3"));
        assertEquals("Has value for key3", "value3", strippedMap.get("key3"));

        // Original map unchanged
        assertEquals("Has 3 keys", 5, sourceMap.size());
        assertEquals("Has value for key1", "value1", sourceMap.get(prefix + "key1"));
        assertEquals("Has value for key2", "value2", sourceMap.get(prefix + "key2"));
        assertEquals("Has value for key3", "value3", sourceMap.get(prefix + "key3"));
        assertEquals("Has value for key4", "value4", sourceMap.get("key4"));
        assertEquals("Has value for key5", "value5", sourceMap.get("key5"));
    }
}