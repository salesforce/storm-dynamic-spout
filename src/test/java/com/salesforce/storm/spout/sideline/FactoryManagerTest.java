package com.salesforce.storm.spout.sideline;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Deserializer;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Utf8StringDeserializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class FactoryManagerTest {

    /**
     * By default, no exceptions should be thrown.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Tests that if you fail to pass a deserializer config it throws an exception.
     */
    @Test
    public void testCreateNewDeserializerInstance_missingConfig() {
        // Try with UTF8 String deserializer
        Map config = Maps.newHashMap();
        final FactoryManager factoryManager = new FactoryManager(config);

        // We expect this to throw an exception.
        expectedException.expect(IllegalStateException.class);
        Deserializer deserializer = factoryManager.createNewDeserializerInstance();
    }

    /**
     * Tests that create new deserializer instance works as expected.
     */
    @Test
    public void testCreateNewDeserializerInstance_usingDefaultImpl() {
        // Try with UTF8 String deserializer
        Map config = Maps.newHashMap();
        config.put(SidelineSpoutConfig.DESERIALIZER_CLASS, "com.salesforce.storm.spout.sideline.kafka.deserializer.Utf8StringDeserializer");
        final FactoryManager factoryManager = new FactoryManager(config);

        // Create a few instances
        List<Deserializer> deserializers = Lists.newArrayList();
        for (int x=0; x<5; x++) {
            Deserializer deserializer = factoryManager.createNewDeserializerInstance();

            // Validate it
            assertNotNull(deserializer);
            assertTrue("Is correct instance", deserializer instanceof Utf8StringDeserializer);

            // Verify its a different instance than our previous ones
            assertFalse("Not a previous instance", deserializers.contains(deserializer));

            // Add to our list
            deserializers.add(deserializer);
        }
    }

}