package com.salesforce.storm.spout.sideline;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Deserializer;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Utf8StringDeserializer;
import com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers.FailedMsgRetryManager;
import com.salesforce.storm.spout.sideline.persistence.PersistenceManager;
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
        factoryManager.createNewDeserializerInstance();
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
        List<Deserializer> instances = Lists.newArrayList();
        for (int x=0; x<5; x++) {
            Deserializer deserializer = factoryManager.createNewDeserializerInstance();

            // Validate it
            assertNotNull(deserializer);
            assertTrue("Is correct instance", deserializer instanceof Utf8StringDeserializer);

            // Verify its a different instance than our previous ones
            assertFalse("Not a previous instance", instances.contains(deserializer));

            // Add to our list
            instances.add(deserializer);
        }
    }

    /**
     * Tests that if you fail to pass a deserializer config it throws an exception.
     */
    @Test
    public void testCreateNewFailedMsgRetryManagerInstance_missingConfig() {
        // Try with UTF8 String deserializer
        Map config = Maps.newHashMap();
        final FactoryManager factoryManager = new FactoryManager(config);

        // We expect this to throw an exception.
        expectedException.expect(IllegalStateException.class);
        factoryManager.createNewFailedMsgRetryManagerInstance();
    }

    /**
     * Tests that create new deserializer instance works as expected.
     */
    @Test
    public void testCreateNewFailedMsgRetryManager_usingDefaultImpl() {
        // Try with UTF8 String deserializer
        Map config = Maps.newHashMap();
        config.put(SidelineSpoutConfig.FAILED_MSG_RETRY_MANAGER_CLASS, "com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers.DefaultFailedMsgRetryManager");
        final FactoryManager factoryManager = new FactoryManager(config);

        // Create a few instances
        List<FailedMsgRetryManager> instances = Lists.newArrayList();
        for (int x=0; x<5; x++) {
            FailedMsgRetryManager retryManager = factoryManager.createNewFailedMsgRetryManagerInstance();

            // Validate it
            assertNotNull(retryManager);
            assertTrue("Is correct instance", retryManager instanceof FailedMsgRetryManager);

            // Verify its a different instance than our previous ones
            assertFalse("Not a previous instance", instances.contains(retryManager));

            // Add to our list
            instances.add(retryManager);
        }
    }

    /**
     * Tests that if you fail to pass a config it throws an exception.
     */
    @Test
    public void testCreateNewPersistenceManagerInstance_missingConfig() {
        // Try with UTF8 String deserializer
        Map config = Maps.newHashMap();
        final FactoryManager factoryManager = new FactoryManager(config);

        // We expect this to throw an exception.
        expectedException.expect(IllegalStateException.class);
        factoryManager.createNewPersistenceManagerInstance();
    }

    /**
     * Tests that create new deserializer instance works as expected.
     */
    @Test
    public void testCreateNewPersistenceManager_usingDefaultImpl() {
        // Try with UTF8 String deserializer
        Map config = Maps.newHashMap();
        config.put(SidelineSpoutConfig.PERSISTENCE_MANAGER_CLASS, "com.salesforce.storm.spout.sideline.persistence.ZookeeperPersistenceManager");
        final FactoryManager factoryManager = new FactoryManager(config);

        // Create a few instances
        List<PersistenceManager> instances = Lists.newArrayList();
        for (int x=0; x<5; x++) {
            PersistenceManager instance = factoryManager.createNewPersistenceManagerInstance();

            // Validate it
            assertNotNull(instance);
            assertTrue("Is correct instance", instance instanceof PersistenceManager);

            // Verify its a different instance than our previous ones
            assertFalse("Not a previous instance", instances.contains(instance));

            // Add to our list
            instances.add(instance);
        }
    }

}