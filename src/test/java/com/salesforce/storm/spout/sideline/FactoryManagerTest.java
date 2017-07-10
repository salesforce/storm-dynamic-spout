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
package com.salesforce.storm.spout.sideline;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.config.SpoutConfig;
import com.salesforce.storm.spout.sideline.retry.DefaultRetryManager;
import com.salesforce.storm.spout.sideline.retry.NeverRetryManager;
import com.salesforce.storm.spout.sideline.retry.RetryManager;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.persistence.ZookeeperPersistenceAdapter;
import com.salesforce.storm.spout.sideline.buffer.FifoBuffer;
import com.salesforce.storm.spout.sideline.buffer.RoundRobinBuffer;
import com.salesforce.storm.spout.sideline.buffer.MessageBuffer;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(DataProviderRunner.class)
public class FactoryManagerTest {

    /**
     * Tests that if you fail to pass a deserializer config it throws an exception.
     */
    @Rule
    public ExpectedException expectedExceptionCreateNewFailedMsgRetryManagerInstance_missingConfig = ExpectedException.none();
    @Test
    public void testCreateNewFailedMsgRetryManagerInstance_missingConfig() {
        // Try with UTF8 String deserializer
        final Map config = Maps.newHashMap();
        final FactoryManager factoryManager = new FactoryManager(config);

        // We expect this to throw an exception.
        expectedExceptionCreateNewFailedMsgRetryManagerInstance_missingConfig.expect(IllegalStateException.class);
        factoryManager.createNewFailedMsgRetryManagerInstance();
    }

    /**
     * Provides various tuple buffer implementation.
     */
    @DataProvider
    public static Object[][] provideFailedMsgRetryManagerClasses() {
        return new Object[][]{
            { NeverRetryManager.class },
            { DefaultRetryManager.class }
        };
    }

    /**
     * Tests that create new deserializer instance works as expected.
     */
    @Test
    @UseDataProvider("provideFailedMsgRetryManagerClasses")
    public void testCreateNewFailedMsgRetryManager(final Class clazz) {
        // Try with UTF8 String deserializer
        final Map config = Maps.newHashMap();
        config.put(SpoutConfig.RETRY_MANAGER_CLASS, clazz.getName());
        final FactoryManager factoryManager = new FactoryManager(config);

        // Create a few instances
        final List<RetryManager> instances = Lists.newArrayList();
        for (int x=0; x<5; x++) {
            final RetryManager retryManager = factoryManager.createNewFailedMsgRetryManagerInstance();

            // Validate it
            assertNotNull(retryManager);
            assertEquals("Is correct instance type", retryManager.getClass(), clazz);

            // Verify its a different instance than our previous ones
            assertFalse("Not a previous instance", instances.contains(retryManager));

            // Add to our list
            instances.add(retryManager);
        }
    }

    /**
     * Tests that if you fail to pass a config it throws an exception.
     */
    @Rule
    public ExpectedException expectedExceptionCreateNewPersistenceAdapterInstance_missingConfig = ExpectedException.none();
    @Test
    public void testCreateNewPersistenceAdapterInstance_missingConfig() {
        // Try with UTF8 String deserializer
        final Map config = Maps.newHashMap();
        final FactoryManager factoryManager = new FactoryManager(config);

        // We expect this to throw an exception.
        expectedExceptionCreateNewPersistenceAdapterInstance_missingConfig.expect(IllegalStateException.class);
        factoryManager.createNewPersistenceAdapterInstance();
    }

    /**
     * Tests that create new deserializer instance works as expected.
     */
    @Test
    public void testCreateNewPersistenceAdapter_usingDefaultImpl() {
        // Try with UTF8 String deserializer
        final Map config = Maps.newHashMap();
        config.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, "com.salesforce.storm.spout.sideline.persistence.ZookeeperPersistenceAdapter");
        final FactoryManager factoryManager = new FactoryManager(config);

        // Create a few instances
        final List<PersistenceAdapter> instances = Lists.newArrayList();
        for (int x=0; x<5; x++) {
            final PersistenceAdapter instance = factoryManager.createNewPersistenceAdapterInstance();

            // Validate it
            assertNotNull(instance);
            assertTrue("Is correct instance", instance instanceof ZookeeperPersistenceAdapter);

            // Verify its a different instance than our previous ones
            assertFalse("Not a previous instance", instances.contains(instance));

            // Add to our list
            instances.add(instance);
        }
    }

    /**
     * Tests that if you fail to pass a deserializer config it throws an exception.
     */
    @Rule
    public ExpectedException expectedExceptionCreateNewMessageBufferInstance_missingConfig = ExpectedException.none();
    @Test
    public void testCreateNewMessageBufferInstance_missingConfig() {
        // Try with UTF8 String deserializer
        final Map config = Maps.newHashMap();
        final FactoryManager factoryManager = new FactoryManager(config);

        // We expect this to throw an exception.
        expectedExceptionCreateNewMessageBufferInstance_missingConfig.expect(IllegalStateException.class);
        factoryManager.createNewMessageBufferInstance();
    }

    /**
     * Provides various tuple buffer implementation.
     */
    @DataProvider
    public static Object[][] provideMessageBufferClasses() {
        return new Object[][]{
                { FifoBuffer.class },
                { RoundRobinBuffer.class }
        };
    }

    /**
     * Tests that create new deserializer instance works as expected.
     */
    @Test
    @UseDataProvider("provideMessageBufferClasses")
    public void testCreateNewMessageBuffer(final Class clazz) {
        // Try with UTF8 String deserializer
        final Map config = Maps.newHashMap();
        config.put(SpoutConfig.TUPLE_BUFFER_CLASS, clazz.getName());
        final FactoryManager factoryManager = new FactoryManager(config);

        // Create a few instances
        final List<MessageBuffer> instances = Lists.newArrayList();
        for (int x=0; x<5; x++) {
            final MessageBuffer messageBuffer = factoryManager.createNewMessageBufferInstance();

            // Validate it
            assertNotNull(messageBuffer);
            assertEquals("Is correct instance type", messageBuffer.getClass(), clazz);

            // Verify its a different instance than our previous ones
            assertFalse("Not a previous instance", instances.contains(messageBuffer));

            // Add to our list
            instances.add(messageBuffer);
        }
    }
}