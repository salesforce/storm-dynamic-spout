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

package com.salesforce.storm.spout.sideline.recipes.trigger.zookeeper;

import com.salesforce.kafka.test.junit5.SharedZookeeperTestResource;
import com.salesforce.storm.spout.dynamic.Tools;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.filter.FilterChainStep;
import com.salesforce.storm.spout.dynamic.mocks.MockConsumer;
import com.salesforce.storm.spout.dynamic.filter.StaticMessageFilter;
import com.salesforce.storm.spout.dynamic.persistence.zookeeper.CuratorFactory;
import com.salesforce.storm.spout.dynamic.persistence.zookeeper.CuratorHelper;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;
import com.salesforce.storm.spout.sideline.handler.SidelineSpoutHandler;
import com.salesforce.storm.spout.sideline.persistence.InMemoryPersistenceAdapter;
import com.salesforce.storm.spout.sideline.recipes.trigger.TriggerEvent;
import com.salesforce.storm.spout.sideline.recipes.trigger.TriggerEventHelper;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test that the reference implementation for sideline triggers works correctly.
 */
class ZookeeperWatchTriggerTest {

    private static final String CREATED_BY = "CreatedBy";
    private static final String REASON = "Reason";

    /**
     * Create shared zookeeper test server.
     */
    @RegisterExtension
    static final SharedZookeeperTestResource sharedZookeeperTestResource = new SharedZookeeperTestResource();

    /**
     * Test that when a new {@link TriggerEvent} is created it picked up by the watch trigger.
     */
    @Test
    void testOpenWithNewEvents() {
        final String zkRoot = "/test-trigger" + System.currentTimeMillis();

        final String consumerId = "VirtualSpoutPrefix";

        final Map<String, Object> config = SpoutConfig.setDefaults(new HashMap<>());
        config.put(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX, consumerId);
        config.put(Config.ZK_SERVERS, Collections.singletonList(getZookeeperConnectionString()));
        config.put(Config.ZK_ROOT, zkRoot);
        config.put(SpoutConfig.CONSUMER_CLASS, MockConsumer.class.getName());
        config.put(
            SpoutConfig.PERSISTENCE_ADAPTER_CLASS,
            com.salesforce.storm.spout.dynamic.persistence.InMemoryPersistenceAdapter.class.getName()
        );
        config.put(
            SidelineConfig.PERSISTENCE_ADAPTER_CLASS,
            InMemoryPersistenceAdapter.class.getName()
        );
        config.put(SidelineConfig.FILTER_CHAIN_STEP_CLASS, StaticMessageFilter.class.getName());

        final CuratorFramework curator = CuratorFactory.createNewCuratorInstance(
            Tools.stripKeyPrefix(Config.PREFIX, config),
            getClass().getSimpleName()
        );
        final CuratorHelper curatorHelper = new CuratorHelper(curator, config);

        final SidelineSpoutHandler sidelineSpoutHandler = Mockito.mock(SidelineSpoutHandler.class);

        final TriggerEventHelper triggerEventHelper = new TriggerEventHelper(config);

        final ZookeeperWatchTrigger trigger = new ZookeeperWatchTrigger();
        trigger.setSidelineController(sidelineSpoutHandler);
        trigger.open(config);

        final FilterChainStep startFilterChainStep = new StaticMessageFilter("start1");

        final String id1 = triggerEventHelper.startTriggerEvent(startFilterChainStep, CREATED_BY, REASON);
        final String path1 = zkRoot + "/" + id1;

        // Since this is an async operation, use await() to watch for when the nodes are created
        await()
            .until(() -> curator.checkExists().forPath(path1), notNullValue());

        await()
            .until(() -> findSidelineRequest("start1", trigger.getSidelineRequests()), notNullValue());

        Mockito.verify(sidelineSpoutHandler).start(
            findSidelineRequest("start1", trigger.getSidelineRequests())
        );

        assertTrue(
            curatorHelper.readJson(path1, TriggerEvent.class).isProcessed(),
            "Starting trigger has been processed"
        );

        // Clean it all up
        trigger.close();
        curator.close();
    }

    /**
     * Test that when a {@link TriggerEvent} is created before the watch trigger is open it will get picked up when it finally starts.
     * @throws Exception something bad
     */
    @Test
    void testOpenWithExistingState() throws Exception {
        final String zkRoot = "/test-trigger" + System.currentTimeMillis();

        final String consumerId = "VirtualSpoutPrefix";

        final Map<String, Object> config = SpoutConfig.setDefaults(new HashMap<>());
        config.put(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX, consumerId);
        config.put(Config.ZK_SERVERS, Collections.singletonList(getZookeeperConnectionString()));
        config.put(Config.ZK_ROOT, zkRoot);
        config.put(SpoutConfig.CONSUMER_CLASS, MockConsumer.class.getName());
        config.put(
            SpoutConfig.PERSISTENCE_ADAPTER_CLASS,
            com.salesforce.storm.spout.dynamic.persistence.InMemoryPersistenceAdapter.class.getName()
        );
        config.put(
            SidelineConfig.PERSISTENCE_ADAPTER_CLASS,
            InMemoryPersistenceAdapter.class.getName()
        );
        config.put(SidelineConfig.FILTER_CHAIN_STEP_CLASS, StaticMessageFilter.class.getName());

        final CuratorFramework curator = CuratorFactory.createNewCuratorInstance(
            Tools.stripKeyPrefix(Config.PREFIX, config),
            getClass().getSimpleName()
        );
        final CuratorHelper curatorHelper = new CuratorHelper(curator, config);

        final SidelineSpoutHandler sidelineSpoutHandler = Mockito.mock(SidelineSpoutHandler.class);

        final TriggerEventHelper triggerEventHelper = new TriggerEventHelper(config);

        // This is a garbage trigger, we do this so we can use it's Gson instance
        final ZookeeperWatchTrigger trigger1 = new ZookeeperWatchTrigger();
        trigger1.setSidelineController(sidelineSpoutHandler);
        trigger1.open(config);
        trigger1.close();

        assertTrue(trigger1.getSidelineRequests().isEmpty(), "There should not be any sideline requests");

        final FilterChainStep startFilterChainStep = new StaticMessageFilter("start2");

        // Create this before we spin up our working trigger
        final String id1 = triggerEventHelper.startTriggerEvent(startFilterChainStep, CREATED_BY, REASON);
        final String path1 = zkRoot + "/" + id1;

        // Since this is an async operation, use await() to watch for when the nodes are created
        await()
            .until(() -> curator.checkExists().forPath(path1), notNullValue());

        // This is the real trigger, and it should pick up the sideline request it missed while down
        final ZookeeperWatchTrigger trigger2 = new ZookeeperWatchTrigger();
        trigger2.setSidelineController(sidelineSpoutHandler);
        trigger2.open(config);

        // We should find the trigger created before startup
        await()
            .until(() -> findSidelineRequest("start2", trigger2.getSidelineRequests()), notNullValue());

        // Clean it all up
        trigger2.close();
        curator.close();
    }

    private SidelineRequest findSidelineRequest(final String id, Set<SidelineRequest> sidelineRequests) {
        for (final SidelineRequest sidelineRequest : sidelineRequests) {
            StaticMessageFilter filterChainStep = (StaticMessageFilter) sidelineRequest.step;

            if (filterChainStep.getId().equals(id)) {
                return sidelineRequest;
            }
        }

        return null;
    }

    private String getZookeeperConnectionString() {
        return sharedZookeeperTestResource.getZookeeperTestServer().getConnectString();
    }
}