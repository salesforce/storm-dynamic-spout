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

package com.salesforce.storm.spout.sideline.trigger.example;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.salesforce.kafka.test.junit4.SharedZookeeperTestResource;
import com.salesforce.storm.spout.dynamic.Tools;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.mocks.MockConsumer;
import com.salesforce.storm.spout.dynamic.filter.FilterChainStep;
import com.salesforce.storm.spout.dynamic.filter.StaticMessageFilter;
import com.salesforce.storm.spout.dynamic.persistence.zookeeper.CuratorFactory;
import com.salesforce.storm.spout.dynamic.persistence.zookeeper.CuratorHelper;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;
import com.salesforce.storm.spout.sideline.handler.SidelineSpoutHandler;
import com.salesforce.storm.spout.sideline.persistence.InMemoryPersistenceAdapter;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertTrue;

/**
 * Test that the reference implementation for sideline triggers works correctly.
 */
public class ZookeeperWatchTriggerTest {

    private static final String CREATED_BY = "Test";
    private static final String DESCRIPTION = "Description";

    /**
     * Create shared zookeeper test server.
     */
    @ClassRule
    public static final SharedZookeeperTestResource sharedZookeeperTestResource = new SharedZookeeperTestResource();

    /**
     * Test that the sideline trigger opens and that it calls start and stop properly when nodes are changed.
     * @throws Exception something bad
     */
    @Test
    public void testOpenWithNewEvents() throws Exception {
        final String zkRoot = "/test-trigger" + System.currentTimeMillis();

        final String consumerId = "VirtualSpoutPrefix";

        final Map<String, Object> config = SpoutConfig.setDefaults(new HashMap<>());
        config.put(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX, consumerId);
        config.put(Config.ZK_SERVERS, Collections.singletonList(getZkServer().getConnectString()));
        config.put(Config.ZK_ROOTS, Collections.singletonList(zkRoot));
        config.put(SpoutConfig.CONSUMER_CLASS, MockConsumer.class.getName());
        config.put(
            SpoutConfig.PERSISTENCE_ADAPTER_CLASS,
            com.salesforce.storm.spout.dynamic.persistence.InMemoryPersistenceAdapter.class.getName()
        );
        config.put(
            SidelineConfig.PERSISTENCE_ADAPTER_CLASS,
            InMemoryPersistenceAdapter.class.getName()
        );
        config.put(Config.FILTER_CHAIN_STEP_BUILDER_CLASS, MockFilterChainStepBuilder.class.getName());

        final CuratorFramework currator = CuratorFactory.createNewCuratorInstance(
            Tools.stripKeyPrefix(Config.PREFIX, config),
            getClass().getSimpleName()
        );
        final CuratorHelper curatorHelper = new CuratorHelper(currator);

        final SidelineSpoutHandler sidelineSpoutHandler = Mockito.mock(SidelineSpoutHandler.class);

        final ZookeeperWatchTrigger trigger = new ZookeeperWatchTrigger();
        trigger.setSidelineController(sidelineSpoutHandler);
        trigger.open(config);

        // We're going to turn some events into JSON
        final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();

        final Map<String,Object> startData = new HashMap<>();
        startData.put("id", "start");
        final TriggerEvent startTriggerEvent = new TriggerEvent(
            SidelineType.START,
            startData,
            LocalDateTime.now(),
            CREATED_BY,
            DESCRIPTION,
            false,
            LocalDateTime.now()
        );
        final String id1 = "foo" + System.currentTimeMillis();
        final String path1 = zkRoot + "/" + id1;

        // Create a starting request
        currator
            .create()
            .creatingParentsIfNeeded()
            .forPath(zkRoot + "/" + id1, gson.toJson(startTriggerEvent).getBytes());

        // Since this is an async operation, use await() to watch for when the nodes are created
        await()
            .until(() -> currator.checkExists().forPath(path1), notNullValue());

        await()
            .until(() -> findSidelineRequest("start", trigger.getSidelineRequests()), notNullValue());

        Mockito.verify(sidelineSpoutHandler).start(
            findSidelineRequest("start", trigger.getSidelineRequests())
        );

        final Map<String,Object> stopData = new HashMap<>();
        stopData.put("id", "stop");
        final TriggerEvent stopTriggerEvent = new TriggerEvent(
            SidelineType.RESOLVE,
            stopData,
            LocalDateTime.now(),
            CREATED_BY,
            DESCRIPTION,
            false,
            LocalDateTime.now()
        );
        final String id2 = "bar" + System.currentTimeMillis();
        final String path2 = zkRoot + "/" + id2;

        // Create a stopping request
        currator
            .create()
            .creatingParentsIfNeeded()
            .forPath(path2, gson.toJson(stopTriggerEvent).getBytes());

        await()
            .until(() -> currator.checkExists().forPath(path2), notNullValue());

        await()
            .until(() -> findSidelineRequest("stop", trigger.getSidelineRequests()), notNullValue());

        Mockito.verify(sidelineSpoutHandler).resolve(
            findSidelineRequest("stop", trigger.getSidelineRequests())
        );

        assertTrue(
            "Starting trigger has been processed",
            curatorHelper.readJson(path1, TriggerEvent.class).isProcessed()
        );

        assertTrue(
            "Stopping trigger has been processed",
            curatorHelper.readJson(path2, TriggerEvent.class).isProcessed()
        );

        // Clean it all up
        trigger.close();
        currator.close();
    }

    /**
     * Test that the sideline trigger opens and that it calls start and stop properly when nodes are changed.
     * @throws Exception something bad
     */
    @Test
    public void testOpenWithExistingState() throws Exception {
        final String zkRoot = "/test-trigger" + System.currentTimeMillis();

        final String consumerId = "VirtualSpoutPrefix";

        final Map<String, Object> config = SpoutConfig.setDefaults(new HashMap<>());
        config.put(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX, consumerId);
        config.put(Config.ZK_SERVERS, Collections.singletonList(getZkServer().getConnectString()));
        config.put(Config.ZK_ROOTS, Collections.singletonList(zkRoot));
        config.put(SpoutConfig.CONSUMER_CLASS, MockConsumer.class.getName());
        config.put(
            SpoutConfig.PERSISTENCE_ADAPTER_CLASS,
            com.salesforce.storm.spout.dynamic.persistence.InMemoryPersistenceAdapter.class.getName()
        );
        config.put(
            SidelineConfig.PERSISTENCE_ADAPTER_CLASS,
            InMemoryPersistenceAdapter.class.getName()
        );
        config.put(Config.FILTER_CHAIN_STEP_BUILDER_CLASS, MockFilterChainStepBuilder.class.getName());

        final CuratorFramework currator = CuratorFactory.createNewCuratorInstance(
            Tools.stripKeyPrefix(Config.PREFIX, config),
            getClass().getSimpleName()
        );
        final CuratorHelper curatorHelper = new CuratorHelper(currator);

        // We're going to turn some events into JSON
        final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();

        final Map<String,Object> startData = new HashMap<>();
        startData.put("id", "start");
        final TriggerEvent startTriggerEvent = new TriggerEvent(
            SidelineType.START,
            startData,
            LocalDateTime.now(),
            CREATED_BY,
            DESCRIPTION,
            false,
            LocalDateTime.now()
        );
        final String id1 = "foo" + System.currentTimeMillis();
        final String path1 = zkRoot + "/" + id1;
        final String startTriggerEventJson = gson.toJson(startTriggerEvent);

        // Create a starting request
        currator
            .create()
            .creatingParentsIfNeeded()
            .forPath(zkRoot + "/" + id1, startTriggerEventJson.getBytes());

        // Since this is an async operation, use await() to watch for when the nodes are created
        await()
            .until(() -> currator.checkExists().forPath(path1), notNullValue());

        final Map<String,Object> stopData = new HashMap<>();
        stopData.put("id", "stop");
        final TriggerEvent stopTriggerEvent = new TriggerEvent(
            SidelineType.RESOLVE,
            stopData,
            LocalDateTime.now(),
            CREATED_BY,
            DESCRIPTION,
            false,
            LocalDateTime.now()
        );
        final String id2 = "bar" + System.currentTimeMillis();
        final String path2 = zkRoot + "/" + id2;
        final String stopTriggerEventJson = gson.toJson(stopTriggerEvent);

        // Create a stopping request
        currator
            .create()
            .creatingParentsIfNeeded()
            .forPath(path2, stopTriggerEventJson.getBytes());

        await()
            .until(() -> currator.checkExists().forPath(path2), notNullValue());

        final SidelineSpoutHandler sidelineSpoutHandler = Mockito.mock(SidelineSpoutHandler.class);

        final ZookeeperWatchTrigger trigger = new ZookeeperWatchTrigger();
        trigger.setSidelineController(sidelineSpoutHandler);
        trigger.open(config);

        await()
            .until(() -> findSidelineRequest("start", trigger.getSidelineRequests()), notNullValue());

        Mockito.verify(sidelineSpoutHandler).start(
            findSidelineRequest("start", trigger.getSidelineRequests())
        );

        await()
            .until(() -> findSidelineRequest("stop", trigger.getSidelineRequests()), notNullValue());

        Mockito.verify(sidelineSpoutHandler).resolve(
            findSidelineRequest("stop", trigger.getSidelineRequests())
        );

        assertTrue(
            "Starting trigger has been processed",
            curatorHelper.readJson(path1, TriggerEvent.class).isProcessed()
        );

        assertTrue(
            "Stopping trigger has been processed",
            curatorHelper.readJson(path2, TriggerEvent.class).isProcessed()
        );

        // Clean it all up
        trigger.close();
        currator.close();
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

    private TestingServer getZkServer() {
        return sharedZookeeperTestResource.getZookeeperTestServer();
    }

    /**
     * {@link FilterChainStepBuilder} implementation for testing.
     */
    public static class MockFilterChainStepBuilder implements FilterChainStepBuilder {

        @Override
        public FilterChainStep build(final Map<String,Object> data) {
            return new StaticMessageFilter((String) data.get("id"));
        }
    }
}