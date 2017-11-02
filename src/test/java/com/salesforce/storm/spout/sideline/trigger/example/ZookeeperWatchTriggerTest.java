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
import com.salesforce.storm.spout.dynamic.Tools;
import com.salesforce.storm.spout.dynamic.filter.FilterChainStep;
import com.salesforce.storm.spout.dynamic.filter.StaticMessageFilter;
import com.salesforce.storm.spout.dynamic.persistence.zookeeper.CuratorFactory;
import com.salesforce.storm.spout.dynamic.utils.SharedZookeeperTestResource;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;
import com.salesforce.storm.spout.sideline.handler.SidelineSpoutHandler;
import com.salesforce.storm.spout.sideline.persistence.InMemoryPersistenceAdapter;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.notNullValue;

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
    public void testOpen() throws Exception {
        final String zkRoot = "/test-trigger";

        final Map<String,Object> config = new HashMap<>();
        config.put(Config.ZK_SERVERS, Collections.singletonList(getZkServer().getConnectString()));
        config.put(Config.ZK_ROOTS, Collections.singletonList(zkRoot));
        config.put(
            SidelineConfig.PERSISTENCE_ADAPTER_CLASS,
            InMemoryPersistenceAdapter.class.getName()
        );
        config.put(Config.FILTER_CHAIN_STEP_BUILDER_CLASS, MockFilterChainStepBuilder.class.getName());

        final CuratorFramework currator = CuratorFactory.createNewCuratorInstance(
            Tools.stripKeyPrefix(Config.PREFIX, config)
        );

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);

        final ZookeeperWatchTrigger trigger = new ZookeeperWatchTrigger();
        trigger.setSidelineController(sidelineSpoutHandler);
        trigger.open(config);

        // We're going to turn some events into JSON
        final Gson gson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd HH:mm:ss")
            .create();

        final Map<String,Object> startData = new HashMap<>();

        final TriggerEvent startTriggerEvent = new TriggerEvent(
            SidelineType.START,
            startData,
            new Date(),
            CREATED_BY,
            DESCRIPTION
        );

        final String id1 = "foo";

        final Map<String,Object> stopData = new HashMap<>();

        final TriggerEvent stopTriggerEvent = new TriggerEvent(
            SidelineType.STOP,
            stopData,
            new Date(),
            CREATED_BY,
            DESCRIPTION
        );

        final String id2 = "bar";

        final String path1 = zkRoot + "/" + id1;

        // Create a starting request
        currator
            .create()
            .creatingParentsIfNeeded()
            .forPath(zkRoot + "/" + id1, gson.toJson(startTriggerEvent).getBytes());

        final String path2 = zkRoot + "/" + id2;

        // Create a stopping request
        currator
            .create()
            .creatingParentsIfNeeded()
            .forPath(path2, gson.toJson(stopTriggerEvent).getBytes());

        // Since this is an async operation, use await() to watch for when the nodes are created
        await()
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> currator.checkExists().forPath(path1), notNullValue());

        await()
            .atMost(5, TimeUnit.SECONDS)
            .until(() -> currator.checkExists().forPath(path2), notNullValue());

        // Clean it all up
        trigger.close();
        currator.close();
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
            // Temp
            return new StaticMessageFilter();
        }
    }
}