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

package com.salesforce.storm.spout.sideline.recipes.trigger;

import com.salesforce.kafka.test.junit5.SharedZookeeperTestResource;
import com.salesforce.storm.spout.dynamic.Tools;
import com.salesforce.storm.spout.dynamic.filter.FilterChainStep;
import com.salesforce.storm.spout.dynamic.persistence.zookeeper.CuratorFactory;
import com.salesforce.storm.spout.dynamic.persistence.zookeeper.CuratorHelper;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;
import com.salesforce.storm.spout.sideline.recipes.trigger.zookeeper.Config;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test the {@link TriggerEventHelper} to make sure it starts, resumes and resolves sidelines for the
 * recipe implementation.
 */
class TriggerEventHelperTest {

    /**
     * Create shared zookeeper test server.
     */
    @RegisterExtension
    static final SharedZookeeperTestResource sharedZookeeperTestResource = new SharedZookeeperTestResource();

    private static Map<String, Object> config = new HashMap<>();
    private static TriggerEventHelper triggerEventHelper;
    private static CuratorFramework curator;
    private static CuratorHelper curatorHelper;

    @BeforeAll
    static void setUp() {
        final String zkRoot = "/test-trigger" + System.currentTimeMillis();

        config.put(Config.ZK_ROOT, zkRoot);
        config.put(Config.ZK_SERVERS, Collections.singletonList(
            sharedZookeeperTestResource.getZookeeperTestServer().getConnectString()
        ));
        config.put(SidelineConfig.FILTER_CHAIN_STEP_CLASS, KeyFilter.class.getName());

        curator = CuratorFactory.createNewCuratorInstance(
            Tools.stripKeyPrefix(Config.PREFIX, config),
            TriggerEventHelperTest.class.getSimpleName()
        );

        curatorHelper = new CuratorHelper(curator, config);

        triggerEventHelper = new TriggerEventHelper(
            config,
            curator,
            curatorHelper
        );
    }

    @AfterAll
    static void tearDown() {
        curator.close();
    }

    @Test
    void startTriggerEvent() {
        final FilterChainStep filterChainStep = new KeyFilter(Collections.singletonList("key1"));

        final String createdBy = "Stan Lemon";
        final String description = "Testing trigger events.";

        // Start a new sideline using a trigger event
        final String triggerEventId = triggerEventHelper.startTriggerEvent(
            filterChainStep,
            createdBy,
            description
        );

        final TriggerEvent triggerEvent = curatorHelper.readJson(
            config.get(Config.ZK_ROOT) + "/" + triggerEventId,
            TriggerEvent.class
        );

        assertEquals(SidelineType.START, triggerEvent.getType());
        assertEquals(createdBy, triggerEvent.getCreatedBy());
        assertEquals(description, triggerEvent.getDescription());
        assertNotNull(triggerEvent.getFilterChainStep());
    }

    @Test
    void resumeTriggerEvent() {
        final FilterChainStep filterChainStep = new KeyFilter(Collections.singletonList("key1"));

        final String createdBy = "Stan Lemon";
        final String description = "Testing trigger events.";

        // Start a new sideline using a trigger event
        final String triggerEventId = triggerEventHelper.startTriggerEvent(
            filterChainStep,
            createdBy,
            description
        );

        triggerEventHelper.resumeTriggerEvent(triggerEventId);

        final TriggerEvent triggerEvent = curatorHelper.readJson(
            config.get(Config.ZK_ROOT) + "/" + triggerEventId,
            TriggerEvent.class
        );

        assertEquals(SidelineType.RESUME, triggerEvent.getType());
    }

    @Test
    void resolveTriggerEvent() {
        final FilterChainStep filterChainStep = new KeyFilter(Collections.singletonList("key1"));

        final String createdBy = "Stan Lemon";
        final String description = "Testing trigger events.";

        // Start a new sideline using a trigger event
        final String triggerEventId = triggerEventHelper.startTriggerEvent(
            filterChainStep,
            createdBy,
            description
        );

        triggerEventHelper.resolveTriggerEvent(triggerEventId);

        final TriggerEvent triggerEvent = curatorHelper.readJson(
            config.get(Config.ZK_ROOT) + "/" + triggerEventId,
            TriggerEvent.class
        );

        assertEquals(SidelineType.RESOLVE, triggerEvent.getType());
    }
}