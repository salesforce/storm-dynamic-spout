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

import com.google.common.base.Preconditions;
import com.salesforce.storm.spout.dynamic.JSON;
import com.salesforce.storm.spout.dynamic.Tools;
import com.salesforce.storm.spout.dynamic.persistence.zookeeper.CuratorFactory;
import com.salesforce.storm.spout.dynamic.persistence.zookeeper.CuratorHelper;
import com.salesforce.storm.spout.sideline.recipes.trigger.zookeeper.Config;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Start, resume and resolve sidelines using {@link TriggerEvent} instances.
 *
 * This class can be used from external tooling to control sidelines using this recipe.
 */
public class TriggerEventHelper {

    private static final Logger logger = LoggerFactory.getLogger(TriggerEventHelper.class);

    private final Map<String, Object> config;
    private final CuratorFramework curator;
    private final CuratorHelper curatorHelper;

    /**
     * Start, resume and resolve sidelines using {@link TriggerEvent} instances.
     * @param config same configuration utilized by the topology.
     */
    public TriggerEventHelper(final Map<String, Object> config) {
        this.config = config;

        curator = CuratorFactory.createNewCuratorInstance(
            Tools.stripKeyPrefix(Config.PREFIX, config),
            "sideline"
        );

        curatorHelper = new CuratorHelper(curator, config);
    }

    /**
     * Create a new helper, but provide the curator and helper instances - useful only for testing.
     * @param config same configuration utilized by the topology.
     * @param curator curator instance
     * @param curatorHelper curator helper instance
     */
    TriggerEventHelper(final Map<String, Object> config, CuratorFramework curator, CuratorHelper curatorHelper) {
        this.config = config;
        this.curator = curator;
        this.curatorHelper = curatorHelper;
    }

    /**
     * Create a {@link TriggerEvent} to start a sideline.
     *
     * @param data data for the trigger event, this will be handed off to the {@link FilterChainStepBuilder}.
     * @param createdBy the name or a way of identifying who or what started the sideline.
     * @param reason the reason for starting the sideline.
     * @return the identifier of the sideline.
     */
    public String startTriggerEvent(
        final Map<String, Object> data,
        final String createdBy,
        final String reason
    ) {
        Preconditions.checkArgument(
            data != null && !data.isEmpty(),
            "TriggerEvent's require data"
        );

        final LocalDateTime createdAt = LocalDateTime.now();

        final TriggerEvent triggerEvent = new TriggerEvent(
            SidelineType.START,
            data,
            createdAt,
            createdBy,
            reason,
            false,
            createdAt
        );

        final String id = getMd5Hash(data);
        final String path = getZkRoot() + "/" + id;

        logger.info("Sending trigger event to start sideline {}", id);

        curatorHelper.writeJson(path, triggerEvent);

        logger.info("Saved to {}", path);

        return id;
    }

    /**
     * Update an existing {@link TriggerEvent} to resume a sideline.
     * @param id the identifier of the sideline.
     */
    public void resumeTriggerEvent(final String id) {
        logger.info("Sending trigger event to resume sideline {}", id);
        updateTriggerEventType(id, SidelineType.RESUME);
    }

    /**
     * Update an existing {@link TriggerEvent} to resolve a sideline.
     * @param id the identifier of the sideline.
     */
    public void resolveTriggerEvent(final String id) {
        logger.info("Sending trigger event to resolve sideline {}", id);
        updateTriggerEventType(id, SidelineType.RESOLVE);
    }

    /**
     * Closes the curator instance when the helper is done being used.
     */
    public void close() {
        curator.close();
    }

    private void updateTriggerEventType(final String id, SidelineType type) {
        final String path = getZkRoot() + "/" + id;

        final TriggerEvent originalTriggerEvent = curatorHelper.readJson(path, TriggerEvent.class);

        Preconditions.checkNotNull(originalTriggerEvent, "Could not find the original trigger event!");

        logger.debug("Loaded {} {}", path, originalTriggerEvent);

        final LocalDateTime updatedAt = LocalDateTime.now();

        final TriggerEvent triggerEvent = new TriggerEvent(
            type,
            originalTriggerEvent.getData(),
            originalTriggerEvent.getCreatedAt(),
            originalTriggerEvent.getCreatedBy(),
            originalTriggerEvent.getDescription(),
            false,
            updatedAt
        );

        curatorHelper.writeJson(path, triggerEvent);

        logger.info("Saved to {}", path);
    }

    private String getMd5Hash(final Map<String, Object> data) {
        return Tools.makeMd5Hash(new JSON(new HashMap<>()).to(data));
    }

    private String getZkRoot() {
        @SuppressWarnings("unchecked")
        final String zkRoot = (String) config.get(Config.ZK_ROOT);
        return zkRoot;
    }
}
