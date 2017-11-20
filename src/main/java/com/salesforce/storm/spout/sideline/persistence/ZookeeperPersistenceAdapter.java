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

package com.salesforce.storm.spout.sideline.persistence;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.dynamic.Tools;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.persistence.zookeeper.CuratorFactory;
import com.salesforce.storm.spout.dynamic.persistence.zookeeper.CuratorHelper;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;
import com.salesforce.storm.spout.dynamic.filter.FilterChainStep;
import com.salesforce.storm.spout.dynamic.filter.Serializer;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Persistence layer implemented using Zookeeper.
 * Why Zookeeper?  Because its easy, and you most likely have it around.
 */
public class ZookeeperPersistenceAdapter implements PersistenceAdapter {

    /**
     * Logger for logging logs.
     */
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperPersistenceAdapter.class);

    /**
     * Helper for common tasks with Curator.
     */
    private CuratorHelper curatorHelper;

    /**
     * Root in zookeeper to store things at.
     */
    private String zkRoot;

    /**
     * Curator instance for working with Zookeeper.
     */
    private CuratorFramework curator;

    /**
     * Loads in configuration and sets up zookeeper/curator connection.
     * @param spoutConfig spout configuration.
     */
    @Override
    public void open(final Map spoutConfig) {
        // Root node / prefix to write entries under.
        final String zkRoot = (String) spoutConfig.get(SidelineConfig.PERSISTENCE_ZK_ROOT);
        final String consumerId = (String) spoutConfig.get(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX);

        Preconditions.checkArgument(
            zkRoot != null && !zkRoot.isEmpty(),
            "Zookeeper root is required"
        );

        Preconditions.checkArgument(
            consumerId != null && !consumerId.isEmpty(),
            "Zookeeper consumer id is required"
        );

        // The root we'll use for this instance is our configured root + our consumer id
        this.zkRoot = zkRoot + "/" + consumerId;

        this.curator = CuratorFactory.createNewCuratorInstance(
            // Take out sideline persistence config and strip the key from it for our factory.
            Tools.stripKeyPrefix("sideline.persistence.zookeeper.", spoutConfig),
            "Sideline" + getClass().getSimpleName()
        );

        this.curatorHelper = new CuratorHelper(curator);
    }

    /**
     * Close up shop, shut down zookeeper/curator connection.
     */
    @Override
    public void close() {
        if (curator == null) {
            return;
        }
        curator.close();
        curatorHelper = null;
        curator = null;
    }

    /**
     * Persist a sideline request
     * @param type Sideline Type (Start/Stop)
     * @param id Unique identifier for the sideline request.
     * @param request Sideline Request
     * @param partitionId Partition id
     * @param startingOffset Ending offset
     * @param endingOffset Starting offset
     */
    @Override
    public void persistSidelineRequestState(
        SidelineType type,
        SidelineRequestIdentifier id,
        SidelineRequest request,
        int partitionId,
        Long startingOffset,
        Long endingOffset
    ) {
        // Validate we're in a state that can be used.
        verifyHasBeenOpened();

        Preconditions.checkNotNull(type, "SidelineType is required.");
        Preconditions.checkNotNull(id, "SidelineRequestIdentifier is required.");
        Preconditions.checkNotNull(request, "SidelineRequest is required.");

        Map<String, Object> data = Maps.newHashMap();
        data.put("type", type.toString());
        data.put("startingOffset", startingOffset);
        if (endingOffset != null) { // Optional
            data.put("endingOffset", endingOffset);
        }
        data.put("filterChainStep", Serializer.serialize(request.step));

        // Persist!
        curatorHelper.writeJson(getZkRequestStatePathForPartition(id.toString(), partitionId), data);
    }

    @Override
    public SidelinePayload retrieveSidelineRequest(SidelineRequestIdentifier id, int partitionId) {
        // Validate we're in a state that can be used.
        verifyHasBeenOpened();

        Preconditions.checkNotNull(id, "SidelineRequestIdentifier is required.");

        // Read!
        final String path = getZkRequestStatePathForPartition(id.toString(), partitionId);
        // TODO: We should make a real object for this and update readJson() to support a class declaration
        Map<Object, Object> json = curatorHelper.readJson(path);
        logger.debug("Read request state from Zookeeper at {}: {}", path, json);

        if (json == null) {
            return null;
        }

        final String typeString = (String) json.get("type");

        final SidelineType type = typeString.equals(SidelineType.STOP.toString()) ? SidelineType.STOP : SidelineType.START;

        final FilterChainStep step = parseJsonToFilterChainSteps(json);

        final Double startingOffset = (Double) json.get("startingOffset");
        final Double endingOffset = (Double) json.get("endingOffset");

        return new SidelinePayload(
            type,
            id,
            new SidelineRequest(id, step),
            startingOffset != null ? startingOffset.longValue() : null,
            endingOffset != null ? endingOffset.longValue() : null
        );
    }

    /**
     * Removes a sideline request from the persistence layer.
     * @param id SidelineRequestIdentifier you want to clear.
     * @param partitionId partition id to clear for.
     */
    @Override
    public void clearSidelineRequest(SidelineRequestIdentifier id, int partitionId) {
        // Validate we're in a state that can be used.
        verifyHasBeenOpened();

        Preconditions.checkNotNull(id, "SidelineRequestIdentifier is required.");

        // Delete!
        final String path = getZkRequestStatePathForPartition(id.toString(), partitionId);
        logger.info("Delete request from Zookeeper at {}", path);
        curatorHelper.deleteNode(path);

        // Attempt to delete the parent path.
        // This is a noop if the parent path is not empty.
        final String parentPath = path.substring(0, path.lastIndexOf('/'));
        curatorHelper.deleteNodeIfNoChildren(parentPath);
    }

    /**
     * Lists out a unique list of current sideline requests.
     * @return list of sideline request identifier objects
     */
    public List<SidelineRequestIdentifier> listSidelineRequests() {
        verifyHasBeenOpened();

        final List<SidelineRequestIdentifier> ids = Lists.newArrayList();

        try {
            final String path = getZkRequestStateRoot();

            if (curator.checkExists().forPath(path) == null) {
                return ids;
            }

            final List<String> requests = curator.getChildren().forPath(path);

            for (String request : requests) {
                ids.add(new SidelineRequestIdentifier(request));
            }

            logger.debug("Existing sideline request identifiers = {}", ids);
        } catch (Exception ex) {
            logger.error("{}", ex);
        }

        return ids;
    }

    /**
     * List the partitions for the given sideline request.
     * @param id identifier for the sideline request that you want the partitions for
     * @return a list of the partitions for the sideline request
     */
    @Override
    public Set<Integer> listSidelineRequestPartitions(final SidelineRequestIdentifier id) {
        verifyHasBeenOpened();

        Preconditions.checkNotNull(id, "SidelineRequestIdentifier is required.");

        final Set<Integer> partitions = Sets.newHashSet();

        try {
            final String path = getZkRequestStatePath(id.toString());

            if (curator.checkExists().forPath(path) == null) {
                return partitions;
            }

            final List<String> partitionNodes = curator.getChildren().forPath(path);

            for (String partition : partitionNodes) {
                partitions.add(Integer.valueOf(partition));
            }

            logger.debug("Partitions for sideline request {} = {}", id, partitions);
        } catch (Exception ex) {
            logger.error("{}", ex);
        }

        return Collections.unmodifiableSet(partitions);
    }

    private FilterChainStep parseJsonToFilterChainSteps(final Map<Object, Object> json) {
        if (json == null) {
            return null;
        }

        final String chainStepData = (String) json.get("filterChainStep");

        return Serializer.deserialize(chainStepData);
    }

    /**
     * @return full zookeeper path for our sideline request.
     */
    String getZkRequestStatePath(final String sidelineIdentifierStr) {
        return getZkRoot() + "/requests/" + sidelineIdentifierStr;
    }

    /**
     * @return full zookeeper path for our sideline request for a specific partition.
     */
    String getZkRequestStatePathForPartition(final String sidelineIdentifierStr, final int partitionId) {
        return getZkRequestStatePath(sidelineIdentifierStr) + "/" + partitionId;
    }

    /**
     * @return full zookeeper root to where our request state is stored.
     */
    String getZkRequestStateRoot() {
        return getZkRoot() + "/requests";
    }

    /**
     * @return configured zookeeper root path.
     */
    String getZkRoot() {
        return zkRoot;
    }

    /**
     * Makes sure we don't try to interact w/ this instance unless its been properly opened.
     */
    private void verifyHasBeenOpened() {
        if (curator == null) {
            throw new IllegalStateException("Instance has not been initialized via open() call yet!");
        }
    }
}
