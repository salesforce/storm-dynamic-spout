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

package com.salesforce.storm.spout.dynamic.persistence;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.salesforce.storm.spout.dynamic.Tools;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.persistence.zookeeper.CuratorFactory;
import com.salesforce.storm.spout.dynamic.persistence.zookeeper.CuratorHelper;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

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
    public void open(final AbstractConfig spoutConfig) {
        // Root node / prefix to write entries under.
        final String zkRoot = (String) spoutConfig.get(SpoutConfig.PERSISTENCE_ZK_ROOT);
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
            // Take out spout persistence config and strip the key from it for our factory.
            spoutConfig.stripKeyPrefix("spout.persistence.zookeeper."),
            getClass().getSimpleName()
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
     * Pass in the consumer state that you'd like persisted.
     * @param consumerId consumer id.
     * @param partitionId partition id
     * @param offset offset for the partition to be persisted
     */
    @Override
    public void persistConsumerState(final String consumerId, final int partitionId, final long offset) {
        // Validate we're in a state that can be used.
        verifyHasBeenOpened();

        // Persist!
        curatorHelper.writeBytes(
            getZkConsumerStatePathForPartition(consumerId, partitionId),
            String.valueOf(offset).getBytes(Charsets.UTF_8)
        );
    }

    /**
     * Retrieves the consumer state from the persistence layer.
     * @param consumerId consumer id.
     * @param partitionId partition id
     * @return offset that was persisted.
     */
    @Override
    public Long retrieveConsumerState(final String consumerId, final int partitionId) {
        // Validate we're in a state that can be used.
        verifyHasBeenOpened();

        // Read!
        final String path = getZkConsumerStatePathForPartition(consumerId, partitionId);

        final byte[] bytes = curatorHelper.readBytes(path);

        if (bytes == null) {
            return null;
        }
        return Long.valueOf(new String(bytes, Charsets.UTF_8));
    }

    /**
     * Removes consumer state.
     * @param consumerId consumer id.
     * @param partitionId partition id
     */
    @Override
    public void clearConsumerState(final String consumerId, final int partitionId) {
        // Validate we're in a state that can be used.
        verifyHasBeenOpened();

        // Delete!
        final String path = getZkConsumerStatePathForPartition(consumerId, partitionId);
        logger.info("Delete state from Zookeeper at {}", path);
        curatorHelper.deleteNode(path);

        // Attempt to delete the parent path.
        // This is a noop if the parent path is not empty.
        final String parentPath = path.substring(0, path.lastIndexOf('/'));
        curatorHelper.deleteNodeIfNoChildren(parentPath);
    }

    /**
     * @return full zookeeper path to where our consumer state is stored for the given partition.
     */
    String getZkConsumerStatePathForPartition(final String consumerId, final int partitionId) {
        return getZkRoot() + "/consumers/" + consumerId + "/" + String.valueOf(partitionId);
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
