/*
 * Copyright (c) 2018, Salesforce.com, Inc.
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

package com.salesforce.storm.spout.dynamic.persistence.zookeeper;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper methods for common tasks when working with Curator.
 */
public class CuratorHelper {

    /**
     * Logger for logging logs.
     */
    private static final Logger logger = LoggerFactory.getLogger(CuratorHelper.class);

    /**
     * Curator instance.
     */
    private CuratorFramework curator;

    /**
     * JSON parser.
     */
    private final Gson gson;

    /**
     * Helper methods for common tasks when working with Curator.
     * @param curator curator instance.
     */
    public CuratorHelper(final CuratorFramework curator) {
        this(
            curator,
            new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create()
        );
    }

    /**
     * Helper methods for common tasks when working with Curator.
     * @param curator curator instance.
     * @param gson JSON parser instance.
     */
    public CuratorHelper(final CuratorFramework curator, Gson gson) {
        this.curator = curator;
        this.gson = gson;
    }

    /**
     * Internal method to write JSON structured data into a zookeeper node.
     * @param path node to write the JSON data into.
     * @param data map representation of JSON data to write.
     */
    public void writeJson(final String path, final Object data) {
        logger.debug("Zookeeper Writing {} the data {}", path, data.toString());
        writeBytes(path, gson.toJson(data).getBytes(Charsets.UTF_8));
    }

    /**
     * Read data from Zookeeper that has been stored as JSON.
     *
     * This method will return a HashMap from the JSON.  You should consider using {@link #readJson(String, Class)} instead as it
     * will deserialize the JSON to a concrete class.
     *
     * @param path node containing JSON to read from.
     * @param <K> key to the json field.
     * @param <V> value of the json field.
     * @return map representing the JSON stored within the zookeeper node.
     */
    public <K, V> Map<K, V> readJson(final String path) {
        return readJson(path, HashMap.class);
    }

    /**
     * Read data from Zookeeper that has been stored as JSON.
     *
     * This method will return the JSON deserialized to the provided class.
     *
     * @param path node containing JSON to read from.
     * @param clazz class of the object the JSON should be deserialized to.
     * @param <T> object type we are deserializing into.
     * @return map representing the JSON stored within the zookeeper node.
     */
    public <T> T readJson(final String path, final Class<T> clazz) {
        try {
            byte[] bytes = readBytes(path);
            if (bytes == null) {
                return null;
            }
            return gson.fromJson(new String(bytes, Charsets.UTF_8), clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Internal method to read a node out of zookeeper.
     * @param path node's path to read from zookeeper.
     * @return bytes representing that node.
     */
    public byte[] readBytes(final String path) {
        try {
            // Make sure our curator client has started.
            ensureCuratorHasStarted();

            if (curator.checkExists().forPath(path) != null) {
                return curator.getData().forPath(path);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Internal method used to write a byte array into a zookeeper node.
     * @param path path to write data into.
     * @param bytes data to write into the node.
     */
    public void writeBytes(final String path, final byte[] bytes) {
        try {
            // Make sure our curator client has started.
            ensureCuratorHasStarted();

            if (curator.checkExists().forPath(path) == null) {
                curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, bytes);
            } else {
                curator.setData().forPath(path, bytes);
            }
        } catch (KeeperException.NodeExistsException ex) {
            logger.warn("Tried creating node {} that already exists, cowardly refusing to overwrite it.", path);
        } catch (Exception ex) {
            logger.error("Unable to write bytes to Zookeeper {} {}", ex, ex.getStackTrace());
            throw new RuntimeException(ex);
        }
    }

    /**
     * Internal method to delete a node from Zookeeper.
     * @param path node to delete.
     */
    public void deleteNode(final String path) {
        try {
            // Make sure our curator client has started.
            ensureCuratorHasStarted();

            // If it doesn't exist,
            if (curator.checkExists().forPath(path) == null) {
                // Nothing to do!
                logger.warn("Tried to delete {}, but it does not exist.", path);
                return;
            }

            // Delete.
            curator.delete().forPath(path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Removes a path only if it has no children.
     * @param path - path to remove.
     */
    public void deleteNodeIfNoChildren(final String path) {
        try {
            // Make sure our curator client has started.
            ensureCuratorHasStarted();

            // If it doesn't exist,
            if (curator.checkExists().forPath(path) == null) {
                // Nothing to do!
                return;
            }

            // Delete.
            final List<String> children = curator.getChildren().forPath(path);
            if (children.isEmpty()) {
                logger.info("Removing empty path {}", path);
                curator.delete().forPath(path);
            }
        } catch (final KeeperException.NoNodeException noNodeException) {
            // We caught a no-node exception. That means the node we wanted to delete didn't exist.
            // Well, that's more or less the end result we wanted right?  This happens because of a
            // race conditions between checking if the node exists and actually removing it, some other client removed
            // the node for us. For more information see https://github.com/salesforce/storm-dynamic-spout/issues/92
            logger.info("Requested to remove zookeeper node {} but that node did not exist.", path);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Quick check to ensure that Curator has been started.
     */
    private void ensureCuratorHasStarted() {
        // If our client isn't started yet
        if (CuratorFrameworkState.STARTED != curator.getState()) {
            // Lets start it!
            logger.debug("Curator not started, starting it now!");
            curator.start();
        }
    }
}
