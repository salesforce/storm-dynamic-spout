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

package com.salesforce.storm.spout.dynamic.persistence.zookeeper;

import com.google.common.base.Charsets;
import com.salesforce.kafka.test.junit4.SharedZookeeperTestResource;
import com.salesforce.storm.spout.dynamic.Tools;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Aims to provide integration testing over CuratorHelper and CuratorFactory against a 'live'
 * embedded Zookeeper server.
 */
public class CuratorHelperTest {

    /**
     * Start embedded ZK server.
     */
    @ClassRule
    public static SharedZookeeperTestResource sharedZookeeperTestResource = new SharedZookeeperTestResource();

    /**
     * Test reading and writing bytes.
     */
    @Test
    public void testReadAndWriteBytes() {
        final String path = "/test";
        final String valueStr = "My String";
        final byte[] valueBytes = valueStr.getBytes(Charsets.UTF_8);

        try (final CuratorFramework curator = createCurator()) {
            // Write
            final CuratorHelper curatorHelper = new CuratorHelper(curator);
            curatorHelper.writeBytes(path, valueBytes);

            // Read
            final byte[] resultBytes = curatorHelper.readBytes(path);
            final String resultStr = new String(resultBytes, Charsets.UTF_8);

            assertEquals("Has correct value", valueStr, resultStr);
        }
    }

    /**
     * Test reading and writing bytes.
     */
    @Test
    public void testReadAndWriteJson() {
        final String path = "/jsonTest";

        // Define input map
        final Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("key1", "value1");
        jsonMap.put("key2", 2L);
        jsonMap.put("key3", true);

        try (final CuratorFramework curator = createCurator()) {
            // Write
            final CuratorHelper curatorHelper = new CuratorHelper(curator);
            curatorHelper.writeJson(path, jsonMap);

            // Read
            final Map<String, Object> resultMap = curatorHelper.readJson(path);

            // Validate
            assertNotNull("Not null result", resultMap);
            assertEquals("Has 3 keys", 3, resultMap.size());
            assertEquals("Has key 1", "value1", resultMap.get("key1"));
            // Note, we put in a Long and got a Double so if we want Long we need to do some casting on maps
            assertEquals("Has key 2", 2.0, resultMap.get("key2"));
            assertEquals("Has key 3", true, resultMap.get("key3"));
        }
    }

    /**
     * Tests that if we attempt to delete a node with children, it won't delete it.
     */
    @Test
    public void testDeleteNodeIfNoChildren_hasChildrenShouldNotDelete() throws Exception {
        final String basePath = "/testDeleteNodeIfNoChildren_hasChildrenShouldNotDelete";
        final String childPath = basePath + "/childNode";

        try (final CuratorFramework curator = createCurator()) {
            // Create basePath
            curator
                .create()
                .creatingParentsIfNeeded()
                .forPath(basePath);

            // Create some child nodes
            curator
                .create()
                .forPath(childPath);

            // Now create our helper
            final CuratorHelper curatorHelper = new CuratorHelper(curator);

            // Call our method
            curatorHelper.deleteNodeIfNoChildren(basePath);

            // Validate child nodes still exist
            Stat result = curator
                .checkExists()
                .forPath(childPath);
            assertNotNull("Child path should exist", result);

            // Validate base exists
            result = curator
                .checkExists()
                .forPath(basePath);
            assertNotNull("base path should exist", result);
            assertEquals("Should have 1 child", 1, result.getNumChildren());

            // Cleanup
            curator
                .delete()
                .deletingChildrenIfNeeded()
                .forPath(basePath);

            // Validate is gone, sanity check.
            result = curator
                .checkExists()
                .forPath(basePath);
            assertNull("base path should be removed", result);
        }
    }

    /**
     * Tests that if we attempt to delete a node without any children, it will delete it.
     */
    @Test
    public void testDeleteNodeIfNoChildren_hasNoChildrenShouldDelete() throws Exception {
        final String basePath = "/testDeleteNodeIfNoChildren_hasNoChildrenShouldDelete";

        try (final CuratorFramework curator = createCurator()) {
            // Create basePath
            curator
                .create()
                .creatingParentsIfNeeded()
                .forPath(basePath);

            // Now create our helper
            final CuratorHelper curatorHelper = new CuratorHelper(curator);

            // Call our method
            curatorHelper.deleteNodeIfNoChildren(basePath);

            // Validate is gone
            final Stat result = curator
                .checkExists()
                .forPath(basePath);
            assertNull("base path should be removed", result);
        }
    }

    /**
     * Tests that if we attempt to delete a node that doesnt actually exist
     * just silently returns.
     *
     * To simulate a race condition we do this using mocks.
     */
    @Test
    public void testDeleteNodeIfNoChildren_withNodeThatDoesntExist() throws Exception {
        final String basePath = "/testDeleteNodeIfNoChildren_withNodeThatDoesntExist";

        final CuratorFramework mockCurator = mock(CuratorFramework.class);

        // Exists builder should return true saying our basePath exists.
        final ExistsBuilder mockExistsBuilder = mock(ExistsBuilder.class);
        when(mockExistsBuilder.forPath(eq(basePath))).thenReturn(new Stat());
        when(mockCurator.checkExists()).thenReturn(mockExistsBuilder);

        // When we look for children, make sure it returns an empty list.
        final GetChildrenBuilder mockGetChildrenBuilder = mock(GetChildrenBuilder.class);
        when(mockGetChildrenBuilder.forPath(eq(basePath))).thenReturn(new ArrayList<>());
        when(mockCurator.getChildren()).thenReturn(mockGetChildrenBuilder);

        // When we go to delete the actual node, we toss a no-node exception.
        // This effectively simulates a race condition between checking if the node exists (our mock above says yes)
        // and it being removed before we call delete on it.
        final DeleteBuilder mockDeleteBuilder = mock(DeleteBuilder.class);
        when(mockDeleteBuilder.forPath(eq(basePath))).thenThrow(new KeeperException.NoNodeException());
        when(mockCurator.delete()).thenReturn(mockDeleteBuilder);

        // Now create our helper
        final CuratorHelper curatorHelper = new CuratorHelper(mockCurator);

        // Call our method
        curatorHelper.deleteNodeIfNoChildren(basePath);
    }

    /**
     * Uses CuratorFactory to create a curator instance.
     */
    private CuratorFramework createCurator() {
        // Create list of Servers
        final String serverStr = sharedZookeeperTestResource.getZookeeperConnectString();
        final List<String> serverList = Arrays.asList(Tools.splitAndTrim(serverStr));

        // Create config map
        final Map<String, Object> config = new HashMap<>();
        config.put("servers", serverList);

        return CuratorFactory.createNewCuratorInstance(config, getClass().getSimpleName());
    }
}