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
import com.salesforce.kafka.test.junit.SharedZookeeperTestResource;
import com.salesforce.storm.spout.dynamic.Tools;
import org.apache.curator.framework.CuratorFramework;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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