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

package com.salesforce.storm.spout.sideline.handler;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import com.salesforce.storm.spout.sideline.SidelineVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.mocks.MockConsumer;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;
import com.salesforce.storm.spout.dynamic.filter.StaticMessageFilter;
import com.salesforce.storm.spout.dynamic.mocks.MockDelegateSpout;
import com.salesforce.storm.spout.sideline.persistence.InMemoryPersistenceAdapter;
import com.salesforce.storm.spout.sideline.persistence.SidelinePayload;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertNull;

/**
 * Test that the {@link SidelineVirtualSpoutHandler} completes sidelines correctly.
 */
public class SidelineVirtualSpoutHandlerTest {

    /**
     * Test that upon completion of a virtual spout, the sideline state is properly cleaned up.
     */
    @Test
    public void testOnVirtualSpoutCompletion() {
        final String prefix = "MyVirtualSpout";
        final String namespace = MockConsumer.topic;
        final SidelineRequestIdentifier sidelineRequestIdentifier = new SidelineRequestIdentifier("SidelineRequest");
        final SidelineRequest sidelineRequest = new SidelineRequest(sidelineRequestIdentifier, new StaticMessageFilter());

        final SidelineVirtualSpoutIdentifier sidelineVirtualSpoutIdentifier = new SidelineVirtualSpoutIdentifier(
            prefix,
            sidelineRequestIdentifier
        );

        final Map<String,Object> config = Maps.newHashMap();
        config.put(SidelineConfig.PERSISTENCE_ADAPTER_CLASS, InMemoryPersistenceAdapter.class.getName());
        final AbstractConfig spoutConfig = new AbstractConfig(new ConfigDefinition(), config);

        final MockDelegateSpout mockDelegateSpout = new MockDelegateSpout(sidelineVirtualSpoutIdentifier);

        MockConsumer.partitions = Arrays.asList(0, 5);

        final SidelineVirtualSpoutHandler sidelineVirtualSpoutHandler = new SidelineVirtualSpoutHandler();
        sidelineVirtualSpoutHandler.open(spoutConfig);

        // Persist some stopping requests that we cleanup upon completion
        sidelineVirtualSpoutHandler.getPersistenceAdapter().persistSidelineRequestState(
            SidelineType.STOP,
            sidelineRequestIdentifier,
            sidelineRequest,
            new ConsumerPartition(namespace, 0), // partition
            1L, // starting offset
            1L // ending offset
        );
        sidelineVirtualSpoutHandler.getPersistenceAdapter().persistSidelineRequestState(
            SidelineType.START,
            sidelineRequestIdentifier,
            sidelineRequest,
            new ConsumerPartition(namespace, 5), // partition
            3L, // starting offset
            2L // ending offset
        );

        // Complete the sideline
        sidelineVirtualSpoutHandler.onVirtualSpoutCompletion(mockDelegateSpout);

        // Do we still have a record for partition 0?
        SidelinePayload partition0 = sidelineVirtualSpoutHandler.getPersistenceAdapter()
            .retrieveSidelineRequest(sidelineRequestIdentifier, new ConsumerPartition(namespace, 0));

        assertNull(partition0);

        // Do we still have a record for partition 5?
        SidelinePayload partition5 = sidelineVirtualSpoutHandler.getPersistenceAdapter()
            .retrieveSidelineRequest(sidelineRequestIdentifier, new ConsumerPartition(namespace, 5));

        assertNull(partition5);

        // Call close
        sidelineVirtualSpoutHandler.close();
    }
}