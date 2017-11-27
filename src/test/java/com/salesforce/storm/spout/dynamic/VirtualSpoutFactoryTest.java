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

package com.salesforce.storm.spout.dynamic;

import com.salesforce.storm.spout.dynamic.config.DynamicSpoutConfig;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerState;
import com.salesforce.storm.spout.dynamic.metrics.LogRecorder;
import com.salesforce.storm.spout.dynamic.metrics.MetricsRecorder;
import com.salesforce.storm.spout.dynamic.mocks.MockTopologyContext;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Test that the factory correctly create {@link VirtualSpout instances}.
 */
public class VirtualSpoutFactoryTest {

    /**
     * Test that the factory correctly create {@link VirtualSpout instances}.
     */
    @Test
    public void testCreate() {
        final DynamicSpoutConfig spoutConfig = new DynamicSpoutConfig(new HashMap<>());
        final MockTopologyContext topologyContext = new MockTopologyContext();
        final FactoryManager factoryManager = new FactoryManager(spoutConfig);
        final MetricsRecorder metricsRecorder = new LogRecorder();

        final VirtualSpoutFactory virtualSpoutFactory = new VirtualSpoutFactory(
            spoutConfig,
            topologyContext,
            factoryManager,
            metricsRecorder
        );

        final VirtualSpoutIdentifier identifier = new DefaultVirtualSpoutIdentifier("test");

        final ConsumerState startingState = ConsumerState.builder()
            .withPartition(new ConsumerPartition("foo", 0), 42L)
            .build();

        final ConsumerState endingState = ConsumerState.builder()
            .withPartition(new ConsumerPartition("bar", 1), 72L)
            .build();

        final VirtualSpout virtualSpout = (VirtualSpout) virtualSpoutFactory.create(identifier, startingState, endingState);

        assertEquals(
            "Config doesn't match",
            spoutConfig,
            virtualSpout.getSpoutConfig()
        );

        assertEquals(
            "Topology context does not match",
            topologyContext,
            virtualSpout.getTopologyContext()
        );

        assertEquals(
            "Factory manager does not match",
            factoryManager,
            virtualSpout.getFactoryManager()
        );

        assertEquals(
            "Metrics recorder does not match",
            metricsRecorder,
            virtualSpout.getMetricsRecorder()
        );

        assertEquals(
            "Starting state does not match",
            startingState,
            virtualSpout.getStartingState()
        );

        assertEquals(
            "Ending state does not match",
            endingState,
            virtualSpout.getEndingState()
        );
    }
}