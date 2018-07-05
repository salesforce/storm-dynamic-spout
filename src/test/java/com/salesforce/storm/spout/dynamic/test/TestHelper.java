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

package com.salesforce.storm.spout.dynamic.test;

import com.salesforce.storm.spout.dynamic.FactoryManager;
import com.salesforce.storm.spout.dynamic.VirtualSpout;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerPeerContext;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerState;
import com.salesforce.storm.spout.dynamic.metrics.LogRecorder;

import java.util.Map;

// TODO: Use the VirtualSpoutFactory in these methods

/**
 * Helper for creating things in tests.
 */
public class TestHelper {

    /**
     * Create a {@link VirtualSpout} and use sane defaults for most things.
     * @param config spout configuration.
     * @param identifier identifier of the {@link VirtualSpout}
     * @return a {@link VirtualSpout} instance.
     */
    public static VirtualSpout createVirtualSpout(
        final Map config,
        final VirtualSpoutIdentifier identifier
    ) {
        return createVirtualSpout(config, identifier, null, null);
    }

    /**
     * Create a {@link VirtualSpout} and use sane defaults for most things.
     * @param config spout configuration.
     * @param identifier identifier of the {@link VirtualSpout}
     * @param startingState state for the consumer to start at.
     * @param endingState state for the consumer to end at.
     * @return a {@link VirtualSpout} instance.
     */
    public static VirtualSpout createVirtualSpout(
        final Map config,
        final VirtualSpoutIdentifier identifier,
        final ConsumerState startingState,
        final ConsumerState endingState
    ) {
        final FactoryManager factoryManager = new FactoryManager(config);

        return new VirtualSpout(
            identifier,
            config,
            new ConsumerPeerContext(1, 0),
            factoryManager,
            new LogRecorder(),
            startingState,
            endingState
        );
    }
}
