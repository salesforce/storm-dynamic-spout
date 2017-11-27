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

import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.consumer.ConsumerState;
import com.salesforce.storm.spout.dynamic.metrics.MetricsRecorder;
import org.apache.storm.task.TopologyContext;

/**
 * Factory for easily creating {@link DelegateSpout} instances.
 *
 * Handy in things like {@link com.salesforce.storm.spout.dynamic.handler.SpoutHandler} where we want to abstract away particulars, such
 * as the things passed down from {@link DynamicSpout} such as the {@link MetricsRecorder} as an example.
 */
public class VirtualSpoutFactory implements DelegateSpoutFactory {

    /**
     * Spout configuration.
     */
    private final SpoutConfig spoutConfig;
    /**
     * Topology context.
     */
    private final TopologyContext topologyContext;
    /**
     * Factory manager, for creating instances of classes driven by configuration.
     */
    private final FactoryManager factoryManager;
    /**
     * Metrics recorder, for capturing metrics.
     */
    private final MetricsRecorder metricsRecorder;

    /**
     * Factory for easily creating {@link DelegateSpout} instances.
     *
     * Handy in things like {@link com.salesforce.storm.spout.dynamic.handler.SpoutHandler} where we want to abstract away particulars, such
     * as the things passed down from {@link DynamicSpout} such as the {@link MetricsRecorder} as an example.
     * @param spoutConfig Our topology config
     * @param topologyContext Our topology context
     * @param factoryManager FactoryManager instance.
     * @param metricsRecorder MetricsRecorder instance.
     */
    public VirtualSpoutFactory(
        final SpoutConfig spoutConfig,
        final TopologyContext topologyContext,
        final FactoryManager factoryManager,
        final MetricsRecorder metricsRecorder
    ) {
        this.spoutConfig = spoutConfig;
        this.topologyContext = topologyContext;
        this.factoryManager = factoryManager;
        this.metricsRecorder = metricsRecorder;
    }

    /**
     * Create a {@link DelegateSpout} instance.
     * @param identifier identifier to use for this instance.
     * @param startingState starting consumer state for this instance.
     * @param endingState ending consumer state for this instance.
     * @return instance of {@link DelegateSpout}.
     */
    @Override
    public DelegateSpout create(
        final VirtualSpoutIdentifier identifier,
        final ConsumerState startingState,
        final ConsumerState endingState
    ) {
        return new VirtualSpout(
            identifier,
            this.spoutConfig,
            this.topologyContext,
            this.factoryManager,
            this.metricsRecorder,
            startingState,
            endingState
        );
    }

    /**
     * Create a {@link DelegateSpout} instance.
     * @param identifier identifier to use for this instance.
     * @param startingState starting consumer state for this instance.
     * @return instance of {@link DelegateSpout}.
     */
    @Override
    public DelegateSpout create(
        final VirtualSpoutIdentifier identifier,
        final ConsumerState startingState
    ) {
        return create(identifier, startingState, null);
    }

    /**
     * Create a {@link DelegateSpout} instance.
     * @param identifier identifier to use for this instance.
     * @return instance of {@link DelegateSpout}.
     */
    @Override
    public DelegateSpout create(
        final VirtualSpoutIdentifier identifier
    ) {
        return create(identifier, null, null);
    }
}
