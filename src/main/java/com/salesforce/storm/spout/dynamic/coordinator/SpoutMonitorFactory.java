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

package com.salesforce.storm.spout.dynamic.coordinator;

import com.salesforce.storm.spout.dynamic.DelegateSpout;
import com.salesforce.storm.spout.dynamic.VirtualSpoutMessageBus;
import com.salesforce.storm.spout.dynamic.metrics.MetricsRecorder;

import java.time.Clock;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

/**
 * Used to create instances of SpoutMonitor.
 */
public class SpoutMonitorFactory {

    /**
     * Factory method.
     * @param newSpoutQueue Queue monitored for new Spouts that should be started.
     * @param virtualSpoutMessageBus Message bus for passing messages FROM VirtualSpouts TO DynamicSpout.
     * @param latch Latch to allow startup synchronization.
     * @param clock Which clock instance to use, allows injecting a mock clock.
     * @param topologyConfig Storm topology config.
     * @param metricsRecorder MetricRecorder implementation for recording metrics.
     * @return new SpoutMonitor instance.
     */
    public SpoutMonitor create(
        final Queue<DelegateSpout> newSpoutQueue,
        final VirtualSpoutMessageBus virtualSpoutMessageBus,
        final CountDownLatch latch,
        final Clock clock,
        final Map<String, Object> topologyConfig,
        final MetricsRecorder metricsRecorder) {

        // Create instance.
        return new SpoutMonitor(
            newSpoutQueue,
            virtualSpoutMessageBus,
            latch,
            clock,
            topologyConfig,
            metricsRecorder
        );
    }
}
