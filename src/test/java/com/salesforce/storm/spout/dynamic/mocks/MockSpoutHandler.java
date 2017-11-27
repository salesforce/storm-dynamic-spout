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

package com.salesforce.storm.spout.dynamic.mocks;

import com.salesforce.storm.spout.dynamic.DelegateSpoutFactory;
import com.salesforce.storm.spout.dynamic.DynamicSpout;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.handler.SpoutHandler;
import org.apache.storm.task.TopologyContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A stand-in implementation of SpoutHandler used in tests.
 */
public class MockSpoutHandler implements SpoutHandler {
    private SpoutConfig spoutConfig = null;

    private boolean hasCalledOpen = false;
    private boolean hasCalledClosed = false;

    private List<OpenedSpoutParams> openedSpouts = new ArrayList<>();
    private List<DynamicSpout> activatedSpouts = new ArrayList<>();
    private List<DynamicSpout> closedSpouts = new ArrayList<>();
    private List<DynamicSpout> deactivatedSpouts = new ArrayList<>();

    /**
     * Open the handler.
     * @param spoutConfig Spout configuration.
     */
    @Override
    public void open(SpoutConfig spoutConfig, DelegateSpoutFactory delegateSpoutFactory) {
        hasCalledOpen = true;
        this.spoutConfig = spoutConfig;
    }

    /**
     * Close the handler.
     */
    @Override
    public void close() {
        hasCalledClosed = true;
    }

    /**
     * Called when the DynamicSpout is opened.
     * @param spout DynamicSpout instance.
     * @param topologyConfig Topology configuration.
     * @param topologyContext Topology context.
     */
    @Override
    public void onSpoutOpen(DynamicSpout spout, Map topologyConfig, TopologyContext topologyContext) {
        openedSpouts.add(
            new OpenedSpoutParams(spout, topologyConfig, topologyContext)
        );
    }

    /**
     * Called when the DynamicSpout is activated.
     * @param spout DynamicSpout instance.
     */
    @Override
    public void onSpoutActivate(final DynamicSpout spout) {
        activatedSpouts.add(spout);
    }

    /**
     * Called when the DynamicSpout is deactivated.
     * @param spout DynamicSpout instance.
     */
    @Override
    public void onSpoutDeactivate(final DynamicSpout spout) {
        deactivatedSpouts.add(spout);
    }

    /**
     * Called when the DynamicSpout is closed.
     * @param spout DynamicSpout instance.
     */
    @Override
    public void onSpoutClose(final DynamicSpout spout) {
        closedSpouts.add(spout);
    }

    public SpoutConfig getSpoutConfig() {
        return spoutConfig;
    }

    public boolean isHasCalledOpen() {
        return hasCalledOpen;
    }

    public boolean isHasCalledClosed() {
        return hasCalledClosed;
    }

    public List<OpenedSpoutParams> getOpenedSpouts() {
        return openedSpouts;
    }

    public List<DynamicSpout> getActivatedSpouts() {
        return activatedSpouts;
    }

    public List<DynamicSpout> getClosedSpouts() {
        return closedSpouts;
    }

    public List<DynamicSpout> getDeactivatedSpouts() {
        return deactivatedSpouts;
    }

    /**
     * For collecting parameters passed to open() method.
     */
    public static class OpenedSpoutParams {
        private final DynamicSpout spout;
        private final Map config;
        private final TopologyContext topologyContext;

        private OpenedSpoutParams(final DynamicSpout spout, final Map config, final TopologyContext topologyContext) {
            this.spout = spout;
            this.config = config;
            this.topologyContext = topologyContext;
        }

        public DynamicSpout getSpout() {
            return spout;
        }

        public Map getConfig() {
            return config;
        }

        public TopologyContext getTopologyContext() {
            return topologyContext;
        }
    }
}
