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

package com.salesforce.storm.spout.dynamic.handler;

import com.salesforce.storm.spout.dynamic.DelegateSpout;
import com.salesforce.storm.spout.dynamic.DelegateSpoutFactory;
import com.salesforce.storm.spout.dynamic.DynamicSpout;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

/**
 * Handlers (or callbacks) used by the DynamicSpout during it's lifecycle. Integrations can hook into the DynamicSpout
 * by creating a SpoutHanlder implementation.
 */
public interface SpoutHandler {

    /**
     * Open the handler.
     * @param spoutConfig Spout configuration.
     * @param delegateSpoutFactory Factory for creating {@link DelegateSpout} instances.
     */
    default void open(AbstractConfig spoutConfig, DelegateSpoutFactory delegateSpoutFactory) {

    }

    /**
     * Close the handler.
     */
    default void close() {

    }

    /**
     * Called when the DynamicSpout is opened.
     * @param spout DynamicSpout instance.
     * @param topologyConfig Topology configuration.
     * @param topologyContext Topology context.
     */
    default void onSpoutOpen(DynamicSpout spout, Map topologyConfig, TopologyContext topologyContext) {

    }

    /**
     * Called when the DynamicSpout is activated.
     * @param spout DynamicSpout instance.
     */
    default void onSpoutActivate(DynamicSpout spout) {

    }

    /**
     * Called when the DynamicSpout is deactivated.
     * @param spout DynamicSpout instance.
     */
    default void onSpoutDeactivate(DynamicSpout spout) {

    }

    /**
     * Called when the DynamicSpout is closed.
     * @param spout DynamicSpout instance.
     */
    default void onSpoutClose(DynamicSpout spout) {

    }
}
