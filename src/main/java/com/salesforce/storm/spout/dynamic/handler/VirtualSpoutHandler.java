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
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;

/**
 * Handlers (or callbacks) used by the VirtualSpout during it's lifecycle. Integrations can hook into the VirtualSpout
 * by creating a VirtualSpoutHandler implementation.
 */
public interface VirtualSpoutHandler {

    /**
     * Open the handler.
     * @param spoutConfig Spout configuration.
     */
    default void open(final SpoutConfig spoutConfig) {

    }

    /**
     * Close the handler.
     */
    default void close() {

    }

    /**
     * Called when the VirtualSpout is opened.
     * @param virtualSpout VirtualSpout instance.
     */
    default void onVirtualSpoutOpen(DelegateSpout virtualSpout) {

    }

    /**
     * Called when the VirtualSpout is activated.
     * @param virtualSpout VirtualSpout instance.
     */
    default void onVirtualSpoutClose(DelegateSpout virtualSpout) {

    }

    /**
     * Called when the VirtualSpout is completed, this happens before the call to onVirtualSpoutClose(), but only when
     * the spout is closing and has reached it's endingOffset.
     * @param virtualSpout VirtualSpout instance.
     */
    default void onVirtualSpoutCompletion(DelegateSpout virtualSpout) {

    }
}
