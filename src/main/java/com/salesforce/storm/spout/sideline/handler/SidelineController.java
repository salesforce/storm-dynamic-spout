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

package com.salesforce.storm.spout.sideline.handler;

import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;

/**
 * A proxy to create a layer of indirection between the SpoutHandler and the Triggers. This allows us to refactor where
 * starting and stopping a sideline is handled from without breaking every single trigger implementation. It also prevents
 * other methods on the SpoutHandler from being accessible.
 */
public interface SidelineController {

    /**
     * Determines if a sideline is in the start state.
     *
     * @param sidelineRequest sideline request.
     * @return true it is in this state, false if it is not.
     */
    boolean isStarted(SidelineRequest sidelineRequest);

    /**
     * Start sidelining.
     * 
     * Starting a sideline applies a {@link com.salesforce.storm.spout.dynamic.filter.FilterChainStep} to the firehose
     * such that messages are filtered. When this happens the current offset of the firehose is captured and persisted,
     * as it will be used later when the sideline goes to {@link #resume(SidelineRequest)}.
     *
     * @param sidelineRequest sideline request.
     */
    void start(final SidelineRequest sidelineRequest);

    /**
     * Determines if a sideline is in the resume state.
     *
     * @param sidelineRequest sideline request.
     * @return true it is in this state, false if it is not.
     */
    boolean isResumed(SidelineRequest sidelineRequest);

    /**
     * Resume sidelining.
     *
     * Resuming a sideline create a new {@link com.salesforce.storm.spout.dynamic.VirtualSpout} instance that will
     * using the offset captured in {@link #start(SidelineRequest)} as its starting point.  While a sideline is in
     * this state the {@link com.salesforce.storm.spout.dynamic.filter.FilterChainStep} will remain on the firehose.
     *
     * @param sidelineRequest Sideline request, container an id and a filter chain step.
     */
    void resume(final SidelineRequest sidelineRequest);

    /**
     * Determines if a sideline is in the resolve state.
     *
     * @param sidelineRequest sideline request.
     * @return true it is in this state, false if it is not.
     */
    boolean isResolving(SidelineRequest sidelineRequest);

    /**
     * Resolve a sideline.
     *
     * The {@link com.salesforce.storm.spout.dynamic.VirtualSpout} processing the historical data from the sideline window is
     * is given an ending offset, and the {@link com.salesforce.storm.spout.dynamic.filter.FilterChainStep} is removed
     * from the firehose {@link com.salesforce.storm.spout.dynamic.VirtualSpout}.
     *
     * The verbiage here is that we are 'resolving' the sideline, meaning that we are moving back to processing on the firehose.
     *
     * @param sidelineRequest Sideline request, container an id and a filter chain step.
     */
    void resolve(final SidelineRequest sidelineRequest);
}
