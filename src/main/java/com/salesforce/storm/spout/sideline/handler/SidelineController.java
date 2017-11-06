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

import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;

/**
 * A proxy to create a layer of indirection between the SpoutHandler and the Triggers. This allows us to refactor where
 * starting and stopping a sideline is handled from without breaking every single trigger implementation. It also prevents
 * other methods on the SpoutHandler from being accessible.
 */
public interface SidelineController {

    /**
     * Does a sideline exist in the started state?
     * @param sidelineRequest sideline request.
     * @return true it does, false it does not.
     */
    boolean isSidelineStarted(SidelineRequest sidelineRequest);

    /**
     * Start sidelining.
     * @param request Sideline request, container an id and a filter chain step.
     */
    void startSidelining(final SidelineRequest request);

    /**
     * Does a sideline exist in the stopped state?
     * @param sidelineRequest sideline request.
     * @return true it has, false it has not.
     */
    boolean isSidelineStopped(SidelineRequest sidelineRequest);

    /**
     * Stop sidelining.
     * @param request Sideline request, container an id and a filter chain step.
     */
    void stopSidelining(final SidelineRequest request);
}
