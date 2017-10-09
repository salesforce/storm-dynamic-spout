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

import com.salesforce.storm.spout.dynamic.handler.SidelineSpoutHandler;
import com.salesforce.storm.spout.dynamic.trigger.SidelineRequest;
import com.salesforce.storm.spout.dynamic.trigger.SidelineRequestIdentifier;

/**
 * A proxy to create a layer of indirection between the SpoutHandler and the Triggers. This allows us to refactor where
 * starting and stopping a sideline is handled from without breaking every single trigger implementation.
 */
public class SpoutTriggerProxy {

    /**
     * DynamicSpout SpoutHandler for sidelining.
     */
    private final SidelineSpoutHandler spoutHandler;

    /**
     * Create a new proxy.
     * @param spoutHandler Sidelining SpoutHandler instance.
     */
    public SpoutTriggerProxy(final SidelineSpoutHandler spoutHandler) {
        this.spoutHandler = spoutHandler;
    }

    /**
     * Start sidelining.
     * @param request Sideline request, container an id and a filter chain step.
     * @return Identifier of the sideline request. You probably shouldn't count on this, it might go away.
     */
    public SidelineRequestIdentifier startSidelining(final SidelineRequest request) {
        return this.spoutHandler.startSidelining(request);
    }

    /**
     * Stop sidelining.
     * @param request Sideline request, container an id and a filter chain step.
     */
    public void stopSidelining(final SidelineRequest request) {
        this.spoutHandler.stopSidelining(request);
    }
}
