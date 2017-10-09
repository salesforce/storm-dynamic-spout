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
package com.salesforce.storm.spout.dynamic.trigger;

import com.salesforce.storm.spout.dynamic.SpoutTriggerProxy;

import java.util.Map;

/**
 * Static trigger, you should only ever use this for testing.
 */
public class StaticTrigger implements SidelineTrigger {

    /**
     * Sideline proxy instance that created the trigger.
     */
    private static SpoutTriggerProxy sidelineSpout;

    /**
     * Current sideline request, which is captured after a sideline request identifier is started.
     */
    private static SidelineRequestIdentifier currentSidelineRequestIdentifier;

    /**
     * Open the trigger.
     * @param config spout configuration.
     */
    @Override
    public void open(final Map config) {
    }

    /**
     * Close the trigger.
     */
    @Override
    public void close() {
    }

    /**
     * Set the sideline proxy instance.
     * @param sidelineSpoutProxy sideline proxy instance.
     */
    @Override
    public void setSidelineSpout(final SpoutTriggerProxy sidelineSpoutProxy) {
        sidelineSpout = sidelineSpoutProxy;
    }

    /**
     * Start a sideline request.
     * @param request sideline request.
     */
    public static void sendStartRequest(final SidelineRequest request) {
        currentSidelineRequestIdentifier = sidelineSpout.startSidelining(request);
    }

    /**
     * Stop a sideline request.
     * @param request sideline request.
     */
    public static void sendStopRequest(final SidelineRequest request) {
        sidelineSpout.stopSidelining(request);
    }

    /**
     * Get current the sideline request identifier.
     * @return current sideline request identifier.
     */
    public static SidelineRequestIdentifier getCurrentSidelineRequestIdentifier() {
        return currentSidelineRequestIdentifier;
    }
}
