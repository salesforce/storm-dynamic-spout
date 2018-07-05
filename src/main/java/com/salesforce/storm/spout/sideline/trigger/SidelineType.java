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

package com.salesforce.storm.spout.sideline.trigger;

/**
 * Type of sideline request, either a start of stop operation.
 */
public enum SidelineType {

    /**
     * Start means that a filter is being applied to the firehose and the current offset is captured so it can be used later
     * during the resume phase.
     */
    START,
    /**
     * Resume means that a filter is on the firehose, and a new {@link com.salesforce.storm.spout.dynamic.VirtualSpout} is being
     * spun up with the negation of that filter. In other words, whatever that filter was filtering will now be processed
     * in it's own thread where it can be throttled independently of the firehose.
     */
    RESUME,
    /**
     * Resolve means that an ending offset is being applied to the {@link com.salesforce.storm.spout.dynamic.VirtualSpout} and it
     * cease to run whenever it reached that {@link com.salesforce.storm.spout.dynamic.consumer.ConsumerState}. The filter is also
     * removed from the firehose, so that future record's are processed in real time.
     */
    RESOLVE,
}
