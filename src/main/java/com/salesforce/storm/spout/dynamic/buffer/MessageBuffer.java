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

package com.salesforce.storm.spout.dynamic.buffer;

import com.salesforce.storm.spout.dynamic.Message;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;

/**
 * This interface defines an abstraction around essentially a concurrent queue.
 * Abstracting this instead of using directly a queue object allows us to do things like
 * implement a "fairness" algorithm on the poll() method for pulling off of the queue.
 * Using a straight ConcurrentQueue would give us FIFO semantics (see {@link FifoBuffer})
 * but with an abstraction we could implement round robin (see {@link RoundRobinBuffer}) across
 * VirtualSpouts or any scheduling algorithm that we'd like.
 */
public interface MessageBuffer {

    /**
     * Called prior to utilizing the instance.
     * @param spoutConfig - a copy of the storm topology config.
     */
    void open(final SpoutConfig spoutConfig);

    /**
     * Let the Implementation know that we're adding a new VirtualSpoutId.
     * @param virtualSpoutId - Identifier of new Virtual Spout.
     */
    void addVirtualSpoutId(final VirtualSpoutIdentifier virtualSpoutId);

    /**
     * Let the Implementation know that we're removing/cleaning up from closing a VirtualSpout.
     * @param virtualSpoutId - Identifier of Virtual Spout to be cleaned up.
     */
    void removeVirtualSpoutId(final VirtualSpoutIdentifier virtualSpoutId);

    /**
     * Put a new message onto the queue.  This method is blocking if the queue buffer is full.
     * @param message - Message to be added to the queue.
     * @throws InterruptedException - thrown if a thread is interrupted while blocked adding to the queue.
     */
    void put(final Message message) throws InterruptedException;

    /**
     * @return - return the size of the buffer.
     */
    int size();

    /**
     * @return - returns the next Message to be processed out of the queue.
     */
    Message poll();
}
