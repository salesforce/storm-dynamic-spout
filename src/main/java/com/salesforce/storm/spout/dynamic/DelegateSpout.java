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

import com.salesforce.storm.spout.dynamic.consumer.ConsumerState;
import com.salesforce.storm.spout.dynamic.consumer.Consumer;

/**
 * A virtualized spout that is run from within the DyanmicSpout by the SpoutCoordinator.
 */
public interface DelegateSpout {

    /**
     * Open the spout.
     */
    void open();

    /**
     * Close the spout.
     */
    void close();

    /**
     * Get the next message from the spout.
     * @return Message to pass along to Storm.
     */
    Message nextTuple();

    /**
     * Acknowledge a message that came from this spout as having completed processing.
     * @param msgId Message id to acknowledge.
     */
    void ack(Object msgId);

    /**
     * Fail a message that from this spout.
     * @param msgId Message id to fail.
     */
    void fail(Object msgId);

    /**
     * Get this spout's unique identifier.
     * @return A unique VirtualSpoutIdentifier instance.
     */
    VirtualSpoutIdentifier getVirtualSpoutId();

    /**
     * Flush the state of the current spout, usually this means persisting the spout's Consumer 'state' to the
     * PersistenceAdapter.
     */
    void flushState();

    /**
     * Request that the current spout stop running for async shutdown.
     */
    void requestStop();

    /**
     * Has the current spout been told to stop?
     * @return True if it has, false if it has not.
     */
    boolean isStopRequested();

    /**
     * Get the current ConsumerState from this spout's Consumer instance.
     * @return Current ConsumerState.
     */
    ConsumerState getCurrentState();

    /**
     * Get the starting ConsumerState that this spout was spun up with.
     * @return Starting ConsumerState.
     */
    ConsumerState getStartingState();

    /**
     * Get the ending ConsumerState that this spout was spun up with.
     * @return Ending ConsumerState.
     */
    ConsumerState getEndingState();

    /**
     * Used by SpoutPartitionProgressMonitor to find the max lag of any partitions in the consumer for the current
     * virtual spout.
     * TODO This should be revisited, this feels out of place in the interface.
     * @return Max lag.
     */
    double getMaxLag();

    /**
     * Get the number of filters applied to spout's filter chain. Used for metrics in the spout monitor.
     * TODO Should we drop this metric? This feels out of place in the interface.
     * @return Number of filters applied.
     */
    int getNumberOfFiltersApplied();

    /**
     * Get the Consumer this spout is using to pull messages from.
     * @return Consumer instance.
     */
    Consumer getConsumer();
}