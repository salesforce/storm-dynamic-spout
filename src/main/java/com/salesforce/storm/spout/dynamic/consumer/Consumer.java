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

package com.salesforce.storm.spout.dynamic.consumer;

import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.persistence.PersistenceAdapter;

import java.util.Map;

/**
 * This defines the interface for Consumers.
 * Implementing the following methods will allow you to build a drop in Consumer that processes data
 * within a VirtualSpout instance.
 */
public interface Consumer {

    /**
     * This method is called once, after your implementation has been constructed.
     * This method should handle all setup and configuration.
     * @param spoutConfig Configuration of Spout.
     * @param virtualSpoutIdentifier VirtualSpout running this consumer.
     * @param consumerPeerContext defines how many instances in total are running of this consumer.
     * @param persistenceAdapter The persistence adapter used to manage any state.
     * @param startingState (Optional) If not null, This defines the state at which the consumer should resume from.
     */
    void open(
        final Map<String, Object> spoutConfig,
        final VirtualSpoutIdentifier virtualSpoutIdentifier,
        final ConsumerPeerContext consumerPeerContext,
        final PersistenceAdapter persistenceAdapter,
        final ConsumerState startingState
    );

    /**
     * This method is called when a VirtualSpout is shutting down.  It should perform any necessary cleanup.
     */
    void close();

    /**
     * @return The next Record that should be processed.
     */
    Record nextRecord();

    /**
     * Called when a specific Record has completed processing successfully.
     * @param namespace Namespace the record originated from.
     * @param partition Partition the record originated from.
     * @param offset Offset the record originated from.
     */
    void commitOffset(final String namespace, final int partition, final long offset);

    // State related methods

    /**
     * @return The Consumer's current state.
     */
    ConsumerState getCurrentState();

    /**
     * Requests the consumer to persist state to the Persistence adapter.
     * @return The Consumer's current state.
     */
    ConsumerState flushConsumerState();

    /**
     * This is likely to change in signature in the future to return some standardized object instead of a double.
     * @return The consumer's maximum lag.
     */
    double getMaxLag();

    // The following methods are likely to be removed in future refactorings.
    void removeConsumerState();

    PersistenceAdapter getPersistenceAdapter();

    // Maybe the logic in VSpout that needs this can be refactored within KafkaConsumer?
    boolean unsubscribeConsumerPartition(final ConsumerPartition consumerPartitionToUnsubscribe);
}
