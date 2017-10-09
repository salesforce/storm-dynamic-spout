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

package com.salesforce.storm.spout.dynamic.persistence;

import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Interface that controls persistence of state.
 */
// TODO: Split sideline storage out of this class into it's own persistence adapter
public interface PersistenceAdapter {
    /**
     * Performs any required initialization/connection/setup required for
     * the implementation.  By contract, this will be called once prior to calling
     * persistState() or getState().
     * @param spoutConfig - The storm topology config map.
     */
    void open(Map spoutConfig);

    /**
     * Performs any cleanup required for the implementation on shutdown.
     */
    void close();

    /**
     * Pass in the consumer state that you'd like persisted.
     * @param consumerId The consumer's id.
     * @param partitionId The partition id
     * @param offset Offset of the consumer on the given partition
     */
    void persistConsumerState(final String consumerId, final int partitionId, final long offset);

    /**
     * Retrieves the consumer state from the persistence layer.
     * @param consumerId The consumer's id.
     * @param partitionId The partition id
     * @return Offset of the consumer on the given partition
     */
    Long retrieveConsumerState(final String consumerId, final int partitionId);

    /**
     * Removes consumer state from the persistence layer.
     * @param consumerId The consumer's id you'd like cleared.
     * @param partitionId The partition id
     */
    void clearConsumerState(final String consumerId, final int partitionId);

    /**
     * @param type Sideline Type (Start/Stop)
     * @param id unique identifier for the sideline request.
     * @param partitionId Partition id
     * @param startingOffset Ending offset
     * @param endingOffset Starting offset
     */
    void persistSidelineRequestState(
        final SidelineType type,
        final SidelineRequestIdentifier id,
        final SidelineRequest request,
        final int partitionId,
        final Long startingOffset,
        final Long endingOffset
    );

    /**
     * Retrieves a sideline request state for the given SidelineRequestIdentifier.
     * @param id SidelineRequestIdentifier you want to retrieve the state for.
     * @param partitionId PartitionId to persist.
     * @return The ConsumerState that was persisted via persistSidelineRequestState().
     */
    SidelinePayload retrieveSidelineRequest(final SidelineRequestIdentifier id, int partitionId);

    /**
     * List the partitions for the given sideline request.
     * @param id Identifier for the sideline request that you want the partitions for
     * @return A list of the partitions for the sideline request
     */
    Set<Integer> listSidelineRequestPartitions(final SidelineRequestIdentifier id);

    /**
     * Removes a sideline request from the persistence layer.
     * @param id - SidelineRequestIdentifier you want to clear.
     * @param partitionId - Partition of the sideline request you want to clear
     */
    void clearSidelineRequest(SidelineRequestIdentifier id, int partitionId);

    /**
     * Lists existing sideline requests.
     * @return A list of identifiers for existing sideline requests
     */
    List<SidelineRequestIdentifier> listSidelineRequests();
}
