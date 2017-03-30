package com.salesforce.storm.spout.sideline.persistence;

import com.salesforce.storm.spout.sideline.kafka.ConsumerState;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;

import java.util.List;
import java.util.Map;

/**
 * Interface that controls persistence of state.
 */
public interface PersistenceManager {
    /**
     * Performs any required initialization/connection/setup required for
     * the implementation.  By contract, this will be called once prior to calling
     * persistState() or getState().
     * @param topologyConfig - The storm topology config map.
     */
    void open(Map topologyConfig);

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
     * @param type - Sideline Type (Start/Stop)
     * @param id - unique identifier for the sideline request.
     * @param endingState - The consumer state we will stop at.
     */
    void persistSidelineRequestState(SidelineType type, final SidelineRequestIdentifier id, final SidelineRequest request, final ConsumerState startingState, ConsumerState endingState);

    /**
     * Retrieves a sideline request state for the given SidelineRequestIdentifier.
     * @param id - SidelineRequestIdentifier you want to retrieve the state for.
     * @return The ConsumerState that was persisted via persistSidelineRequestState().
     */
    SidelinePayload retrieveSidelineRequest(final SidelineRequestIdentifier id);

    /**
     * Removes a sideline request from the persistence layer.
     * @param id - SidelineRequestIdentifier you want to clear.
     */
    void clearSidelineRequest(SidelineRequestIdentifier id);

    /**
     * Lists existing sideline requests.
     * @return A list of identifiers for existing sideline requests
     */
    List<SidelineRequestIdentifier> listSidelineRequests();
}
