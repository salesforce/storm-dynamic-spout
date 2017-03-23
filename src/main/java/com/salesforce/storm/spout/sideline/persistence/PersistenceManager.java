package com.salesforce.storm.spout.sideline.persistence;

import com.salesforce.storm.spout.sideline.kafka.ConsumerState;
import com.salesforce.storm.spout.sideline.trigger.SidelineIdentifier;
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
     * @param consumerId - The consumer's id.
     * @param consumerState - ConsumerState to be persisted.
     */
    void persistConsumerState(final String consumerId, final ConsumerState consumerState);

    /**
     * Retrieves the consumer state from the persistence layer.
     * @return ConsumerState
     */
    ConsumerState retrieveConsumerState(final String consumerId);

    /**
     * Removes consumer state from the persistence layer.
     * @param consumerId - the consumer's id you'd like cleared.
     */
    void clearConsumerState(String consumerId);

    /**
     * @param type
     * @param id - unique identifier for the sideline request.
     * @param endingState
     */
    void persistSidelineRequestState(SidelineType type, final SidelineIdentifier id, final SidelineRequest request, final ConsumerState startingState, ConsumerState endingState);

    /**
     * Retrieves a sideline request state for the given SidelineIdentifier.
     * @param id - SidelineIdentifier you want to retrieve the state for.
     * @return The ConsumerState that was persisted via persistSidelineRequestState().
     */
    SidelinePayload retrieveSidelineRequest(final SidelineIdentifier id);

    /**
     * Lists existing sideline requests.
     * @return A list of identifiers for existing sideline requests
     */
    List<SidelineIdentifier> listSidelineRequests();
}
