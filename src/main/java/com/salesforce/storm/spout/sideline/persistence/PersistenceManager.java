package com.salesforce.storm.spout.sideline.persistence;

import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.trigger.SidelineIdentifier;

import java.util.Map;

/**
 * Interface that controls all persistence.
 */
public interface PersistenceManager {
    /**
     * Performs any required initialization/connection/setup required for
     * the implementation.  By contract, this will be called once prior to calling
     * persistState() or getState().
     */
    public void open(Map topologyConfig);

    /**
     * Performs any cleanup required for the implementation on shutdown.
     */
    public void close();

    /**
     * Pass in the consumer state that you'd like persisted.
     * @param consumerState - ConsumerState to be persisted.
     */
    public void persistConsumerState(final String consumerId, final ConsumerState consumerState);

    /**
     * Retrieves the consumer state from the persistence layer.
     * @return ConsumerState
     */
    public ConsumerState retrieveConsumerState(final String consumerId);

    /**
     * @param id - unique identifier for the sideline request.
     * @param state - the associated state to be stored w/ the request.
     */
    void persistSidelineRequestState(final SidelineIdentifier id, final ConsumerState state);

    /**
     * Retrieves a sideline request state for the given SidelineIdentifier.
     * @param id - SidelineIdentifier you want to retrieve the state for.
     * @return The ConsumerState that was persisted via persistSidelineRequestState().
     */
    public ConsumerState retrieveSidelineRequestState(final SidelineIdentifier id);
}
