package com.salesforce.storm.spout.sideline.persistence;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import com.salesforce.storm.spout.sideline.trigger.SidelineIdentifier;

import java.util.HashMap;
import java.util.Map;

/**
 * In memory persistence layer implementation. useful for tests.
 * NOT for production use as all state will be lost between JVM restarts.
 */
public class InMemoryPersistenceManager implements PersistenceManager {
    // "Persists" consumer state in memory.
    private final Map<String,ConsumerState> storedConsumerState = new HashMap<>();

    // "Persists" side line request states in memory.
    private final Map<SidelineIdentifier, ConsumerState> storedSidelineRequests = new HashMap<>();

    @Override
    public void init() {
    }

    @Override
    public void close() {
        // Cleanup
        storedConsumerState.clear();
        storedSidelineRequests.clear();
    }

    /**
     * Pass in the consumer state that you'd like persisted.
     * @param consumerState - ConsumerState to be persisted.
     */
    @Override
    public void persistConsumerState(String consumerId, ConsumerState consumerState) {
        storedConsumerState.put(consumerId, consumerState);
    }

    /**
     * Retrieves the consumer state from the persistence layer.
     * @return ConsumerState
     */
    @Override
    public ConsumerState retrieveConsumerState(String consumerId) {
        return storedConsumerState.get(consumerId);
    }

    /**
     * @param id - unique identifier for the sideline request.
     * @param state - the associated state to be stored w/ the request.
     */
    @Override
    public void persistSidelineRequestState(SidelineIdentifier id, ConsumerState state) {
        storedSidelineRequests.put(id, state);
    }

    /**
     * Retrieves a sideline request state for the given SidelineIdentifier.
     * @param id - SidelineIdentifier you want to retrieve the state for.
     * @return The ConsumerState that was persisted via persistSidelineRequestState().
     */
    @Override
    public ConsumerState retrieveSidelineRequestState(SidelineIdentifier id) {
        return storedSidelineRequests.get(id);
    }
}
