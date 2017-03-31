package com.salesforce.storm.spout.sideline.persistence;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.kafka.ConsumerState;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;

import java.util.List;
import java.util.Map;

/**
 * In memory persistence layer implementation. useful for tests.
 * NOT for production use as all state will be lost between JVM restarts.
 */
public class InMemoryPersistenceAdapter implements PersistenceAdapter {
    // "Persists" consumer state in memory.
    private Map<String, Long> storedConsumerState;

    // "Persists" side line request states in memory.
    private Map<SidelineRequestIdentifier, SidelinePayload> storedSidelineRequests;

    @Override
    public void open(Map topologyConfig) {
        // Allow non-destructive re-opening
        if (storedConsumerState == null) {
            storedConsumerState = Maps.newHashMap();
        }
        if (storedSidelineRequests == null) {
            storedSidelineRequests = Maps.newHashMap();
        }
    }

    @Override
    public void close() {
        // Cleanup
        storedConsumerState.clear();
        storedSidelineRequests.clear();
    }

    /**
     * Pass in the consumer state that you'd like persisted.
     * @param consumerId Id of consumer to persist partition offset for.
     * @param partitionId The partitionId to persist an offset for.
     * @param offset The offset to persist.
     */
    @Override
    public void persistConsumerState(String consumerId, int partitionId, long offset) {
        storedConsumerState.put(getConsumerStateKey(consumerId, partitionId), offset);
    }

    /**
     * Retrieves the consumer state from the persistence layer.
     * @return ConsumerState
     */
    @Override
    public Long retrieveConsumerState(String consumerId, int partitionId) {
        return storedConsumerState.get(getConsumerStateKey(consumerId, partitionId));
    }

    @Override
    public void clearConsumerState(String consumerId, int partitionId) {
        storedConsumerState.remove(getConsumerStateKey(consumerId, partitionId));
    }

    /**
     * @param type - SidelineType (Start or Stop)
     * @param id - unique identifier for the sideline request.
     * @param endingState - The state when we can stop consuming.
     */
    @Override
    public void persistSidelineRequestState(SidelineType type, SidelineRequestIdentifier id, SidelineRequest request, ConsumerState startingState, ConsumerState endingState) {
        storedSidelineRequests.put(id, new SidelinePayload(type, id, request, startingState, null));
    }

    /**
     * Retrieves a sideline request state for the given SidelineRequestIdentifier.
     * @param id - SidelineRequestIdentifier you want to retrieve the state for.
     * @return The ConsumerState that was persisted via persistSidelineRequestState().
     */
    @Override
    public SidelinePayload retrieveSidelineRequest(SidelineRequestIdentifier id) {
        return storedSidelineRequests.get(id);
    }

    @Override
    public void clearSidelineRequest(SidelineRequestIdentifier id) {
        storedSidelineRequests.remove(id);
    }

    @Override
    public List<SidelineRequestIdentifier> listSidelineRequests() {
        return Lists.newArrayList(storedSidelineRequests.keySet());
    }

    private String getConsumerStateKey(final String consumerId, final int partitionId) {
        return consumerId.concat(String.valueOf(partitionId));
    }
}
