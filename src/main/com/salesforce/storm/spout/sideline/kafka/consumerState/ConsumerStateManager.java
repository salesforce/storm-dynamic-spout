package com.salesforce.storm.spout.sideline.kafka.consumerState;

/**
 * This handles reading and writing state to/from a datasource
 */
public interface ConsumerStateManager {

    /**
     * Performs any required initialization/connection/setup required for
     * the implementation.  By contract, this will be called once prior to calling
     * persistState() or getState().
     */
    public void init();

    /**
     * Performs any cleanup required for the implementation on shutdown.
     */
    public void close();

    /**
     * Pass in the consumer state that you'd like persisted.
     * @param consumerState - ConsumerState to be persisted.
     */
    public void persistState(ConsumerState consumerState);

    /**
     * Retrieves the consumer state from the persistence layer.
     * @return ConsumerState
     */
    public ConsumerState getState();
}
