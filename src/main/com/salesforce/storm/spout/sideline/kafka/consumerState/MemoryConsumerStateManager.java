package com.salesforce.storm.spout.sideline.kafka.consumerState;

/**
 * In memory consumer state, useful for tests, not production use.
 */
public class MemoryConsumerStateManager implements ConsumerStateManager {
    private ConsumerState consumerState = new ConsumerState();

    @Override
    public void init() {

    }

    @Override
    public void close() {

    }

    @Override
    public void persistState(ConsumerState consumerState) {
        this.consumerState = consumerState;
    }

    @Override
    public ConsumerState getState() {
        return consumerState;
    }
}
