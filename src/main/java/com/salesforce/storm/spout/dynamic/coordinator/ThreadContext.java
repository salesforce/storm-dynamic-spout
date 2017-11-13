package com.salesforce.storm.spout.dynamic.coordinator;

/**
 * ThreadContext provides details about the context an instance
 * is running within.
 */
public class ThreadContext {
    /**
     * Represents the name of the Storm component that it's executing within.
     */
    private final String componentId;

    /**
     * Represents the instance number of the Storm component it's executing within.
     */
    private final int componentIndex;

    /**
     * Constructor.
     * @param componentId Name of the Storm component that it's executing within.
     * @param componentIndex Instance number of the Storm component it's executing within.
     */
    public ThreadContext(final String componentId, final int componentIndex) {
        this.componentId = componentId;
        this.componentIndex = componentIndex;
    }

    public String getComponentId() {
        return componentId;
    }

    public int getComponentIndex() {
        return componentIndex;
    }

    @Override
    public String toString() {
        return componentId + ':' + componentIndex;
    }
}
