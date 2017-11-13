package com.salesforce.storm.spout.dynamic.coordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ThreadContext {
    private final String componentId;
    private final int componentIndex;

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
