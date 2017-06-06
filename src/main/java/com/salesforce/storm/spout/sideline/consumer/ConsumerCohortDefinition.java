package com.salesforce.storm.spout.sideline.consumer;

/**
 * TODO write this.
 */
public class ConsumerCohortDefinition {
    private final int totalInstances;
    private final int instanceNumber;

    public ConsumerCohortDefinition(int totalInstances, int instanceNumber) {
        this.totalInstances = totalInstances;
        this.instanceNumber = instanceNumber;
    }

    public int getTotalInstances() {
        return totalInstances;
    }

    public int getInstanceNumber() {
        return instanceNumber;
    }

    @Override
    public String toString() {
        return "ConsumerCohortDefinition{"
            + "totalInstances=" + totalInstances
            + ", instanceNumber=" + instanceNumber
            + '}';
    }
}
