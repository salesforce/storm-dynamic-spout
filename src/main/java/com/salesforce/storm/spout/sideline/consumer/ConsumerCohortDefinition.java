package com.salesforce.storm.spout.sideline.consumer;

/**
 * This class presents information to a Consumer about how many total instances of it are running, along with
 * the unique instance number the consumer is.
 */
public class ConsumerCohortDefinition {
    /**
     * This represents how many consumer instances are running.
     * This should always be AT LEAST 1.
     */
    private final int totalInstances;

    /**
     * This defines what instance number the current instance is out of the total number of instances.
     * This number is 0 indexed.
     *
     * Example, if there are 10 instances total, this will have a value ranging from [0-9]
     */
    private final int instanceNumber;

    /**
     * Constructor.
     * @param totalInstances Total number of consumer instances running.
     * @param instanceNumber The instance number.
     */
    public ConsumerCohortDefinition(int totalInstances, int instanceNumber) {
        this.totalInstances = totalInstances;
        this.instanceNumber = instanceNumber;
    }

    /**
     * @return How many consumer instances are running.
     */
    public int getTotalInstances() {
        return totalInstances;
    }

     /**
     * @return Instance number the current instance is out of the total number of instances, indexed at 0.
     */
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
