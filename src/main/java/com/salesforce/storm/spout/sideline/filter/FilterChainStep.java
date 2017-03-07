package com.salesforce.storm.spout.sideline.filter;

import com.salesforce.storm.spout.sideline.KafkaMessage;

import java.io.Serializable;

/**
 * A step in a chain for processing records, these steps must be serializable and should include
 * an equals() method.
 */
public interface FilterChainStep extends Serializable {

    /**
     * Inputs an object, performs some business logic on it and then returns the result
     *
     * @param message The filter to be processed by this step of the chain
     * @return The resulting filter after being processed
     */
    boolean filter(KafkaMessage message);
}
