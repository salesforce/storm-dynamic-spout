package com.salesforce.storm.spout.sideline;

/**
 * Identifier for a virtual spout.
 */
public interface VirtualSpoutIdentifier {

    /**
     * Get the string form of the identifier.
     * @return A string of the identifier
     */
    String toString();

    /**
     * Is this identifier equal to another.
     * @param obj The other identifier
     * @return Whether or not the two identifiers are equal
     */
    boolean equals(Object obj);
}
