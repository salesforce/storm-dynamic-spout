package com.salesforce.storm.spout.sideline;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;

/**
 * Identifier for sideline virtual spouts.
 */
public class SidelineVirtualSpoutIdentifier implements VirtualSpoutIdentifier {

    /**
     * Delimiter for when we create a string for this identifier.
     */
    private final static String DELIMITER = ":";

    /**
     * Prefix of the spout, usually something corresponding to the consumer.
     */
    private final String prefix;

    /**
     * Identifier for the sideline request the virtual spout was created for.
     */
    private final SidelineRequestIdentifier sidelineRequestIdentifier;

    /**
     * New instance of a SidelineVirtualSpoutIdentifier using a prefix and a SidelineRequestIdentifier.
     * @param prefix
     * @param sidelineRequestIdentifier
     */
    public SidelineVirtualSpoutIdentifier(final String prefix, final SidelineRequestIdentifier sidelineRequestIdentifier) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prefix), "Prefix is required!");
        Preconditions.checkArgument(sidelineRequestIdentifier != null, "SidelineRequest identifier is required!");

        this.prefix = prefix;
        this.sidelineRequestIdentifier = sidelineRequestIdentifier;
    }

    /**
     * Get the prefix of the identifier, this is usually related to the consumer.
     * @return Prefix of the identifier.
     */
    public String getPrefix() {
        return prefix;
    }

    /**
     * Get the SidelineRequestIdentifier for the virtual spout.
     * @return SidelineRequestIdentifier of the virtual spout.
     */
    public SidelineRequestIdentifier getSidelineRequestIdentifier() {
        return sidelineRequestIdentifier;
    }

    /**
     * Create a string representation of the identifier.
     * @return String representation of the identifier.
     */
    @Override
    public String toString() {
        return prefix + DELIMITER + sidelineRequestIdentifier.toString();
    }

    /**
     * Evaluates the equality of two sideline virtual spout identifiers.
     * @param o Identifier to be compared against.
     * @return Are they equal?
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SidelineVirtualSpoutIdentifier that = (SidelineVirtualSpoutIdentifier) o;

        if (prefix != null ? !prefix.equals(that.prefix) : that.prefix != null) return false;
        return sidelineRequestIdentifier != null ? sidelineRequestIdentifier.equals(that.sidelineRequestIdentifier) : that.sidelineRequestIdentifier == null;
    }

    /**
     * Generate a hash code for this identifier instance.
     * @return Hash code.
     */
    @Override
    public int hashCode() {
        int result = prefix != null ? prefix.hashCode() : 0;
        result = 31 * result + (sidelineRequestIdentifier != null ? sidelineRequestIdentifier.hashCode() : 0);
        return result;
    }
}
