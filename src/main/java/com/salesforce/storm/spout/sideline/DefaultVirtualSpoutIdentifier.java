package com.salesforce.storm.spout.sideline;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Identifier for a virtual spout.
 */
public class DefaultVirtualSpoutIdentifier implements VirtualSpoutIdentifier {

    /**
     * The actual identifier.
     */
    private final String id;

    /**
     * Create a new virtual spout identifier from a string.
     * @param id String of the id
     */
    public DefaultVirtualSpoutIdentifier(final String id) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id), "You must provide something in order to create an identifier!");
        this.id = id;
    }

    /**
     * Get the string form of the identifier.
     * @return A string of the identifier
     */
    @Override
    public String toString() {
        return id;
    }

    /**
     * Is this identifier equal to another.
     * @param obj The other identifier
     * @return Whether or not the two identifiers are equal
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DefaultVirtualSpoutIdentifier that = (DefaultVirtualSpoutIdentifier) obj;

        return id != null ? id.equals(that.id) : that.id == null;
    }

    /**
     * Make a hash code for this object.
     * @return Hash code
     */
    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
