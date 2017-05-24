package com.salesforce.storm.spout.sideline.trigger;

import java.util.UUID;

/**
 * Identifies a sideline request, this should be unique to the request
 */
public class SidelineRequestIdentifier {

    private String id;

    public SidelineRequestIdentifier(final String id) {
        this.id = id;
    }

    @Deprecated
    public SidelineRequestIdentifier(final UUID id) {
        this(id.toString());
    }

    /**
     * Will generate a UUID, this is no longer recommended
     */
    @Deprecated
    public SidelineRequestIdentifier() {
        this(UUID.randomUUID());
    }

    /**
     * Override toString to return the id.
     */
    @Override
    public String toString() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SidelineRequestIdentifier that = (SidelineRequestIdentifier) o;

        return id != null ? id.equals(that.id) : that.id == null;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
