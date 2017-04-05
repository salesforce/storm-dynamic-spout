package com.salesforce.storm.spout.sideline.trigger;

import java.util.UUID;

public class SidelineRequestIdentifier {

    public final UUID id;

    public SidelineRequestIdentifier(final UUID id) {
        this.id = id;
    }

    public SidelineRequestIdentifier() {
        this(UUID.randomUUID());
    }

    /**
     * Override toString to return the id.
     */
    @Override
    public String toString() {
        return id.toString();
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
