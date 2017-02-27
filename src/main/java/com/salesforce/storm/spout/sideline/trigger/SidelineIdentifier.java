package com.salesforce.storm.spout.sideline.trigger;

import java.util.UUID;

public class SidelineIdentifier {

    final public UUID id;

    public SidelineIdentifier(final UUID id) {
        this.id = id;
    }

    public SidelineIdentifier() {
        this(UUID.randomUUID());
    }

    /**
     * Override toString to return the id.
     */
    @Override
    public String toString() {
        return id.toString();
    }
}
