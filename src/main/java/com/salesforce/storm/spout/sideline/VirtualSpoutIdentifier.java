package com.salesforce.storm.spout.sideline;

public class VirtualSpoutIdentifier {

    private final String id;

    public VirtualSpoutIdentifier(final String id) {
        this.id = id;
    }

    public String toString() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VirtualSpoutIdentifier that = (VirtualSpoutIdentifier) o;

        return id != null ? id.equals(that.id) : that.id == null;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
