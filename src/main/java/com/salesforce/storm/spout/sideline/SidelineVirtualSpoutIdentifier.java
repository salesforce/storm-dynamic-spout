package com.salesforce.storm.spout.sideline;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;

public class SidelineVirtualSpoutIdentifier implements VirtualSpoutIdentifier {

    private final static String DELIMITER = ":";

    private final String prefix;
    private final SidelineRequestIdentifier sidelineRequestIdentifier;

    public SidelineVirtualSpoutIdentifier(final String prefix, final SidelineRequestIdentifier sidelineRequestIdentifier) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prefix), "Prefix is required!");
        Preconditions.checkArgument(sidelineRequestIdentifier != null, "SidelineRequest identifier is required!");

        this.prefix = prefix;
        this.sidelineRequestIdentifier = sidelineRequestIdentifier;
    }

    public SidelineRequestIdentifier getSidelineRequestIdentifier() {
        return sidelineRequestIdentifier;
    }

    @Override
    public String toString() {
        return prefix + DELIMITER + sidelineRequestIdentifier.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SidelineVirtualSpoutIdentifier that = (SidelineVirtualSpoutIdentifier) o;

        if (prefix != null ? !prefix.equals(that.prefix) : that.prefix != null) return false;
        return sidelineRequestIdentifier != null ? sidelineRequestIdentifier.equals(that.sidelineRequestIdentifier) : that.sidelineRequestIdentifier == null;
    }

    @Override
    public int hashCode() {
        int result = prefix != null ? prefix.hashCode() : 0;
        result = 31 * result + (sidelineRequestIdentifier != null ? sidelineRequestIdentifier.hashCode() : 0);
        return result;
    }
}
