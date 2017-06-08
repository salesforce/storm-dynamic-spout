package com.salesforce.storm.spout.sideline;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;

public class SidelineVirtualSpoutIdentifier implements VirtualSpoutIdentifier {

    private final static String DELIMITER = ":";

    private final String prefix;
    private final String suffix;
    private final SidelineRequestIdentifier sidelineRequestIdentifier;

    public SidelineVirtualSpoutIdentifier(final String prefix, final String suffix) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prefix), "Prefix is required!");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prefix), "Suffix is required!");

        this.prefix = prefix;
        this.suffix = suffix;
        this.sidelineRequestIdentifier = null;
    }

    public SidelineVirtualSpoutIdentifier(final String prefix, final SidelineRequestIdentifier sidelineRequestIdentifier) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prefix), "Prefix is required!");
        Preconditions.checkArgument(sidelineRequestIdentifier != null, "SidelineRequest identifier is required!");

        this.prefix = prefix;
        this.suffix = null;
        this.sidelineRequestIdentifier = sidelineRequestIdentifier;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append(prefix);

        if (suffix != null) {
            builder.append(DELIMITER).append(suffix);
        }

        if (sidelineRequestIdentifier != null) {
            builder.append(DELIMITER).append(sidelineRequestIdentifier.toString());
        }

        return builder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SidelineVirtualSpoutIdentifier that = (SidelineVirtualSpoutIdentifier) o;

        if (prefix != null ? !prefix.equals(that.prefix) : that.prefix != null) return false;
        if (suffix != null ? !suffix.equals(that.suffix) : that.suffix != null) return false;
        return sidelineRequestIdentifier != null ? sidelineRequestIdentifier.equals(that.sidelineRequestIdentifier) : that.sidelineRequestIdentifier == null;
    }

    @Override
    public int hashCode() {
        int result = prefix != null ? prefix.hashCode() : 0;
        result = 31 * result + (suffix != null ? suffix.hashCode() : 0);
        result = 31 * result + (sidelineRequestIdentifier != null ? sidelineRequestIdentifier.hashCode() : 0);
        return result;
    }
}
