/**
 * Copyright (c) 2017, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 *   disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * * Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or promote products
 *   derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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
