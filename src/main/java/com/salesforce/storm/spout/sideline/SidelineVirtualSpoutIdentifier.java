/*
 * Copyright (c) 2018, Salesforce.com, Inc.
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
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;

/**
 * Identifier for sideline virtual spouts.
 */
public class SidelineVirtualSpoutIdentifier implements VirtualSpoutIdentifier {

    /**
     * Delimiter for when we create a string for this identifier.
     */
    private static final String DELIMITER = ":";

    /**
     * Prefix that sits between the consumer id and the sideline request id.
     */
    private static final String PREFIX = "sideline";

    /**
     * Consumer id of the spout.
     */
    private final String consumerId;

    /**
     * Identifier for the sideline request the virtual spout was created for.
     */
    private final SidelineRequestIdentifier sidelineRequestIdentifier;

    /**
     * New instance of a SidelineVirtualSpoutIdentifier using a consumerId and a SidelineRequestIdentifier.
     * @param consumerId Prefix to append to the Identifier
     * @param sidelineRequestIdentifier SidelineRequestIdentifier to associate with the VirtualSpoutId.
     */
    public SidelineVirtualSpoutIdentifier(final String consumerId, final SidelineRequestIdentifier sidelineRequestIdentifier) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(consumerId), "Consumer id is required!");
        Preconditions.checkArgument(sidelineRequestIdentifier != null, "SidelineRequest identifier is required!");

        this.consumerId = consumerId;
        this.sidelineRequestIdentifier = sidelineRequestIdentifier;
    }

    /**
     * Get the consumerId of the identifier, this is usually related to the consumer.
     * @return Prefix of the identifier.
     */
    public String getConsumerId() {
        return consumerId;
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
        return consumerId + DELIMITER + PREFIX + DELIMITER + sidelineRequestIdentifier.toString();
    }

    /**
     * Evaluates the equality of two sideline virtual spout identifiers.
     * @param other Identifier to be compared against.
     * @return Are they equal?
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        SidelineVirtualSpoutIdentifier that = (SidelineVirtualSpoutIdentifier) other;

        if (consumerId != null ? !consumerId.equals(that.consumerId) : that.consumerId != null) {
            return false;
        }
        return sidelineRequestIdentifier != null ? sidelineRequestIdentifier.equals(that.sidelineRequestIdentifier) :
            that.sidelineRequestIdentifier == null;
    }

    /**
     * Generate a hash code for this identifier instance.
     * @return Hash code.
     */
    @Override
    public int hashCode() {
        int result = consumerId != null ? consumerId.hashCode() : 0;
        result = 31 * result + (sidelineRequestIdentifier != null ? sidelineRequestIdentifier.hashCode() : 0);
        return result;
    }
}
