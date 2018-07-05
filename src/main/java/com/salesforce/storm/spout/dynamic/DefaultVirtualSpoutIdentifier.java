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

package com.salesforce.storm.spout.dynamic;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Identifier for a virtual spout.
 */
public final class DefaultVirtualSpoutIdentifier implements VirtualSpoutIdentifier {

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
        return id.hashCode();
    }
}
