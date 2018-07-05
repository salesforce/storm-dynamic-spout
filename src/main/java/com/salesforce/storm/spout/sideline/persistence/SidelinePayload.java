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

package com.salesforce.storm.spout.sideline.persistence;

import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;

/**
 * Payload for a sideline, includes the type, request, id and positions for the sideline.
 */
public class SidelinePayload {

    /**
     * Type of sideline, either start of stop.
     */
    public final SidelineType type;
    /**
     * Identifier of the sideline request.
     */
    public final SidelineRequestIdentifier id;
    /**
     * Sideline request, contains the identifier and filter chain step to be applied.
     */
    public final SidelineRequest request;
    /**
     * Offset to begin the sideline at.
     */
    public final Long startingOffset;
    /**
     * Offset to end the sideline at.
     */
    public final Long endingOffset;

    /**
     * Payload for a sideline, includes the type, request, id and positions for the sideline.
     *
     * @param type type of sideline, either start of stop.
     * @param id identifier of the sideline request.
     * @param request sideline request, contains the identifier and filter chain step to be applied.
     * @param startingOffset offset to begin the sideline at.
     * @param endingOffset offset to end the sideline at.
     */
    SidelinePayload(
        final SidelineType type,
        final SidelineRequestIdentifier id,
        final SidelineRequest request,
        final Long startingOffset,
        final Long endingOffset
    ) {
        this.type = type;
        this.id = id;
        this.request = request;
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        SidelinePayload that = (SidelinePayload) object;

        if (type != that.type) {
            return false;
        }
        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        if (request != null ? !request.equals(that.request) : that.request != null) {
            return false;
        }
        if (startingOffset != null ? !startingOffset.equals(that.startingOffset) : that.startingOffset != null) {
            return false;
        }
        return endingOffset != null ? endingOffset.equals(that.endingOffset) : that.endingOffset == null;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (request != null ? request.hashCode() : 0);
        result = 31 * result + (startingOffset != null ? startingOffset.hashCode() : 0);
        result = 31 * result + (endingOffset != null ? endingOffset.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SidelinePayload{"
            + "type="
            + type
            + ", id="
            + id
            + ", request="
            + request
            + ", startingOffset="
            + startingOffset
            + ", endingOffset="
            + endingOffset
            + '}';
    }
}
