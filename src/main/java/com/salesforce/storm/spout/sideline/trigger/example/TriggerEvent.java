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

package com.salesforce.storm.spout.sideline.trigger.example;

import com.salesforce.storm.spout.sideline.trigger.SidelineType;

import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 * An event to a {@link com.salesforce.storm.spout.sideline.trigger.SidelineTrigger} that communicates whether or not a START or STOP
 * request should be processed.
 */
public class TriggerEvent {

    private SidelineType type;

    private Map<String,Object> data;

    private Date createdAt;

    private String createdBy;

    private String description;

    private TriggerEvent() {
    }

    public SidelineType getType() {
        return type;
    }

    public Map<String,Object> getData() {
        return data;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TriggerEvent that = (TriggerEvent) obj;
        return type == that.type
            && Objects.equals(data, that.data)
            && Objects.equals(createdAt, that.createdAt)
            && Objects.equals(createdBy, that.createdBy)
            && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, data, createdAt, createdBy, description);
    }

    @Override
    public String toString() {
        return "TriggerEvent{"
            + "type=" + type
            + ", data=" + data
            + ", createdAt=" + createdAt
            + ", createdBy='" + createdBy + '\''
            + ", description='" + description + '\''
            + '}';
    }
}
