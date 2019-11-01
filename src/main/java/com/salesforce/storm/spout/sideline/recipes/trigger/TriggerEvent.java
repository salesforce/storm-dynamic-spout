/*
 * Copyright (c) 2017, 2018, Salesforce.com, Inc.
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

package com.salesforce.storm.spout.sideline.recipes.trigger;

import com.google.common.base.Preconditions;
import com.salesforce.storm.spout.dynamic.JSON;
import com.salesforce.storm.spout.dynamic.Tools;
import com.salesforce.storm.spout.dynamic.filter.FilterChainStep;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * An event to a {@link com.salesforce.storm.spout.sideline.trigger.SidelineTrigger} that communicates the desired type of sideline state
 * for the system.
 */
public class TriggerEvent {

    private SidelineType type;

    private FilterChainStep filterChainStep;

    private LocalDateTime createdAt;

    private String createdBy;

    private String description;

    private boolean processed;

    private LocalDateTime updatedAt;

    /**
     * An event to a {@link com.salesforce.storm.spout.sideline.trigger.SidelineTrigger} that communicates the desired type of
     * sideline state for the system.
     * <p>
     * When you create a TriggerEvent in Zookeeper always set processed = false, the trigger implementation will flip this to true
     * after it has been picked up and handled by the trigger. This allows you to distinguish an event that's been handled by the
     * trigger and one that has not.
     *
     * @param type        sideline type.
     * @param filterChainStep filter chain step
     * @param createdAt   when the event was created.
     * @param createdBy   who created the event.
     * @param description a description of the reason for the sideline request.
     * @param processed   whether or not the event (in its current state has been processed)
     * @param updatedAt   Timestamp the event was last updated.
     */
    public TriggerEvent(
        final SidelineType type,
        final FilterChainStep filterChainStep,
        final LocalDateTime createdAt,
        final String createdBy,
        final String description,
        final boolean processed,
        final LocalDateTime updatedAt
    ) {
        Preconditions.checkNotNull(type, "Type is required.");
        Preconditions.checkNotNull(filterChainStep, "FilterChainStep is required.");
        Preconditions.checkNotNull(createdAt, "Created at time is required.");
        Preconditions.checkNotNull(createdBy, "Created by is required.");
        Preconditions.checkNotNull(description, "Description is required.");

        this.type = type;
        this.filterChainStep = filterChainStep;
        this.createdAt = createdAt;
        this.createdBy = createdBy;
        this.description = description;
        this.processed = processed;
        this.updatedAt = updatedAt;
    }

    /**
     * Generate an identifier for this {@link TriggerEvent}.
     *
     * The identifier generated here is a JSON serialized version of the {@link FilterChainStep} which we then make
     * an md5 hash of. If we have a created date we tack on it's millis to the end for extra uniqueness. It's the
     * {@link FilterChainStep} that is the important part of a {@link TriggerEvent} and the assumption here is that
     * there are properties that make the given filter unique. For example, if you're filtering out a tenant on a
     * multi tenant stream you probably have a generic filter which has a property to hold the given tenant.
     *
     * If you don't have uniqueness represented in the properties of your filter this whole recipe probably isn't a
     * great fit for your use case.
     *
     * @return string representing the event.
     */
    public String getIdentifier() {
        // Note: This only works for to() because we only need the implementation name for from()
        final String data = (new JSON()).to(getFilterChainStep());

        final StringBuilder identifier = new StringBuilder(Tools.makeMd5Hash(data));

        // If we were provided a date time in the event, append the time stamp of that event to the identifier
        if (this.getCreatedAt() != null) {
            identifier.append("-");
            identifier.append(
                this.getCreatedAt().atZone(ZoneOffset.UTC).toInstant().toEpochMilli()
            );
        }

        return identifier.toString();
    }

    public SidelineType getType() {
        return type;
    }

    public FilterChainStep getFilterChainStep() {
        return filterChainStep;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public String getDescription() {
        return description;
    }

    public boolean isProcessed() {
        return processed;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
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
        return processed == that.processed
            && type == that.type
            && Objects.equals(filterChainStep, that.filterChainStep)
            && Objects.equals(createdAt, that.createdAt)
            && Objects.equals(createdBy, that.createdBy)
            && Objects.equals(description, that.description)
            && Objects.equals(updatedAt, that.updatedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, filterChainStep, createdAt, createdBy, description);
    }

    @Override
    public String toString() {
        return "TriggerEvent{"
            + "type=" + type
            + ", filterChainStep=" + filterChainStep
            + ", createdAt=" + createdAt
            + ", createdBy='" + createdBy + '\''
            + ", description='" + description + '\''
            + ", processed=" + processed
            + ", updatedAt=" + updatedAt
            + '}';
    }
}
