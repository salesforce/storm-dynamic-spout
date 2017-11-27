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

package com.salesforce.storm.spout.sideline.persistence;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * In memory persistence layer implementation. useful for tests.
 * NOT for production use as all state will be lost between JVM restarts.
 */
public class InMemoryPersistenceAdapter implements PersistenceAdapter {

    // "Persists" side line request states in memory.
    private Map<SidelineRequestStateKey, SidelinePayload> storedSidelineRequests;

    @Override
    public void open(final SpoutConfig spoutConfig) {
        if (storedSidelineRequests == null) {
            storedSidelineRequests = Maps.newHashMap();
        }
    }

    @Override
    public void close() {
        // Cleanup
        storedSidelineRequests.clear();
    }

    @Override
    public void persistSidelineRequestState(
        final SidelineType type,
        final SidelineRequestIdentifier id,
        final SidelineRequest request,
        final ConsumerPartition consumerPartition,
        final Long startingOffset,
        final Long endingOffset
    ) {
        storedSidelineRequests.put(
            getSidelineRequestStateKey(id, consumerPartition),
            new SidelinePayload(type, id, request, startingOffset, endingOffset)
        );
    }

    @Override
    public SidelinePayload retrieveSidelineRequest(SidelineRequestIdentifier id, ConsumerPartition consumerPartition) {
        return storedSidelineRequests.get(getSidelineRequestStateKey(id, consumerPartition));
    }

    @Override
    public void clearSidelineRequest(SidelineRequestIdentifier id, ConsumerPartition consumerPartition) {
        storedSidelineRequests.remove(getSidelineRequestStateKey(id, consumerPartition));
    }

    @Override
    public List<SidelineRequestIdentifier> listSidelineRequests() {
        List<SidelineRequestIdentifier> ids = Lists.newArrayList();

        for (SidelinePayload sidelinePayload : storedSidelineRequests.values()) {
            ids.add(sidelinePayload.id);
        }

        return ids;
    }

    @Override
    public Set<ConsumerPartition> listSidelineRequestPartitions(final SidelineRequestIdentifier id) {
        final Set<ConsumerPartition> partitions = Sets.newHashSet();

        for (SidelineRequestStateKey key : storedSidelineRequests.keySet()) {
            if (key.id.equals(id)) {
                partitions.add(key.consumerPartition);
            }
        }

        return Collections.unmodifiableSet(partitions);
    }

    private SidelineRequestStateKey getSidelineRequestStateKey(
        final SidelineRequestIdentifier id,
        final ConsumerPartition consumerPartition
    ) {
        return new SidelineRequestStateKey(id, consumerPartition);
    }

    private static class SidelineRequestStateKey {

        public final SidelineRequestIdentifier id;
        public final ConsumerPartition consumerPartition;

        SidelineRequestStateKey(final SidelineRequestIdentifier id, final ConsumerPartition consumerPartition) {
            this.id = id;
            this.consumerPartition = consumerPartition;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            SidelineRequestStateKey that = (SidelineRequestStateKey) obj;
            return Objects.equals(id, that.id)
                && Objects.equals(consumerPartition, that.consumerPartition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, consumerPartition);
        }
    }
}
