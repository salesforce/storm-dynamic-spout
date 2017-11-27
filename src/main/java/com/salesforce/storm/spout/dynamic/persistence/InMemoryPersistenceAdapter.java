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

package com.salesforce.storm.spout.dynamic.persistence;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;

import java.util.Map;

/**
 * In memory persistence layer implementation. useful for tests.
 * NOT for production use as all state will be lost between JVM restarts.
 */
public class InMemoryPersistenceAdapter implements PersistenceAdapter {

    /**
     * Used within tests to more easily validate assertions.
     */
    public static PersistConsumerStateCallback<String, Integer, Long> persistConsumerStateCallback = (consumerId, partitionId, offset) -> {
        // No-op by default.
    };

    // "Persists" consumer state in memory.
    private Map<String, Long> storedConsumerState;

    @Override
    public void open(final SpoutConfig spoutConfig) {
        // Allow non-destructive re-opening
        if (storedConsumerState == null) {
            storedConsumerState = Maps.newHashMap();
        }
    }

    @Override
    public void close() {
        // Cleanup
        storedConsumerState.clear();
    }

    /**
     * Pass in the consumer state that you'd like persisted.
     * @param consumerId Id of consumer to persist partition offset for.
     * @param partitionId The partitionId to persist an offset for.
     * @param offset The offset to persist.
     */
    @Override
    public void persistConsumerState(String consumerId, int partitionId, long offset) {
        storedConsumerState.put(getConsumerStateKey(consumerId, partitionId), offset);
        persistConsumerStateCallback.apply(consumerId, partitionId, offset);
    }

    /**
     * Retrieves the consumer state from the persistence layer.
     * @return ConsumerState
     */
    @Override
    public Long retrieveConsumerState(String consumerId, int partitionId) {
        return storedConsumerState.get(getConsumerStateKey(consumerId, partitionId));
    }

    @Override
    public void clearConsumerState(String consumerId, int partitionId) {
        storedConsumerState.remove(getConsumerStateKey(consumerId, partitionId));
    }

    private String getConsumerStateKey(final String consumerId, final int partitionId) {
        return consumerId.concat("/").concat(String.valueOf(partitionId));
    }

    /**
     * Callback definition that can be used to hook into (and track) changes to state when testing.
     * @param <String> consumerId
     * @param <Integer> partitionId
     * @param <Long> offset
     */
    @FunctionalInterface
    public interface PersistConsumerStateCallback<String, Integer, Long> {
        void apply(String consumerId, Integer partitionId, Long offset);
    }
}
