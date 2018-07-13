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

package com.salesforce.storm.spout.dynamic.metrics;

import org.slf4j.helpers.MessageFormatter;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages start times by key and returns deltas on stop calls for the same key.
 */
class TimerManager {

    /**
     * Keyed start values in millis.
     */
    private final Map<String, Long> startValuesMs = new ConcurrentHashMap<>();

    /**
     * Clock instance, normally a UTC clock but can be overridden for testing.
     */
    private Clock clock = Clock.systemUTC();

    /**
     * Manages start times by key and returns deltas on stop calls for the same key.
     */
    TimerManager() {
    }

    /**
     * Start a timer based upon a provided key.
     *
     * Defaults start time to right now.
     *
     * @param key key for timer
     */
    void start(final String key) {
        start(key, getClock().millis());
    }

    /**
     * Start a timer based upon a provided key with a specified start time.
     *
     * Most likely you don't need this method and should use the one that defaults to right now.
     *
     * @param key key for timer
     * @param startMs time in millis to set as the start
     */
    void start(final String key, final long startMs) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException(
                "Timer key cannot be null or empty."
            );
        }
        if (startValuesMs.containsKey(key)) {
            throw new IllegalStateException(
                MessageFormatter.format("The timer key {} already exists in this instances of {}", key, getClass()).getMessage()
            );
        }

        startValuesMs.put(key, startMs);
    }

    /**
     * Stop a timer based upon a provided key.
     *
     * @param key key for timer
     * @return time elapsed in millis
     */
    long stop(final String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException(
                "Timer key cannot be null or empty."
            );
        }
        if (!startValuesMs.containsKey(key)) {
            throw new IllegalStateException(
                MessageFormatter.format("The timer key {} does not exist in this instance of {}", key, getClass().toString()).getMessage()
            );
        }

        // This gets the value and then removes it, so we could in theory start this key again with a fresh timer
        final long startTimeMs = startValuesMs.remove(key);
        final long stopTimeMs = getClock().millis();

        return stopTimeMs - startTimeMs;
    }

    /**
     * Get clock instance.
     * @return clock instance
     */
    private Clock getClock() {
        if (this.clock == null) {
            throw new IllegalStateException(
                "Clock instance is null, you can't track elapsed time without a clock."
            );
        }
        return this.clock;
    }

    /**
     * Override the manager's clock instance, this should only be done in tests.
     *
     * @param clock clock instance for testing
     */
    void setClock(final Clock clock) {
        if (clock == null) {
            throw new IllegalArgumentException(
                "Clock instance cannot be null."
            );
        }
        this.clock = clock;
    }
}
