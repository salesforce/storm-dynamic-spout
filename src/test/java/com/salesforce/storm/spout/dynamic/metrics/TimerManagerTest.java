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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test that {@link TimerManager} tracks elapsed time correctly.
 */
class TimerManagerTest {

    private static final long FIXED_TIME_MS = 100000L;
    private static final Instant FIXED_TIME_INSTANT = Instant.ofEpochMilli(FIXED_TIME_MS);

    /**
     * Test that starting and stopping a timer records the proper amount of elapsed time.
     */
    @Test
    void testRecordTime() {
        // Number of millis we expect to skip before capturing our stop point
        final long skipMs = 1000;
        final String timerKey = "timer";

        final TimerManager timerManager = new TimerManager();
        // Set our internal clock to a fixed time, this will effectively be "now"
        timerManager.setClock(
            Clock.fixed(FIXED_TIME_INSTANT, ZoneOffset.UTC)
        );
        // At this point we should have captured the above fixed time for timerKey
        timerManager.start(timerKey);

        // Advance our internal clock by the skip amount
        timerManager.setClock(
            Clock.fixed(Instant.ofEpochMilli(FIXED_TIME_MS + skipMs), ZoneOffset.UTC)
        );

        final long elapsedMs = timerManager.stop(timerKey);

        assertThat(
            "Ms elapsed in the timer manager matches our skip amount",
            skipMs,
            equalTo(elapsedMs)
        );

        // Start the timer again, which should work because once things are stopped the key is removed, basically we should be able to
        // start a timer with the same key after it's been stopped.
        timerManager.start(timerKey);
    }

    /**
     * Test that attempting to start the same timer twice throws an exception.
     */
    @Test
    void testStartingTimerTwice() {
        final String timerKey = "timer";

        final TimerManager timerManager = new TimerManager();
        timerManager.start(timerKey);

        Assertions.assertThrows(IllegalStateException.class, () ->
            timerManager.start(timerKey)
        );
    }

    /**
     * Test that stopping a timer that hasn't been started throws an exception.
     */
    @Test
    void testStoppingUnstartedTimer() {
        final String timerKey = "timer";

        final TimerManager timerManager = new TimerManager();

        Assertions.assertThrows(IllegalStateException.class, () ->
            timerManager.stop(timerKey)
        );
    }
}