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

package com.salesforce.storm.spout.dynamic.mocks.output;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;

import java.util.ArrayList;
import java.util.List;

/**
 * Extension of SpoutOutputCollector to aide in testing.
 */
public class MockSpoutOutputCollector extends SpoutOutputCollector {
    /**
     * This contains all of the Tuples that were 'emitted' to our MockOutputCollector.
     */
    private final List<SpoutEmission> emissions = new ArrayList<>();

    /**
     * This contains any errors that were reported.
     */
    private final List<Throwable> reportedErrors = new ArrayList<>();


    public MockSpoutOutputCollector() {
        super(null);
    }

    /**
     * Not used, but here to comply to SpoutOutputCollector interface.
     * @param delegate not used.
     */
    public MockSpoutOutputCollector(final ISpoutOutputCollector delegate) {
        super(delegate);
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        emissions.add(new SpoutEmission(messageId, streamId, tuple));
        return Lists.newArrayList();
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        emissions.add(new SpoutEmission(messageId, streamId, tuple, taskId));
    }

    @Override
    public long getPendingCount() {
        // Dunno?  Nothing yet.
        return 0;
    }

    @Override
    public void reportError(final Throwable error) {
        reportedErrors.add(error);
    }

    /**
     * Get a clone of our Emissions in an unmodifiable list.
     * @return clone of our Emissions in an unmodifiable list
     */
    public List<SpoutEmission> getEmissions() {
        return ImmutableList.copyOf(emissions);
    }

    /**
     * Get a clone of reported errors in an unmodifiable list.
     * @return clone of reported errors in an unmodifiable list
     */
    public List<Throwable> getReportedErrors() {
        return ImmutableList.copyOf(reportedErrors);
    }

    /**
     * Resets the internal state of our mock output collector.
     */
    public void reset() {
        emissions.clear();
        reportedErrors.clear();
    }
}
