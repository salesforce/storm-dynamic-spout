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
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.tuple.Tuple;

import java.util.Collection;
import java.util.List;

/**
 * This class can be passed into bolt.prepare() to capture all acks, fails, emits, etc. during testing. It provides
 * simple getters so those results can be retrieved and asserted in your tests.
 */
public class MockOutputCollector implements IOutputCollector {
    private List<Tuple> ackedTuples = Lists.newArrayList();
    private List<Emission> emissions = Lists.newArrayList();
    private List<Tuple> failedTuples = Lists.newArrayList();
    private List<Throwable> reportedErrors = Lists.newArrayList();

    public List<Tuple> getAckedTuples() {
        // Clone the list
        return ImmutableList.copyOf(ackedTuples);
    }

    public List<Emission> getEmissions() {
        // Clone the list
        return ImmutableList.copyOf(emissions);
    }

    /**
     * Return all emissions for the given StreamId.
     * @param streamId - the stream Id to filter by.
     */
    public List<Emission> getEmissionsByStreamId(String streamId) {
        List<Emission> matched = Lists.newArrayList();
        for (Emission emission : getEmissions()) {
            if (emission.getStreamId() != null && emission.getStreamId().equals(streamId)) {
                matched.add(emission);
            }
        }
        return matched;
    }

    public Emission getEmission(int index) {
        return emissions.get(index);
    }

    public Emission getLastEmission() {
        return emissions.size() == 0 ? null : emissions.get(emissions.size() - 1);
    }

    public List<Tuple> getFailedTuples() {
        return ImmutableList.copyOf(failedTuples);
    }

    public List<Throwable> getReportedErrors() {
        return ImmutableList.copyOf(reportedErrors);
    }

    @Override
    public void ack(Tuple input) {
        ackedTuples.add(input);
    }

    @Override
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        emitDirect(0, streamId, anchors, tuple);
        return null;
    }

    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        emissions.add(new Emission(taskId, streamId, anchors, tuple));
    }

    @Override
    public void fail(Tuple input) {
        failedTuples.add(input);
    }

    @Override
    public void resetTimeout(Tuple input) { }

    @Override
    public void reportError(Throwable error) {
        reportedErrors.add(error);
    }
}
