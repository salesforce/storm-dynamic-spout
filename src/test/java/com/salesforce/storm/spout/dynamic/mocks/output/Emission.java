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

import org.apache.storm.tuple.Tuple;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This class is used to hold all arguments from a single call to the emit() function (or one of its variants).
 * It is used by MockOutputCollector which holds an array of this class (called emissions) to store, in order, all
 * the emit() calls it receives during a test.
 */
public class Emission {
    private final int taskId;
    private final String streamId;
    private final Collection<Tuple> anchors;
    private final List<Object> tuple;

    /**
     * This class is used to hold all arguments from a single call to the emit() function (or one of its variants).
     * It is used by MockOutputCollector which holds an array of this class (called emissions) to store, in order, all
     * the emit() calls it receives during a test.
     * @param taskId task id.
     * @param streamId stream id.
     * @param anchors anchors.
     * @param tuple tuple.
     */
    public Emission(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        this.taskId = taskId;
        this.streamId = streamId;
        this.anchors = anchors;
        this.tuple = tuple;
    }

    /**
     * This class is used to hold all arguments from a single call to the emit() function (or one of its variants).
     * It is used by MockOutputCollector which holds an array of this class (called emissions) to store, in order, all
     * the emit() calls it receives during a test.
     * @param streamId stream id.
     * @param anchors anchors.
     * @param tuple tuple.
     */
    public Emission(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        this(0, streamId, anchors, tuple);
    }

    public Emission(String streamId, Tuple anchor, List<Object> tuple) {
        this(streamId, makeAnchorCollection(anchor), tuple);
    }

    public int getTaskId() {
        return taskId;
    }

    public String getStreamId() {
        return streamId;
    }

    public Collection<Tuple> getAnchors() {
        return anchors;
    }

    public List<Object> getTuple() {
        return tuple;
    }

    public static Collection<Tuple> makeAnchorCollection(Tuple anchor) {
        return Collections.singletonList(anchor);
    }

    @Override
    public String toString() {
        return "Emission{"
            + "taskId=" + taskId
            + ", streamId='" + streamId + '\''
            + ", anchors=" + anchors
            + ", tuple=" + tuple
            + '}';
    }
}
