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

import java.util.List;

/**
 * Wrapper for tracking emissions from a {@link com.salesforce.storm.spout.dynamic.DynamicSpout}.
 */
public class SpoutEmission {
    private final Object messageId;
    private final String streamId;
    private final List<Object> tuple;
    private final Integer taskId;

    /**
     * Wrapper for tracking emissions from a {@link com.salesforce.storm.spout.dynamic.DynamicSpout}.
     * @param messageId message id.
     * @param streamId stream id.
     * @param tuple tuple.
     */
    public SpoutEmission(Object messageId, String streamId, List<Object> tuple) {
        this(messageId, streamId, tuple, null);
    }

    /**
     * Wrapper for tracking emissions from a {@link com.salesforce.storm.spout.dynamic.DynamicSpout}.
     * @param messageId message id.
     * @param streamId stream id.
     * @param tuple tuple.
     * @param taskId task id.
     */
    public SpoutEmission(Object messageId, String streamId, List<Object> tuple, Integer taskId) {
        this.messageId = messageId;
        this.streamId = streamId;
        this.tuple = tuple;
        this.taskId = taskId;
    }

    public Object getMessageId() {
        return messageId;
    }

    public String getStreamId() {
        return streamId;
    }

    public List<Object> getTuple() {
        return tuple;
    }

    public Integer getTaskId() {
        return taskId;
    }

    @Override
    public String toString() {
        return "SpoutEmission{"
            + "messageId=" + messageId
            + ", streamId='" + streamId + '\''
            + ", tuple=" + tuple
            + ", taskId=" + taskId
            + '}';
    }
}
