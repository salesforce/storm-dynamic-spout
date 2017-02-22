package com.salesforce.storm.spout.sideline.mocks.output;

import java.util.List;

/**
 *
 */
public class SpoutEmission {
    private final Object messageId;
    private final String streamId;
    private final List<Object> tuple;
    private final Integer taskId;

    public SpoutEmission(Object messageId, String streamId, List<Object> tuple) {
        this(messageId, streamId, tuple, null);
    }

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
        return "SpoutEmission{" +
                "messageId=" + messageId +
                ", streamId='" + streamId + '\'' +
                ", tuple=" + tuple +
                ", taskId=" + taskId +
                '}';
    }
}
