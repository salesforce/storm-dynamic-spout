package com.salesforce.storm.spout.sideline.mocks.output;

import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

    public Emission(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        this.taskId = taskId;
        this.streamId = streamId;
        this.anchors = anchors;
        this.tuple = tuple;
    }

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
        return new ArrayList<>(Arrays.asList(anchor));
    }

    @Override
    public String toString() {
        return "Emission{" +
                "taskId=" + taskId +
                ", streamId='" + streamId + '\'' +
                ", anchors=" + anchors +
                ", tuple=" + tuple +
                '}';
    }
}
