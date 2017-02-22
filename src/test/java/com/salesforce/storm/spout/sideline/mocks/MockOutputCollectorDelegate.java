package com.salesforce.storm.spout.sideline.mocks;

import org.apache.storm.task.IOutputCollector;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Use MockOutputCollector instead.
 */
@Deprecated
public class MockOutputCollectorDelegate implements IOutputCollector {

    // Holds our failed tuples
    public ArrayList<Tuple> failedTuples = new ArrayList<Tuple>();

    // Holds our acked tuples
    public ArrayList<Tuple> ackedTuples = new ArrayList<Tuple>();

    // Holds our reported errors
    public ArrayList<Throwable> reportedErrors = new ArrayList<Throwable>();

    public ArrayList<List<Object>> emittedFields = new ArrayList<List<Object>>();

    public ArrayList<Tuple> emittedAnchors = new ArrayList<Tuple>();

    public String lastStreamId = null;


    @Override
    /**
     * Anchors are the tuples that were input the current bolt
     * Tuple is a list of Field values that are to be emitted on to the next bolt
     */
    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        lastStreamId = streamId;
        emittedFields.add(tuple);

        if (anchors != null) {
            for (Tuple anchor : anchors) {
                emittedAnchors.add(anchor);
            }
        }

        return null;
    }

    @Override
    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {
        lastStreamId = streamId;
        emittedFields.add(tuple);

        if (anchors != null) {
            for (Tuple anchor : anchors) {
                emittedAnchors.add(anchor);
            }
        }
    }

    @Override
    public void ack(Tuple input)
    {
        ackedTuples.add(input);
    }

    @Override
    public void fail(Tuple input)
    {
        failedTuples.add(input);
    }

    @Override
    public void resetTimeout(Tuple input) {
        // Does nothing for now..
    }

    @Override
    public void reportError(Throwable error)
    {
        reportedErrors.add(error);
    }

    public void reset() {
        failedTuples.clear();
        ackedTuples.clear();
        reportedErrors.clear();
        emittedAnchors.clear();
        emittedFields.clear();
    }
}
