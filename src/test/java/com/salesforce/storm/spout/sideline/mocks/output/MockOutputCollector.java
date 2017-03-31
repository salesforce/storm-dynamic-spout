package com.salesforce.storm.spout.sideline.mocks.output;

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
        for (Emission emission: getEmissions()) {
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
