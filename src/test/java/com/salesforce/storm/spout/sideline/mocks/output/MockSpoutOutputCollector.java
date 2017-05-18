package com.salesforce.storm.spout.sideline.mocks.output;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;

import java.util.List;

/**
 * Extension of SpoutOutputCollector to aide in testing.
 */
public class MockSpoutOutputCollector extends SpoutOutputCollector {
    /**
     * This contains all of the Tuples that were 'emitted' to our MockOutputCollector.
     */
    private List<SpoutEmission> emissions = Lists.newArrayList();


    public MockSpoutOutputCollector() {
        super(null);
    }

    /**
     * Not used, but here to comply to SpoutOutputCollector interface.
     * @param delegate - not used.
     */
    public MockSpoutOutputCollector(ISpoutOutputCollector delegate) {
        super(delegate);
    }

    // Required Interface methods.

    /**
     *
     * @param streamId - the stream the tuple should be emitted down.
     * @param tuple - the tuple to emit
     * @param messageId - the tuple's message Id.
     * @return - The interface is supposed to return the list of task ids that this tuple was sent to,
     *           but here since we have no task ids to send the tuples to, no idea what to return.
     */
    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        emissions.add(new SpoutEmission(messageId, streamId, tuple));

        // WTF BBQ?
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
    public void reportError(Throwable error) {
        // Not implemented yet.
    }

    // Helper Methods

    /**
     * @return - Return a clone of our Emissions in an unmodifiable list.
     */
    public List<SpoutEmission> getEmissions() {
        return ImmutableList.copyOf(emissions);
    }

    /**
     * Resets the internal state of our mock output collector.
     */
    public void reset() {
        emissions.clear();
    }
}
