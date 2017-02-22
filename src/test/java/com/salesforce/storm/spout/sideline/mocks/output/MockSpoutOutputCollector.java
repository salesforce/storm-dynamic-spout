package com.salesforce.storm.spout.sideline.mocks.output;

import com.google.common.collect.Lists;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;

import java.util.List;

/**
 * Hacked up extension of SpoutOutputCollector.
 */
public class MockSpoutOutputCollector extends SpoutOutputCollector {
    private List<SpoutEmission> emissions = Lists.newArrayList();


    public MockSpoutOutputCollector() {
        super(null);
    }

    // Not used.
    public MockSpoutOutputCollector(ISpoutOutputCollector delegate) {
        super(delegate);
    }

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
        // Dunno? Nothing yet.
    }

    public List<SpoutEmission> getEmissions() {
        return emissions;
    }

    public void reset() {
        emissions.clear();
    }
}
