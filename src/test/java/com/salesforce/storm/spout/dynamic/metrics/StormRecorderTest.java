/**
 * Copyright (c) 2017, Salesforce.com, Inc.
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

package com.salesforce.storm.spout.dynamic.metrics;

import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.mocks.MockTopologyContext;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.metric.api.MultiReducedMetric;
import org.apache.storm.task.TopologyContext;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test that {@link StormRecorder} captures metrics.
 */
public class StormRecorderTest {
    private static final int defaultTimeWindow = 60;

    /**
     * Validate that we set things up with sane defaults.
     */
    @Test
    public void testOpen_useDefaults() {
        // Create empty config
        final SpoutConfig spoutConfig = new SpoutConfig();

        // Create mock TopologyContet
        final TopologyContext mockTopologyContext = mock(TopologyContext.class);

        // Create recorder and call open.
        final StormRecorder recorder = new StormRecorder();
        recorder.open(spoutConfig, mockTopologyContext);

        // Validate we got called as expected.

        // Shouldn't have interacted with the taskId
        verify(mockTopologyContext, never()).getThisTaskIndex();

        // Should have registered 4 metrics.
        verify(mockTopologyContext, times(1)).registerMetric(eq("GAUGES"), any(MultiReducedMetric.class), eq(defaultTimeWindow));
        verify(mockTopologyContext, times(1)).registerMetric(eq("TIMERS"), any(MultiReducedMetric.class), eq(defaultTimeWindow));
        verify(mockTopologyContext, times(1)).registerMetric(eq("COUNTERS"), any(MultiReducedMetric.class), eq(defaultTimeWindow));

        assertEquals("Should have empty prefix", "", recorder.getMetricPrefix());
        assertTrue("Should have empty prefix", recorder.getMetricPrefix().isEmpty());
    }

    /**
     * Validate that we accept a custom time window set from a long value.
     */
    @Test
    public void testOpen_customTimeWindowLong() {
        final Long timeBucket = 30L;

        // Create empty config
        final Map<String, Object> config = new HashMap<>();
        config.put(SpoutConfig.METRICS_RECORDER_TIME_BUCKET, timeBucket);

        // Create Spout Config
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Create mock TopologyContet
        final TopologyContext mockTopologyContext = mock(TopologyContext.class);

        // Create recorder and call open.
        final StormRecorder recorder = new StormRecorder();
        recorder.open(spoutConfig, mockTopologyContext);

        // Validate we got called as expected.

        // Shouldn't have interacted with the taskId
        verify(mockTopologyContext, never()).getThisTaskIndex();

        // Should have registered 4 metrics.
        verify(mockTopologyContext, times(1)).registerMetric(eq("GAUGES"), any(MultiReducedMetric.class), eq(timeBucket.intValue()));
        verify(mockTopologyContext, times(1)).registerMetric(eq("TIMERS"), any(MultiReducedMetric.class), eq(timeBucket.intValue()));
        verify(mockTopologyContext, times(1)).registerMetric(eq("COUNTERS"), any(MultiReducedMetric.class), eq(timeBucket.intValue()));
    }

    /**
     * Validate that we accept a custom time window set from an int value.
     */
    @Test
    public void testOpen_customTimeWindowInt() {
        final int timeBucket = 30;

        // Create empty config
        final Map<String, Object> config = new HashMap<>();
        config.put(SpoutConfig.METRICS_RECORDER_TIME_BUCKET, timeBucket);

        // Create Spout Config
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Create mock TopologyContet
        final TopologyContext mockTopologyContext = mock(TopologyContext.class);

        // Create recorder and call open.
        final StormRecorder recorder = new StormRecorder();
        recorder.open(spoutConfig, mockTopologyContext);

        // Validate we got called as expected.

        // Shouldn't have interacted with the taskId
        verify(mockTopologyContext, never()).getThisTaskIndex();

        // Should have registered 4 metrics.
        verify(mockTopologyContext, times(1)).registerMetric(eq("GAUGES"), any(MultiReducedMetric.class), eq(timeBucket));
        verify(mockTopologyContext, times(1)).registerMetric(eq("TIMERS"), any(MultiReducedMetric.class), eq(timeBucket));
        verify(mockTopologyContext, times(1)).registerMetric(eq("COUNTERS"), any(MultiReducedMetric.class), eq(timeBucket));
    }

    /**
     * Validate that we fall back gracefully for invalid value.
     */
    @Test
    public void testOpen_customTimeWindowInvalid() {
        final boolean timeBucketCfg = true;

        // Create empty config
        final Map<String, Object> config = new HashMap<>();
        config.put(SpoutConfig.METRICS_RECORDER_TIME_BUCKET, timeBucketCfg);

        // Create Spout Config
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Create mock TopologyContet
        final TopologyContext mockTopologyContext = mock(TopologyContext.class);

        // Create recorder and call open.
        final StormRecorder recorder = new StormRecorder();
        recorder.open(spoutConfig, mockTopologyContext);

        // Validate we got called as expected.

        // Shouldn't have interacted with the taskId
        verify(mockTopologyContext, never()).getThisTaskIndex();

        // Should have registered 4 metrics.
        verify(mockTopologyContext, times(1)).registerMetric(eq("GAUGES"), any(MultiReducedMetric.class), eq(defaultTimeWindow));
        verify(mockTopologyContext, times(1)).registerMetric(eq("TIMERS"), any(MultiReducedMetric.class), eq(defaultTimeWindow));
        verify(mockTopologyContext, times(1)).registerMetric(eq("COUNTERS"), any(MultiReducedMetric.class), eq(defaultTimeWindow));
    }

    /**
     * Validate that you can enable taskId prefixing.
     */
    @Test
    public void testOpen_taskIdPrefixEnabled() {
        // Define taskId in mock
        final int taskId = 20;

        // Create empty config
        final Map<String, Object> config = new HashMap<>();
        config.put(SpoutConfig.METRICS_RECORDER_ENABLE_TASK_ID_PREFIX, true);

        // Create Spout Config
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Create mock TopologyContet
        final TopologyContext mockTopologyContext = mock(TopologyContext.class);
        when(mockTopologyContext.getThisTaskIndex()).thenReturn(taskId);

        // Create recorder and call open.
        final StormRecorder recorder = new StormRecorder();
        recorder.open(spoutConfig, mockTopologyContext);

        // Validate
        verify(mockTopologyContext, times(1)).getThisTaskIndex();
        assertEquals("Should have taskId prefix", "task-" + taskId, recorder.getMetricPrefix());
    }

    /**
     * Validate that you can disable taskId prefixing.
     */
    @Test
    public void testOpen_taskIdPrefixDisabled() {
        // Define taskId in mock
        final int taskId = 20;

        // Create empty config
        final Map<String, Object> config = new HashMap<>();
        config.put(SpoutConfig.METRICS_RECORDER_ENABLE_TASK_ID_PREFIX, false);

        // Create Spout Config
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Create mock TopologyContet
        final TopologyContext mockTopologyContext = mock(TopologyContext.class);
        when(mockTopologyContext.getThisTaskIndex()).thenReturn(taskId);

        // Create recorder and call open.
        final StormRecorder recorder = new StormRecorder();
        recorder.open(spoutConfig, mockTopologyContext);

        // Validate
        verify(mockTopologyContext, never()).getThisTaskIndex();
        assertEquals("Should have empty prefix", "", recorder.getMetricPrefix());
        assertTrue("Should have empty prefix", recorder.getMetricPrefix().isEmpty());
    }

    /**
     * Validate that you can disable taskId prefixing.
     */
    @Test
    public void testOpen_taskIdPrefixFallBackInvalidInput() {
        // Define taskId in mock
        final int taskId = 20;

        // Create empty config
        final Map<String, Object> config = new HashMap<>();
        config.put(SpoutConfig.METRICS_RECORDER_ENABLE_TASK_ID_PREFIX, 22);

        // Create Spout Config
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Create mock TopologyContext
        final TopologyContext mockTopologyContext = mock(TopologyContext.class);
        when(mockTopologyContext.getThisTaskIndex()).thenReturn(taskId);

        // Create recorder and call open.
        final StormRecorder recorder = new StormRecorder();
        recorder.open(spoutConfig, mockTopologyContext);

        // Validate
        verify(mockTopologyContext, never()).getThisTaskIndex();
        assertEquals("Should have empty prefix", "", recorder.getMetricPrefix());
        assertTrue("Should have empty prefix", recorder.getMetricPrefix().isEmpty());
    }

    /**
     * Unfortunately this test kind of tests the implementation logic of start and stop timer to a degree.
     * I'm less interested in knowing that MultiReducedMetric and AssignableMetric store values correctly,
     * and more interested in know it updates the two metrics as it should.
     */
    @Test
    public void testStartAndStopTimer() throws InterruptedException {
        // Define inputs
        final CustomMetric metricDefinition = new CustomMetric("MyContext.MyMetric");
        final String expectedMetricName = "MyContext.MyMetric";
        final String expectedTotalTimeMetricName = expectedMetricName + "_totalTimeMs";

        // Create empty config
        final SpoutConfig spoutConfig = new SpoutConfig();

        // Create mock TopologyContet
        final MockTopologyContext mockTopologyContext = new MockTopologyContext();

        // Create recorder and call open.
        final StormRecorder recorder = new StormRecorder();
        recorder.open(spoutConfig, mockTopologyContext);

        // Lets capture the metrics
        final MultiReducedMetric timerMetrics = (MultiReducedMetric) mockTopologyContext.getRegisteredMetricByName("TIMERS");
        final MultiCountMetric counterMetrics = (MultiCountMetric) mockTopologyContext.getRegisteredMetricByName("COUNTERS");

        // Lets time something.  It's ok if this ends up being 0.
        recorder.startTimer(metricDefinition);
        recorder.stopTimer(metricDefinition);

        // Validate
        final Map<String, Long> timerValues = (Map<String, Long>) timerMetrics.getValueAndReset();
        assertEquals("Should have 1 value", 1, timerValues.size());
        assertTrue("Should contain our key", timerValues.containsKey(expectedMetricName));

        // Validate total timer got updated
        final Map<String, Long> counterValues = (Map<String, Long>) counterMetrics.getValueAndReset();
        assertEquals("Should have 1 value", 1, counterValues.size());
        assertTrue("Should contain our key", counterValues.containsKey(expectedTotalTimeMetricName));
    }
}