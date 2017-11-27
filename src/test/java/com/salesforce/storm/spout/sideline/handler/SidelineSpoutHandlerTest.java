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

package com.salesforce.storm.spout.sideline.handler;

import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import com.salesforce.storm.spout.dynamic.DynamicSpout;
import com.salesforce.storm.spout.dynamic.FactoryManager;
import com.salesforce.storm.spout.dynamic.VirtualSpoutFactory;
import com.salesforce.storm.spout.dynamic.metrics.LogRecorder;
import com.salesforce.storm.spout.sideline.SidelineVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.VirtualSpout;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.kafka.KafkaConsumerConfig;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.mocks.MockConsumer;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;
import com.salesforce.storm.spout.dynamic.filter.NegatingFilterChainStep;
import com.salesforce.storm.spout.dynamic.filter.StaticMessageFilter;
import com.salesforce.storm.spout.dynamic.mocks.MockTopologyContext;
import com.salesforce.storm.spout.sideline.persistence.InMemoryPersistenceAdapter;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.persistence.SidelinePayload;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import com.salesforce.storm.spout.sideline.trigger.StaticTrigger;
import org.apache.storm.task.TopologyContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test that {@link SidelineSpoutHandler} opens, closes  and resumes sidelines correctly.
 */
public class SidelineSpoutHandlerTest {

    private static String CONSUMER_ID_PREFIX = "VirtualSpoutPrefix";

    /**
     * Test the open method properly stores the spout's config.
     */
    @Test
    public void testOpen() {
        final Map<String, Object> config = getConfig();
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        final VirtualSpoutFactory virtualSpoutFactory = getVirtualSpoutFactory(config);

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(spoutConfig, virtualSpoutFactory);

        assertEquals(
            spoutConfig,
            sidelineSpoutHandler.getSpoutConfig()
        );

        // close things out.
        sidelineSpoutHandler.close();
    }

    /**
     * Test upon spout opening that we have a firehose virtual spout instance.
     */
    @Test
    public void testOnSpoutOpenCreatesFirehose() {
        final Map<String, Object> config = getConfig();
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(config);

        final DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(null, new MockTopologyContext(), null);

        final VirtualSpoutFactory virtualSpoutFactory = getVirtualSpoutFactory(config);

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(spoutConfig, virtualSpoutFactory);
        sidelineSpoutHandler.onSpoutOpen(spout, new HashMap(), new MockTopologyContext());

        assertNotNull(sidelineSpoutHandler.getFireHoseSpout());

        // close things out.
        sidelineSpoutHandler.close();
        spout.close();
    }

    /**
     * Test upon spout opening sidelines, both starts and stops are resumed properly.
     *
     * This test has one started sideline and one stopped sideline, both of which should resume correctly.
     */
    @Test
    public void testOnSpoutOpenResumesSidelines() {
        final Map<String, Object> config = getConfig();
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        final String namespace = "Test";

        final SidelineRequestIdentifier startRequestId = new SidelineRequestIdentifier("StartRequest");
        final StaticMessageFilter startFilter = new StaticMessageFilter();
        final SidelineRequest startRequest = new SidelineRequest(startRequestId, startFilter);

        final SidelineRequestIdentifier stopRequestId = new SidelineRequestIdentifier("StopRequest");
        final StaticMessageFilter stopFilter = new StaticMessageFilter();
        final SidelineRequest stopRequest = new SidelineRequest(stopRequestId, stopFilter);
        final VirtualSpoutIdentifier virtualSpoutIdentifier2 = new SidelineVirtualSpoutIdentifier(
            CONSUMER_ID_PREFIX,
            stopRequestId
        );

        final DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(null, new MockTopologyContext(), null);

        final VirtualSpoutFactory virtualSpoutFactory = getVirtualSpoutFactory(config);

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(spoutConfig, virtualSpoutFactory);

        final PersistenceAdapter persistenceAdapter = sidelineSpoutHandler.getPersistenceAdapter();

        // Make a starting request that we expect to resume
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            startRequestId,
            startRequest,
            new ConsumerPartition(namespace, 0),
            1L,
            2L
        );
        // Make a stopping request that we expect to resume
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.STOP,
            stopRequestId,
            stopRequest,
            new ConsumerPartition(namespace, 1),
            3L,
            4L
        );

        sidelineSpoutHandler.onSpoutOpen(spout, new HashMap(), new MockTopologyContext());

        // Make sure we have a firehose
        assertNotNull("Firehose should not be null", sidelineSpoutHandler.getFireHoseSpout());

        // Our firehose should have gotten a filter chain step on resume
        assertEquals(
            "FilterChain should have one step for the start request",
            1,
            sidelineSpoutHandler.getFireHoseSpout().getFilterChain().getSteps().size()
        );
        // When we fetch that step by startRequestId it should be our start filter
        assertEquals(
            "FilterChain should have the start filter on it",
            startFilter,
            sidelineSpoutHandler.getFireHoseSpout().getFilterChain().getSteps().get(startRequestId)
        );

        assertTrue(
            "The stop request should have created a new VirtualSpout instance",
            spout.hasVirtualSpout(virtualSpoutIdentifier2)
        );

        // close things out.
        sidelineSpoutHandler.close();
        spout.close();
    }

    /**
     * Test that when start sidelining is called the firehose gets a new filter from the sideline request.
     */
    @Test
    public void testStartSidelining() {
        final Map<String, Object> config = getConfig();
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        final String namespace = MockConsumer.topic;

        final SidelineRequestIdentifier startRequestId = new SidelineRequestIdentifier("StartRequest");
        final StaticMessageFilter startFilter = new StaticMessageFilter();
        final SidelineRequest startRequest = new SidelineRequest(startRequestId, startFilter);

        final DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(null, new MockTopologyContext(), null);

        final VirtualSpoutFactory virtualSpoutFactory = getVirtualSpoutFactory(config);

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(spoutConfig, virtualSpoutFactory);

        final PersistenceAdapter persistenceAdapter = sidelineSpoutHandler.getPersistenceAdapter();

        sidelineSpoutHandler.onSpoutOpen(spout, new HashMap(), new MockTopologyContext());

        // Tell our mock consumer that these are the partitions we're working with
        MockConsumer.partitions = Arrays.asList(0, 5);

        // Finally, start a sideline with out given request
        sidelineSpoutHandler.startSidelining(startRequest);

        assertTrue(
            "Sideline should be started",
            sidelineSpoutHandler.isSidelineStarted(startRequest)
        );

        // Make sure we have a firehose
        assertNotNull(sidelineSpoutHandler.getFireHoseSpout());

        // Our firehose should have gotten a filter chain step on resume
        assertEquals(1, sidelineSpoutHandler.getFireHoseSpout().getFilterChain().getSteps().size());
        // When we fetch that step by startRequestId it should be our start filter
        assertEquals(
            startFilter,
            sidelineSpoutHandler.getFireHoseSpout().getFilterChain().getSteps().get(startRequestId)
        );

        final SidelinePayload partition0 = persistenceAdapter.retrieveSidelineRequest(startRequestId, new ConsumerPartition(namespace, 0));

        assertEquals(SidelineType.START, partition0.type);
        assertEquals(startRequestId, partition0.id);
        assertEquals(startRequest, partition0.request);
        assertEquals(Long.valueOf(1L), partition0.startingOffset);
        assertNull(partition0.endingOffset);

        final SidelinePayload partition5 = persistenceAdapter.retrieveSidelineRequest(startRequestId, new ConsumerPartition(namespace, 5));

        assertEquals(SidelineType.START, partition5.type);
        assertEquals(startRequestId, partition5.id);
        assertEquals(startRequest, partition5.request);
        assertEquals(Long.valueOf(1L), partition5.startingOffset);
        assertNull(partition5.endingOffset);

        // close things out.
        sidelineSpoutHandler.close();
        spout.close();
    }

    /**
     * Test that when a sideline is stopped, the filter is removed from the firehose and a virtual spout
     * is spun up with the negated filter and correct offsets.
     */
    @Test
    public void testStopSidelining() {
        final Map<String, Object> config = getConfig();
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        final String namespace = MockConsumer.topic;

        final SidelineRequestIdentifier stopRequestId = new SidelineRequestIdentifier("StopRequest");
        final StaticMessageFilter stopFilter = new StaticMessageFilter();
        final SidelineRequest stopRequest = new SidelineRequest(stopRequestId, stopFilter);

        final DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(null, new MockTopologyContext(), null);

        final VirtualSpoutFactory virtualSpoutFactory = getVirtualSpoutFactory(config);

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(spoutConfig, virtualSpoutFactory);

        final PersistenceAdapter persistenceAdapter = sidelineSpoutHandler.getPersistenceAdapter();

        sidelineSpoutHandler.onSpoutOpen(spout, new HashMap(), new MockTopologyContext());

        // Persist our stop request as a start, so that when we stop it, it can be found
        // Note that we are doing this AFTER the spout has opened because we are NOT testing the resume logic
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            stopRequestId,
            stopRequest,
            new ConsumerPartition(namespace, 0),
            1L, // starting offset
            null // ending offset
        );
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            stopRequestId,
            stopRequest,
            new ConsumerPartition(namespace, 5),
            3L, // starting offset
            null // ending offset
        );

        // Stick the filter onto the fire hose, it should be removed when we stop the sideline request
        sidelineSpoutHandler.getFireHoseSpout().getFilterChain().addStep(stopRequestId, stopFilter);

        // Tell our mock consumer that these are the partitions we're working with
        MockConsumer.partitions = Arrays.asList(0, 5);

        // Finally, start a sideline with out given request
        sidelineSpoutHandler.stopSidelining(stopRequest);

        assertTrue(
            "Sideline should be stopped",
            sidelineSpoutHandler.isSidelineStopped(stopRequest)
        );

        // Firehose no longer has any filters
        assertEquals(
            0,
            sidelineSpoutHandler.getFireHoseSpout().getFilterChain().getSteps().size()
        );

        final SidelinePayload partition0 = persistenceAdapter.retrieveSidelineRequest(stopRequestId, new ConsumerPartition(namespace, 0));

        assertEquals(SidelineType.STOP, partition0.type);
        assertEquals(stopRequestId, partition0.id);
        assertEquals(new NegatingFilterChainStep(stopRequest.step), partition0.request.step);
        assertEquals(Long.valueOf(1L), partition0.startingOffset);
        assertEquals(Long.valueOf(1L), partition0.endingOffset);

        final SidelinePayload partition5 = persistenceAdapter.retrieveSidelineRequest(stopRequestId, new ConsumerPartition(namespace, 5));

        assertEquals(SidelineType.STOP, partition5.type);
        assertEquals(stopRequestId, partition5.id);
        assertEquals(new NegatingFilterChainStep(stopRequest.step), partition5.request.step);
        assertEquals(Long.valueOf(3L), partition5.startingOffset);
        assertEquals(Long.valueOf(1L), partition5.endingOffset);

        // close things out.
        sidelineSpoutHandler.close();
        spout.close();
    }

    /**
     * Test upon spout closing the triggers are cleaned up.
     */
    @Test
    public void testOnSpoutClose() {
        final Map<String, Object> config = getConfig();
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(config);

        final DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(null, new MockTopologyContext(), null);

        final VirtualSpoutFactory virtualSpoutFactory = getVirtualSpoutFactory(config);

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(spoutConfig, virtualSpoutFactory);
        sidelineSpoutHandler.onSpoutOpen(spout, new HashMap(), new MockTopologyContext());

        assertNotNull(sidelineSpoutHandler.getSidelineTriggers());
        assertEquals(1, sidelineSpoutHandler.getSidelineTriggers().size());
        assertTrue(sidelineSpoutHandler.getSidelineTriggers().get(0) instanceof StaticTrigger);

        sidelineSpoutHandler.onSpoutClose(spout);

        assertEquals(0, sidelineSpoutHandler.getSidelineTriggers().size());

        // close things out.
        sidelineSpoutHandler.close();
        spout.close();
    }

    @Rule
    public ExpectedException expectedExceptionMisconfiguredCreateStartingTrigger = ExpectedException.none();

    /**
     * Test that we get a runtime exception when we configure a class that doesn't utilize our interfaces.
     */
    @Test
    public void testMisconfiguredCreateSidelineTriggers() {
        final Map<String, Object> config = getConfig();
        // Override our trigger class with one that does not actually exist.
        config.put(SidelineConfig.TRIGGER_CLASS, "FooBar" + System.currentTimeMillis());

        final SpoutConfig spoutConfig = new SpoutConfig(config);

        final VirtualSpoutFactory virtualSpoutFactory = getVirtualSpoutFactory(config);

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(spoutConfig, virtualSpoutFactory);

        expectedExceptionMisconfiguredCreateStartingTrigger.expect(RuntimeException.class);

        sidelineSpoutHandler.createSidelineTriggers();

        // close things out.
        sidelineSpoutHandler.close();
    }

    /**
     * Test that the handler creates proper virtual spout identifiers.
     */
    @Test
    public void testGenerateVirtualSpoutId() {
        final SidelineRequestIdentifier expectedSidelineRequestIdentifier = new SidelineRequestIdentifier("SidelineRequestIdentifier");

        // Create our config, specify the consumer id because it will be used as a prefix
        final Map<String, Object> config = getConfig();
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        // Create a persistence adapter, this is called in the handler onSpoutOpen() method, we're just trying to avoid a NullPointer here
        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(config);

        final DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(null, new MockTopologyContext(), null);

        final VirtualSpoutFactory virtualSpoutFactory = getVirtualSpoutFactory(config);

        // Create our handler
        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(spoutConfig, virtualSpoutFactory);
        sidelineSpoutHandler.onSpoutOpen(spout, new HashMap(), new MockTopologyContext());

        final VirtualSpoutIdentifier virtualSpoutIdentifier = sidelineSpoutHandler.generateSidelineVirtualSpoutId(
            expectedSidelineRequestIdentifier
        );

        assertTrue(virtualSpoutIdentifier instanceof SidelineVirtualSpoutIdentifier);

        final SidelineVirtualSpoutIdentifier sidelineVirtualSpoutIdentifier = (SidelineVirtualSpoutIdentifier) virtualSpoutIdentifier;

        assertEquals(CONSUMER_ID_PREFIX, sidelineVirtualSpoutIdentifier.getConsumerId());
        assertEquals(expectedSidelineRequestIdentifier, sidelineVirtualSpoutIdentifier.getSidelineRequestIdentifier());

        // close things out.
        sidelineSpoutHandler.close();
        spout.close();
    }

    /**
     * Test that periodically calling {@link SidelineSpoutHandler#loadSidelines()} will restore {@link VirtualSpout}'s and
     * {@link com.salesforce.storm.spout.dynamic.filter.FilterChainStep}'s after calling
     * {@link SidelineSpoutHandler#onSpoutOpen(DynamicSpout, Map, TopologyContext)}, and that when spouts and filters are removed
     * they are properly restored back onto the spout.
     */
    @Test
    public void testLoadSidelines() {
        final Map<String, Object> config = getConfig();
        final SpoutConfig spoutConfig = new SpoutConfig(config);

        final String namespace = "Test";

        final DynamicSpout spout = new DynamicSpout(spoutConfig);
        spout.open(null, new MockTopologyContext(), null);

        final VirtualSpoutFactory virtualSpoutFactory = getVirtualSpoutFactory(config);

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(spoutConfig, virtualSpoutFactory);
        sidelineSpoutHandler.onSpoutOpen(spout, new HashMap(), new MockTopologyContext());

        assertTrue(
            "There should not be any filters on the firehose",
            sidelineSpoutHandler.getFireHoseSpout().getFilterChain().getSteps().isEmpty()
        );

        // The firehose has to move from the queue to a SpoutRunner, and this happens in a separate thread so we wait a bit.
        await()
            .until(spout::getTotalVirtualSpouts, equalTo(1));

        assertEquals("Only the firehose should be on the spout", 1, spout.getTotalVirtualSpouts());

        final SidelineRequestIdentifier startRequestId = new SidelineRequestIdentifier("StartRequest");
        final StaticMessageFilter startFilter = new StaticMessageFilter();
        final SidelineRequest startRequest = new SidelineRequest(startRequestId, startFilter);

        final SidelineRequestIdentifier stopRequestId = new SidelineRequestIdentifier("StopRequest");
        final StaticMessageFilter stopFilter = new StaticMessageFilter();
        final SidelineRequest stopRequest = new SidelineRequest(stopRequestId, stopFilter);

        final PersistenceAdapter persistenceAdapter = sidelineSpoutHandler.getPersistenceAdapter();
        // Make a starting request that we expect to be loaded
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            startRequestId,
            startRequest,
            new ConsumerPartition(namespace, 0),
            1L,
            2L
        );
        // Make a stopping request that we expect to be loaded
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.STOP,
            stopRequestId,
            stopRequest,
            new ConsumerPartition(namespace, 1),
            3L,
            4L
        );

        final VirtualSpoutIdentifier startRequestVirtualSpoutIdentifier = new SidelineVirtualSpoutIdentifier(
            CONSUMER_ID_PREFIX,
            startRequestId
        );

        final VirtualSpoutIdentifier stopRequestVirtualSpoutIdentifier = new SidelineVirtualSpoutIdentifier(
            CONSUMER_ID_PREFIX,
            stopRequestId
        );

        // Reload the sidelines, this is normally done via a thread on an interval - we're going to validate it behaves correctly now
        sidelineSpoutHandler.loadSidelines();

        assertEquals(
            "There should be a filter on the firehose for our start request",
            1,
            sidelineSpoutHandler.getFireHoseSpout().getFilterChain().getSteps().size()
        );

        assertTrue(
            "Start request should be on the filter chain",
            sidelineSpoutHandler.getFireHoseSpout().getFilterChain().hasStep(startRequestId)
        );

        assertFalse(
            "Stop request should not be on the filter chain",
            sidelineSpoutHandler.getFireHoseSpout().getFilterChain().hasStep(stopRequestId)
        );

        // A new sideline spout should be added for our stopped request
        await()
            .until(spout::getTotalVirtualSpouts, equalTo(2));

        assertTrue(
            "Spout should have a VirtualSpout for the stop request",
            spout.hasVirtualSpout(stopRequestVirtualSpoutIdentifier)
        );

        assertFalse(
            "Spout should not have a VirtualSpout for the start request",
            spout.hasVirtualSpout(startRequestVirtualSpoutIdentifier)
        );

        // Call this again, basically we want to be able to call this without messing with state and nothing else should change,
        // so we'll validate that number of vspouts stays the same and the filter chain stays the same too.
        sidelineSpoutHandler.loadSidelines();

        assertEquals(
            "Spout should have two virtual spouts",
            2,
            spout.getTotalVirtualSpouts()
        );

        assertTrue(
            "Spout should have the firehose VirtualSpout",
            spout.hasVirtualSpout(sidelineSpoutHandler.getFireHoseSpoutIdentifier())
        );

        assertTrue(
            "Spout should have a VirtualSpout for the stop request",
            spout.hasVirtualSpout(stopRequestVirtualSpoutIdentifier)
        );

        assertFalse(
            "Spout should not have a VirtualSpout for the start request",
            spout.hasVirtualSpout(startRequestVirtualSpoutIdentifier)
        );

        assertEquals(
            "Firehose only has 1 filter",
            1,
            sidelineSpoutHandler.getFireHoseSpout().getFilterChain().getSteps().size()
        );

        // Now we're going to mess with the state of the spout and filter chain and see if things reload properly
        spout.removeVirtualSpout(stopRequestVirtualSpoutIdentifier);
        spout.removeVirtualSpout(sidelineSpoutHandler.getFireHoseSpoutIdentifier());
        sidelineSpoutHandler.getFireHoseSpout().getFilterChain().removeStep(startRequestId);

        assertFalse(
            "Spout should not have a VirtualSpout for the stop request",
            spout.hasVirtualSpout(stopRequestVirtualSpoutIdentifier)
        );

        assertFalse(
            "Spout should not have a VirtualSpout for the firehose",
            spout.hasVirtualSpout(sidelineSpoutHandler.getFireHoseSpoutIdentifier())
        );

        assertFalse(
            "Start request should not be on the filter chain",
            sidelineSpoutHandler.getFireHoseSpout().getFilterChain().hasStep(startRequestId)
        );

        // The missing spout and filter chain step should be restored after this
        sidelineSpoutHandler.loadSidelines();

        assertTrue(
            "Spout should have a VirtualSpout for the stop request",
            // Note that this will be in the starting queue, not in the runners at this point and that's OK for this test
            spout.hasVirtualSpout(stopRequestVirtualSpoutIdentifier)
        );

        assertTrue(
            "Spout should have a VirtualSpout for the firehose",
            spout.hasVirtualSpout(sidelineSpoutHandler.getFireHoseSpoutIdentifier())
        );

        assertTrue(
            "Start request should be on the filter chain",
            sidelineSpoutHandler.getFireHoseSpout().getFilterChain().hasStep(startRequestId)
        );

        // Note that I would love to mess with the filter chain on an existing VirtualSpout that isn't the FireHose, but there's no real way
        // to get to the other spouts right now. This may not even be relevant either, because we never manipulate the sideline's filter
        // chains after they've been opened (whereas we do with the firehose). In fact, it's not even possible to do because of the
        // aforementioned lack of accessibility to non-firehose spouts.
        sidelineSpoutHandler.close();
        spout.close();
    }

    private VirtualSpoutFactory getVirtualSpoutFactory(final Map<String,Object> config) {
        return new VirtualSpoutFactory(config, new MockTopologyContext(), new FactoryManager(config), new LogRecorder());
    }

    private Map<String, Object> getConfig() {
        final Map<String, Object> config = new HashMap<>();
        config.put(KafkaConsumerConfig.CONSUMER_ID_PREFIX, CONSUMER_ID_PREFIX);
        config.put(KafkaConsumerConfig.KAFKA_TOPIC, "KafkaTopic");
        config.put(
            SpoutConfig.PERSISTENCE_ADAPTER_CLASS,
            com.salesforce.storm.spout.dynamic.persistence.InMemoryPersistenceAdapter.class.getName()
        );
        config.put(
            SidelineConfig.PERSISTENCE_ADAPTER_CLASS,
            InMemoryPersistenceAdapter.class.getName()
        );
        config.put(SidelineConfig.TRIGGER_CLASS, StaticTrigger.class.getName());
        config.put(SpoutConfig.CONSUMER_CLASS, MockConsumer.class.getName());

        return config;
    }
}