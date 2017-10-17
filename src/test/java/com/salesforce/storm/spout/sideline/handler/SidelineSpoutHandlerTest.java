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

import com.salesforce.storm.spout.dynamic.DynamicSpout;
import com.salesforce.storm.spout.dynamic.FactoryManager;
import com.salesforce.storm.spout.sideline.SidelineVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.VirtualSpout;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.kafka.KafkaConsumerConfig;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.consumer.MockConsumer;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;
import com.salesforce.storm.spout.sideline.filter.NegatingFilterChainStep;
import com.salesforce.storm.spout.sideline.filter.StaticMessageFilter;
import com.salesforce.storm.spout.dynamic.metrics.LogRecorder;
import com.salesforce.storm.spout.dynamic.mocks.MockTopologyContext;
import com.salesforce.storm.spout.dynamic.persistence.InMemoryPersistenceAdapter;
import com.salesforce.storm.spout.dynamic.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.persistence.SidelinePayload;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import com.salesforce.storm.spout.sideline.trigger.StaticTrigger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test that {@link SidelineSpoutHandler} opens, closes  and resumes sidelines correctly.
 */
public class SidelineSpoutHandlerTest {

    /**
     * Test the open method properly stores the spout's config.
     */
    @Test
    public void testOpen() {
        final Map<String, Object> config = SpoutConfig.setDefaults(new HashMap<>());
        config.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, InMemoryPersistenceAdapter.class.getName());

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);

        assertEquals(
            config,
            sidelineSpoutHandler.getSpoutConfig()
        );
    }

    /**
     * Test upon spout opening that we have a firehose virtual spout instance.
     */
    @Test
    public void testOnSpoutOpenCreatesFirehose() {
        final Map<String, Object> config = SpoutConfig.setDefaults(new HashMap<>());
        config.put(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX, "VirtualSpoutPrefix");

        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(config);

        final DynamicSpout spout = Mockito.mock(DynamicSpout.class);
        Mockito.when(spout.getPersistenceAdapter()).thenReturn(persistenceAdapter);
        Mockito.when(spout.getFactoryManager()).thenReturn(new FactoryManager(config));

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);
        sidelineSpoutHandler.onSpoutOpen(spout, new HashMap(), new MockTopologyContext());

        assertNotNull(sidelineSpoutHandler.getFireHoseSpout());
    }

    /**
     * Test upon spout opening sidelines, both starts and stops are resumed properly.
     */
    @Test
    public void testOnSpoutOpenResumesSidelines() {
        final Map<String, Object> config = SpoutConfig.setDefaults(new HashMap<>());
        config.put(KafkaConsumerConfig.CONSUMER_ID_PREFIX, "VirtualSpoutPrefix");
        config.put(KafkaConsumerConfig.KAFKA_TOPIC, "KafkaTopic");

        final SidelineRequestIdentifier startRequestId = new SidelineRequestIdentifier("StartRequest");
        final StaticMessageFilter startFilter = new StaticMessageFilter();
        final SidelineRequest startRequest = new SidelineRequest(startRequestId, startFilter);

        final SidelineRequestIdentifier stopRequestId = new SidelineRequestIdentifier("StopRequest");
        final StaticMessageFilter stopFilter = new StaticMessageFilter();
        final SidelineRequest stopRequest = new SidelineRequest(stopRequestId, stopFilter);

        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(config);
        // Make a starting request that we expect to resume
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            startRequestId,
            startRequest,
            0,
            1L,
            2L
        );
        // Make a stopping request that we expect to resume
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.STOP,
            stopRequestId,
            stopRequest,
            1,
            3L,
            4L
        );

        final List<VirtualSpout> sidelineSpouts = new ArrayList<>();

        // We use this to replace DynamicSpout.addVirtualSpout() so that we can hijack the list of vspouts and validate
        // the sidelines.
        final Answer<Void> addVirtualSpoutAnswer = invocation -> {
            final VirtualSpout virtualSpout = invocation.getArgumentAt(0, VirtualSpout.class);
            sidelineSpouts.add(virtualSpout);
            return null;
        };

        final DynamicSpout spout = Mockito.mock(DynamicSpout.class);
        Mockito.when(spout.getPersistenceAdapter()).thenReturn(persistenceAdapter);
        Mockito.when(spout.getFactoryManager()).thenReturn(new FactoryManager(config));
        Mockito.doAnswer(addVirtualSpoutAnswer).when(spout).addVirtualSpout(
            Matchers.<VirtualSpout>any()
        );

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);
        sidelineSpoutHandler.onSpoutOpen(spout, new HashMap(), new MockTopologyContext());

        // Make sure we have a firehose
        assertNotNull(sidelineSpoutHandler.getFireHoseSpout());

        // Our firehose should have gotten a filter chain step on resume
        assertEquals(1, sidelineSpoutHandler.getFireHoseSpout().getFilterChain().getSteps().size());
        // When we fetch that step by startRequestId it should be our start filter
        assertEquals(
            startFilter,
            sidelineSpoutHandler.getFireHoseSpout().getFilterChain().getSteps().get(startRequestId)
        );

        // Two spouts, one firehose and one sideline
        assertEquals(2, sidelineSpouts.size());

        // Find our sideline spout
        Optional<VirtualSpout> sidelineSpout = sidelineSpouts.stream().reduce(
            (VirtualSpout virtualSpout, VirtualSpout virtualSpout2) -> {
                return !virtualSpout.getVirtualSpoutId().toString().contains("main") ? virtualSpout : virtualSpout2;
            }
        );

        // Make sure that we got it, it was an optional after all
        assertTrue(sidelineSpout.isPresent());

        // Make sure the sideline spout has one filter
        assertEquals(1, sidelineSpout.get().getFilterChain().getSteps().size());
        // Make sure that one filter is our stop request it
        assertEquals(
            stopFilter,
            sidelineSpout.get().getFilterChain().getSteps().get(stopRequestId)
        );
    }

    /**
     * Test that when start sidelining is called the firehose gets a new filter from the sideline request.
     */
    @Test
    public void testStartSidelining() {
        final Map<String, Object> config = SpoutConfig.setDefaults(new HashMap<>());
        config.put(KafkaConsumerConfig.CONSUMER_ID_PREFIX, "VirtualSpoutPrefix");
        config.put(KafkaConsumerConfig.KAFKA_TOPIC, "KafkaTopic");
        config.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, InMemoryPersistenceAdapter.class.getName());
        config.put(SpoutConfig.CONSUMER_CLASS, MockConsumer.class.getName());

        final SidelineRequestIdentifier startRequestId = new SidelineRequestIdentifier("StartRequest");
        final StaticMessageFilter startFilter = new StaticMessageFilter();
        final SidelineRequest startRequest = new SidelineRequest(startRequestId, startFilter);

        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(config);

        final DynamicSpout spout = Mockito.mock(DynamicSpout.class);
        Mockito.when(spout.getPersistenceAdapter()).thenReturn(persistenceAdapter);
        Mockito.when(spout.getFactoryManager()).thenReturn(new FactoryManager(config));
        Mockito.when(spout.getMetricsRecorder()).thenReturn(new LogRecorder());

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);
        sidelineSpoutHandler.onSpoutOpen(spout, new HashMap(), new MockTopologyContext());

        // Normally the SpoutCoordinator calls this, but we don't have a SpoutCoordinator so we're doing it ourselves
        sidelineSpoutHandler.getFireHoseSpout().open();

        // Tell our mock consumer that these are the partitions we're working with
        MockConsumer.partitions = Arrays.asList(0, 5);

        // Finally, start a sideline with out given request
        sidelineSpoutHandler.startSidelining(startRequest);

        // Make sure we have a firehose
        assertNotNull(sidelineSpoutHandler.getFireHoseSpout());

        // Our firehose should have gotten a filter chain step on resume
        assertEquals(1, sidelineSpoutHandler.getFireHoseSpout().getFilterChain().getSteps().size());
        // When we fetch that step by startRequestId it should be our start filter
        assertEquals(
            startFilter,
            sidelineSpoutHandler.getFireHoseSpout().getFilterChain().getSteps().get(startRequestId)
        );

        final SidelinePayload partition0 = persistenceAdapter.retrieveSidelineRequest(startRequestId, 0);

        assertEquals(SidelineType.START, partition0.type);
        assertEquals(startRequestId, partition0.id);
        assertEquals(startRequest, partition0.request);
        assertEquals(Long.valueOf(1L), partition0.startingOffset);
        assertNull(partition0.endingOffset);

        final SidelinePayload partition5 = persistenceAdapter.retrieveSidelineRequest(startRequestId, 5);

        assertEquals(SidelineType.START, partition5.type);
        assertEquals(startRequestId, partition5.id);
        assertEquals(startRequest, partition5.request);
        assertEquals(Long.valueOf(1L), partition5.startingOffset);
        assertNull(partition5.endingOffset);
    }

    /**
     * Test that when a sideline is stopped, the filter is removed from the firehose and a virtual spout
     * is spun up with the negated filter and correct offsets.
     */
    @Test
    public void testStopSidelining() {
        final Map<String, Object> config = SpoutConfig.setDefaults(new HashMap<>());
        config.put(KafkaConsumerConfig.CONSUMER_ID_PREFIX, "VirtualSpoutPrefix");
        config.put(KafkaConsumerConfig.KAFKA_TOPIC, "KafkaTopic");
        config.put(SpoutConfig.PERSISTENCE_ADAPTER_CLASS, InMemoryPersistenceAdapter.class.getName());
        config.put(SpoutConfig.CONSUMER_CLASS, MockConsumer.class.getName());

        final SidelineRequestIdentifier stopRequestId = new SidelineRequestIdentifier("StopRequest");
        final StaticMessageFilter stopFilter = new StaticMessageFilter();
        final SidelineRequest stopRequest = new SidelineRequest(stopRequestId, stopFilter);

        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(config);

        final DynamicSpout spout = Mockito.mock(DynamicSpout.class);
        Mockito.when(spout.getPersistenceAdapter()).thenReturn(persistenceAdapter);
        Mockito.when(spout.getFactoryManager()).thenReturn(new FactoryManager(config));
        Mockito.when(spout.getMetricsRecorder()).thenReturn(new LogRecorder());

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);
        sidelineSpoutHandler.onSpoutOpen(spout, new HashMap(), new MockTopologyContext());

        // Persist our stop request as a start, so that when we stop it, it can be found
        // Note that we are doing this AFTER the spout has opened because we are NOT testing the resume logic
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            stopRequestId,
            stopRequest,
            0, // partition
            1L, // starting offset
            null // ending offset
        );
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            stopRequestId,
            stopRequest,
            5, // partition
            3L, // starting offset
            null // ending offset
        );

        // Normally the SpoutCoordinator calls this, but we don't have a SpoutCoordinator so we're doing it ourselves
        sidelineSpoutHandler.getFireHoseSpout().open();

        // Stick the filter onto the fire hose, it should be removed when we stop the sideline request
        sidelineSpoutHandler.getFireHoseSpout().getFilterChain().addStep(stopRequestId, stopFilter);

        // Tell our mock consumer that these are the partitions we're working with
        MockConsumer.partitions = Arrays.asList(0, 5);

        // Finally, start a sideline with out given request
        sidelineSpoutHandler.stopSidelining(stopRequest);

        // Firehose no longer has any filters
        assertEquals(
            0,
            sidelineSpoutHandler.getFireHoseSpout().getFilterChain().getSteps().size()
        );

        final SidelinePayload partition0 = persistenceAdapter.retrieveSidelineRequest(stopRequestId, 0);

        assertEquals(SidelineType.STOP, partition0.type);
        assertEquals(stopRequestId, partition0.id);
        assertEquals(new NegatingFilterChainStep(stopRequest.step), partition0.request.step);
        assertEquals(Long.valueOf(1L), partition0.startingOffset);
        assertEquals(Long.valueOf(1L), partition0.endingOffset);

        final SidelinePayload partition5 = persistenceAdapter.retrieveSidelineRequest(stopRequestId, 5);

        assertEquals(SidelineType.STOP, partition5.type);
        assertEquals(stopRequestId, partition5.id);
        assertEquals(new NegatingFilterChainStep(stopRequest.step), partition5.request.step);
        assertEquals(Long.valueOf(3L), partition5.startingOffset);
        assertEquals(Long.valueOf(1L), partition5.endingOffset);
    }

    /**
     * Test upon spout closing the triggers are cleaned up.
     */
    @Test
    public void testOnSpoutClose() {
        final Map<String, Object> config = SpoutConfig.setDefaults(new HashMap<>());
        config.put(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX, "VirtualSpoutPrefix");
        config.put(SidelineConfig.TRIGGER_CLASS, StaticTrigger.class.getName());

        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(config);

        final DynamicSpout spout = Mockito.mock(DynamicSpout.class);
        Mockito.when(spout.getPersistenceAdapter()).thenReturn(persistenceAdapter);
        Mockito.when(spout.getFactoryManager()).thenReturn(new FactoryManager(config));

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);
        sidelineSpoutHandler.onSpoutOpen(spout, new HashMap(), new MockTopologyContext());

        assertNotNull(sidelineSpoutHandler.getSidelineTriggers());
        assertEquals(1, sidelineSpoutHandler.getSidelineTriggers().size());
        assertTrue(sidelineSpoutHandler.getSidelineTriggers().get(0) instanceof StaticTrigger);

        sidelineSpoutHandler.onSpoutClose(spout);

        assertEquals(0, sidelineSpoutHandler.getSidelineTriggers().size());
    }

    @Rule
    public ExpectedException expectedExceptionMisconfiguredCreateStartingTrigger = ExpectedException.none();

    /**
     * Test that we get a runtime exception when we configure a class that doesn't utilize our interfaces.
     */
    @Test
    public void testMisconfiguredCreateSidelineTriggers() {
        final Map<String, Object> config = SpoutConfig.setDefaults(new HashMap<>());
        // This class better not exist!
        config.put(SidelineConfig.TRIGGER_CLASS, "FooBar" + System.currentTimeMillis());

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);

        expectedExceptionMisconfiguredCreateStartingTrigger.expect(RuntimeException.class);

        sidelineSpoutHandler.createSidelineTriggers();
    }

    /**
     * Test that the handler creates proper virtual spout identifiers.
     */
    @Test
    public void testGenerateVirtualSpoutId() {
        final String expectedPrefix = "VirtualSpoutPrefix";
        final SidelineRequestIdentifier expectedSidelineRequestIdentifier = new SidelineRequestIdentifier("SidelineRequestIdentifier");

        // Create our config, specify the consumer id because it will be used as a prefix
        final Map<String, Object> config = SpoutConfig.setDefaults(new HashMap<>());
        config.put(SpoutConfig.VIRTUAL_SPOUT_ID_PREFIX, expectedPrefix);

        // Create a persistence adapter, this is called in the handler onSpoutOpen() method, we're just trying to avoid a NullPointer here
        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(config);

        // Mock our core DynamicSpout, which is called from the handler
        final DynamicSpout spout = Mockito.mock(DynamicSpout.class);
        // Again, trying to avoid NullPointer's here
        Mockito.when(spout.getPersistenceAdapter()).thenReturn(persistenceAdapter);
        Mockito.when(spout.getFactoryManager()).thenReturn(new FactoryManager(config));

        // Create our handler
        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);
        sidelineSpoutHandler.onSpoutOpen(spout, new HashMap(), new MockTopologyContext());

        final VirtualSpoutIdentifier virtualSpoutIdentifier = sidelineSpoutHandler.generateSidelineVirtualSpoutId(
            expectedSidelineRequestIdentifier
        );

        assertTrue(virtualSpoutIdentifier instanceof SidelineVirtualSpoutIdentifier);

        final SidelineVirtualSpoutIdentifier sidelineVirtualSpoutIdentifier = (SidelineVirtualSpoutIdentifier) virtualSpoutIdentifier;

        assertEquals(expectedPrefix, sidelineVirtualSpoutIdentifier.getConsumerId());
        assertEquals(expectedSidelineRequestIdentifier, sidelineVirtualSpoutIdentifier.getSidelineRequestIdentifier());
    }
}