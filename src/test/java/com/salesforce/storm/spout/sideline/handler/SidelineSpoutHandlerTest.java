package com.salesforce.storm.spout.sideline.handler;

import com.salesforce.storm.spout.sideline.DynamicSpout;
import com.salesforce.storm.spout.sideline.SidelineVirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.VirtualSpout;
import com.salesforce.storm.spout.sideline.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.filter.StaticMessageFilter;
import com.salesforce.storm.spout.sideline.persistence.InMemoryPersistenceAdapter;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.trigger.NoopStartingStoppingTrigger;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import com.salesforce.storm.spout.sideline.trigger.StartingTrigger;
import com.salesforce.storm.spout.sideline.trigger.StoppingTrigger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;

public class SidelineSpoutHandlerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Test the open method properly stores the spout's config
     */
    @Test
    public void testOpen() {
        final Map<String, Object> config = new HashMap<>();
        config.put(SidelineSpoutConfig.PERSISTENCE_ADAPTER_CLASS, InMemoryPersistenceAdapter.class.getName());

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
        final Map<String, Object> config = new HashMap<>();
        config.put(SidelineSpoutConfig.CONSUMER_ID_PREFIX, "VirtualSpoutPrefix");

        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(config);

        final DynamicSpout spout = Mockito.mock(DynamicSpout.class);
        Mockito.when(spout.getPersistenceAdapter()).thenReturn(persistenceAdapter);

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);
        sidelineSpoutHandler.onSpoutOpen(spout, null, null);

        assertNotNull(sidelineSpoutHandler.getFireHoseSpout());
    }

    /**
     * Test upon spout opening sidelines, both starts and stops are resumed properly.
     */
    @Test
    public void testOnSpoutOpenResumesSidelines() {
        final Map<String, Object> config = new HashMap<>();
        config.put(SidelineSpoutConfig.CONSUMER_ID_PREFIX, "VirtualSpoutPrefix");
        config.put(SidelineSpoutConfig.KAFKA_TOPIC, "KafkaTopic");

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
        Answer<Void> addVirtualSpoutAnswer = invocation -> {
            VirtualSpout virtualSpout = invocation.getArgumentAt(0, VirtualSpout.class);
            sidelineSpouts.add(virtualSpout);
            return null;
        };

        final DynamicSpout spout = Mockito.mock(DynamicSpout.class);
        Mockito.when(spout.getPersistenceAdapter()).thenReturn(persistenceAdapter);
        Mockito.doAnswer(addVirtualSpoutAnswer).when(spout).addVirtualSpout(
            Matchers.<VirtualSpout>any()
        );

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);
        sidelineSpoutHandler.onSpoutOpen(spout, null, null);

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
            (VirtualSpout virtualSpout, VirtualSpout virtualSpout2) ->
                    ((SidelineVirtualSpoutIdentifier) virtualSpout.getVirtualSpoutId()).getSidelineRequestIdentifier().toString().equals("mail")
                        ? virtualSpout : virtualSpout2
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
     * Test upon spout closing the triggers are cleaned up.
     */
    @Test
    public void testOnSpoutClose() {
        final Map<String, Object> config = new HashMap<>();
        config.put(SidelineSpoutConfig.CONSUMER_ID_PREFIX, "VirtualSpoutPrefix");
        config.put(SidelineSpoutConfig.STARTING_TRIGGER_CLASS, NoopStartingStoppingTrigger.class.getName());
        config.put(SidelineSpoutConfig.STOPPING_TRIGGER_CLASS, NoopStartingStoppingTrigger.class.getName());

        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(config);

        final DynamicSpout spout = Mockito.mock(DynamicSpout.class);
        Mockito.when(spout.getPersistenceAdapter()).thenReturn(persistenceAdapter);

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);
        sidelineSpoutHandler.onSpoutOpen(spout, null, null);

        assertNotNull(sidelineSpoutHandler.getStartingTrigger());
        assertTrue(sidelineSpoutHandler.getStartingTrigger() instanceof NoopStartingStoppingTrigger);
        assertNotNull(sidelineSpoutHandler.getStoppingTrigger());
        assertTrue(sidelineSpoutHandler.getStoppingTrigger() instanceof NoopStartingStoppingTrigger);

        sidelineSpoutHandler.onSpoutClose();

        assertNull(sidelineSpoutHandler.getStartingTrigger());
        assertNull(sidelineSpoutHandler.getStoppingTrigger());
    }

    /**
     * Test that we get the configured starting trigger
     */
    @Test
    public void testCreateStartingTrigger() {
        final Map<String, Object> config = new HashMap<>();
        config.put(SidelineSpoutConfig.STARTING_TRIGGER_CLASS, NoopStartingStoppingTrigger.class.getName());

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);

        StartingTrigger startingTrigger = sidelineSpoutHandler.createStartingTrigger();

        assertTrue(startingTrigger instanceof NoopStartingStoppingTrigger);
    }

    /**
     * Test that we get null back when no triggers are configured
     */
    @Test
    public void testNullCreateStartingTrigger() {
        final Map<String, Object> config = new HashMap<>();

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);

        StartingTrigger startingTrigger = sidelineSpoutHandler.createStartingTrigger();

        assertNull(startingTrigger);
    }

    /**
     * Test that we get a runtime exception when we configure a class that doesn't utilize our interfaces
     */
    @Test
    public void testMisconfiguredCreateStartingTrigger() {
        final Map<String, Object> config = new HashMap<>();
        // This is not a valid trigger!
        config.put(SidelineSpoutConfig.STARTING_TRIGGER_CLASS, SidelineSpoutHandlerTest.class.getName());

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);

        expectedException.expect(RuntimeException.class);

        StartingTrigger startingTrigger = sidelineSpoutHandler.createStartingTrigger();
    }

    /**
     * Test that we get the configured stopping trigger.
     */
    @Test
    public void testCreateStoppingTrigger() {
        final Map<String, Object> config = new HashMap<>();
        config.put(SidelineSpoutConfig.STOPPING_TRIGGER_CLASS, NoopStartingStoppingTrigger.class.getName());

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);

        StoppingTrigger stoppingTrigger = sidelineSpoutHandler.createStoppingTrigger();

        assertTrue(stoppingTrigger instanceof NoopStartingStoppingTrigger);
    }

    /**
     * Test that we get null back when no triggers are configured
     */
    @Test
    public void testNullCreateStoppingTrigger() {
        final Map<String, Object> config = new HashMap<>();

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);

        StoppingTrigger stoppingTrigger = sidelineSpoutHandler.createStoppingTrigger();

        assertNull(stoppingTrigger);
    }

    /**
     * Test that we get a runtime exception when we configure a class that doesn't utilize our interfaces
     */
    @Test
    public void testMisconfiguredCreateStoppingTrigger() {
        final Map<String, Object> config = new HashMap<>();
        // This is not a valid trigger!
        config.put(SidelineSpoutConfig.STOPPING_TRIGGER_CLASS, SidelineSpoutHandlerTest.class.getName());

        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);

        expectedException.expect(RuntimeException.class);

        StoppingTrigger stoppingTrigger = sidelineSpoutHandler.createStoppingTrigger();
    }

    /**
     * Test that the handler creates proper virtual spout identifiers.
     */
    @Test
    public void testGenerateVirtualSpoutId() {
        final String expectedPrefix = "VirtualSpoutPrefix";
        final SidelineRequestIdentifier expectedSidelineRequestIdentifier = new SidelineRequestIdentifier("SidelineRequestIdentifier");

        // Create our config, specify the consumer id because it will be used as a prefix
        final Map<String, Object> config = new HashMap<>();
        config.put(SidelineSpoutConfig.CONSUMER_ID_PREFIX, expectedPrefix);

        // Create a persistence adapter, this is called in the handler onSpoutOpen() method, we're just trying to avoid a NullPointer here
        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(config);

        // Mock our core DynamicSpout, which is called from the handler
        final DynamicSpout spout = Mockito.mock(DynamicSpout.class);
        // Again, trying to avoid NullPointer's here
        Mockito.when(spout.getPersistenceAdapter()).thenReturn(persistenceAdapter);

        // Create our handler
        final SidelineSpoutHandler sidelineSpoutHandler = new SidelineSpoutHandler();
        sidelineSpoutHandler.open(config);
        sidelineSpoutHandler.onSpoutOpen(spout, null, null);

        VirtualSpoutIdentifier virtualSpoutIdentifier = sidelineSpoutHandler.generateVirtualSpoutId(expectedSidelineRequestIdentifier);

        assertTrue(virtualSpoutIdentifier instanceof SidelineVirtualSpoutIdentifier);

        SidelineVirtualSpoutIdentifier sidelineVirtualSpoutIdentifier = (SidelineVirtualSpoutIdentifier) virtualSpoutIdentifier;

        assertEquals(expectedPrefix, sidelineVirtualSpoutIdentifier.getPrefix());
        assertEquals(expectedSidelineRequestIdentifier, sidelineVirtualSpoutIdentifier.getSidelineRequestIdentifier());
    }
}