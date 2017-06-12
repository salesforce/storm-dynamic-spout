package com.salesforce.storm.spout.sideline.handler;

import com.salesforce.storm.spout.sideline.DynamicSpout;
import com.salesforce.storm.spout.sideline.SidelineVirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.persistence.InMemoryPersistenceAdapter;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class SidelineSpoutHandlerTest {

    /**
     * Test that the handler creates proper virtual spout identifiers.
     */
    @Test
    public void testGenerateVirtualSpoutId() {
        final String expectedPrefix = "VirtualSpoutPrefix";
        final SidelineRequestIdentifier expectedSidelineRequestIdentifier = new SidelineRequestIdentifier("SidelineRequestIdentifier");

        // Create our config missing the consumerIdPrefix
        final Map<String, Object> config = new HashMap<>();
        config.put(SidelineSpoutConfig.CONSUMER_ID_PREFIX, expectedPrefix);

        final PersistenceAdapter persistenceAdapter = new InMemoryPersistenceAdapter();
        persistenceAdapter.open(config);

        final DynamicSpout spout = Mockito.mock(DynamicSpout.class);
        Mockito.when(spout.getSpoutConfig()).thenReturn(config);
        Mockito.when(spout.getPersistenceAdapter()).thenReturn(persistenceAdapter);

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