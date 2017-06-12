package com.salesforce.storm.spout.sideline.handler;

import com.salesforce.storm.spout.sideline.SidelineVirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.consumer.MockConsumer;
import com.salesforce.storm.spout.sideline.filter.StaticMessageFilter;
import com.salesforce.storm.spout.sideline.mocks.MockDelegateSpout;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.persistence.SidelinePayload;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.*;

public class SidelineVirtualSpoutHandlerTest {

    /**
     * Test that upon completion of a virtual spout, the sideline state is properly cleaned up
     */
    @Test
    public void testOnVirtualSpoutCompletion() {
        final String prefix = "MyVirtualSpout";
        final SidelineRequestIdentifier sidelineRequestIdentifier = new SidelineRequestIdentifier("SidelineRequest");
        final SidelineRequest sidelineRequest = new SidelineRequest(sidelineRequestIdentifier, new StaticMessageFilter());

        final SidelineVirtualSpoutIdentifier sidelineVirtualSpoutIdentifier = new SidelineVirtualSpoutIdentifier(
            prefix,
            sidelineRequestIdentifier
        );

        final MockDelegateSpout mockDelegateSpout = new MockDelegateSpout(sidelineVirtualSpoutIdentifier);
        final PersistenceAdapter persistenceAdapter = mockDelegateSpout.getConsumer().getPersistenceAdapter();
        persistenceAdapter.open(new HashMap());

        // Persist some stopping requests that we cleanup upon completion
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.STOP,
            sidelineRequestIdentifier,
            sidelineRequest,
            0, // partition
            1L, // starting offset
            1L // ending offset
        );
        persistenceAdapter.persistSidelineRequestState(
            SidelineType.START,
            sidelineRequestIdentifier,
            sidelineRequest,
            5, // partition
            3L, // starting offset
            2L // ending offset
        );

        MockConsumer.partitions = Arrays.asList(0, 5);

        final SidelineVirtualSpoutHandler sidelineVirtualSpoutHandler = new SidelineVirtualSpoutHandler();

        // Complete the sideline
        sidelineVirtualSpoutHandler.onVirtualSpoutCompletion(mockDelegateSpout);

        // Do we still have a record for partition 0?
        SidelinePayload partition0 = persistenceAdapter.retrieveSidelineRequest(sidelineRequestIdentifier, 0);

        assertNull(partition0);

        // Do we still have a record for partition 5?
        SidelinePayload partition5 = persistenceAdapter.retrieveSidelineRequest(sidelineRequestIdentifier, 5);

        assertNull(partition5);
    }
}