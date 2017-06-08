package com.salesforce.storm.spout.sideline.consumer;

import org.junit.Test;

import static org.junit.Assert.*;

public class ConsumerPeerContextTest {

    /**
     * Verifies total instances property.
     */
    @Test
    public void getTotalInstances() {
        final int expectedInstances = 23;
        final int expectedInstanceNumber = 45;
        final ConsumerPeerContext cpc = new ConsumerPeerContext(expectedInstances, expectedInstanceNumber);
        assertEquals(expectedInstances, cpc.getTotalInstances());
    }

    /**
     * Verifies instance number property.
     */
    @Test
    public void getInstanceNumber() {
        final int expectedInstances = 23;
        final int expectedInstanceNumber = 45;
        final ConsumerPeerContext cpc = new ConsumerPeerContext(expectedInstances, expectedInstanceNumber);
        assertEquals(expectedInstanceNumber, cpc.getInstanceNumber());
    }

}