package com.salesforce.storm.spout.sideline;

import org.junit.Test;

import static org.junit.Assert.*;

public class VirtualSpoutIdentifierTest {

    /**
     * Test that two identifiers created with the same string match
     * @throws Exception Bad identifier
     */
    @Test
    public void test_toString_and_equals() throws Exception {
        final VirtualSpoutIdentifier virtualSpoutIdentifier = new VirtualSpoutIdentifier("FooBar");

        assertEquals(virtualSpoutIdentifier, new VirtualSpoutIdentifier("FooBar"));
        assertEquals(virtualSpoutIdentifier.toString(), new VirtualSpoutIdentifier("FooBar").toString());
    }

    /**
     * Test that two identifiers that are different do not match
     * @throws Exception Bad identifier
     */
    @Test
    public void test_not_toString_and_equals() throws Exception {
        final VirtualSpoutIdentifier virtualSpoutIdentifier1 = new VirtualSpoutIdentifier("Foo");
        final VirtualSpoutIdentifier virtualSpoutIdentifier2 = new VirtualSpoutIdentifier("Bar");

        assertNotEquals(virtualSpoutIdentifier1, virtualSpoutIdentifier2);
        assertNotEquals(virtualSpoutIdentifier1.toString(), virtualSpoutIdentifier2.toString());
    }

    /**
     * Test that supplying null will throw an exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void test_nullIdentifier() {
        new VirtualSpoutIdentifier(null);
    }

    /**
     * Test that supplying an empty string will throw an exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void test_emptyIdentifier() {
        new VirtualSpoutIdentifier("");
    }
}