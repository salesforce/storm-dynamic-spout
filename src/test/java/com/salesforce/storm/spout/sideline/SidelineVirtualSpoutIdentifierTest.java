package com.salesforce.storm.spout.sideline;

import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import org.junit.Test;

import static org.junit.Assert.*;

public class SidelineVirtualSpoutIdentifierTest {

    /**
     * Test that toString creates correct identifiers.
     * @throws Exception Null or empty data provided.
     */
    @Test
    public void test_toString() throws Exception {
        VirtualSpoutIdentifier virtualSpoutIdentifier1 = new SidelineVirtualSpoutIdentifier(
            "foo",
            new SidelineRequestIdentifier("main")
        );

        assertEquals("foo:main", virtualSpoutIdentifier1.toString());

        VirtualSpoutIdentifier virtualSpoutIdentifier2 = new SidelineVirtualSpoutIdentifier(
            "foo",
            new SidelineRequestIdentifier("bar")
        );

        assertEquals("foo:bar", virtualSpoutIdentifier2.toString());
    }

    /**
     *Test that equals checks correctly.
     * @throws Exception Null or empty data provided.
     */
    @Test
    public void test_equals() throws Exception {
        VirtualSpoutIdentifier virtualSpoutIdentifier1 = new SidelineVirtualSpoutIdentifier(
            "foo",
            new SidelineRequestIdentifier("main")
        );

        VirtualSpoutIdentifier virtualSpoutIdentifier2 = new SidelineVirtualSpoutIdentifier(
            "foo",
            new SidelineRequestIdentifier("main")
        );

        VirtualSpoutIdentifier virtualSpoutIdentifier3 = new SidelineVirtualSpoutIdentifier(
            "foo",
            new SidelineRequestIdentifier("bar")
        );

        assertTrue(virtualSpoutIdentifier1.equals(virtualSpoutIdentifier2));

        assertFalse(virtualSpoutIdentifier1.equals(virtualSpoutIdentifier3));;
    }
}