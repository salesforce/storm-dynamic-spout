/*
 * Copyright (c) 2018, Salesforce.com, Inc.
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

package com.salesforce.storm.spout.sideline;

import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Test {@link SidelineVirtualSpoutIdentifier} work correctly.
 */
public class SidelineVirtualSpoutIdentifierTest {

    /**
     * Test that toString creates correct identifiers.
     * @throws Exception Null or empty data provided.
     */
    @Test
    public void test_toString() throws Exception {
        VirtualSpoutIdentifier virtualSpoutIdentifier1 = new SidelineVirtualSpoutIdentifier(
            "foo",
            new SidelineRequestIdentifier("bar")
        );

        // Note that the identifier inserts 'sideline' in the middle of the consumer id and the request identifier
        assertEquals("foo:sideline:bar", virtualSpoutIdentifier1.toString());

        VirtualSpoutIdentifier virtualSpoutIdentifier2 = new SidelineVirtualSpoutIdentifier(
            "foo",
            new SidelineRequestIdentifier("baz")
        );

        // Note that the identifier inserts 'sideline' in the middle of the consumer id and the request identifier
        assertEquals("foo:sideline:baz", virtualSpoutIdentifier2.toString());
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