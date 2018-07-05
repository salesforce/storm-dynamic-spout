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

package com.salesforce.storm.spout.dynamic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test that {@link DefaultVirtualSpoutIdentifier}'s can be created correctly.
 */
public class DefaultVirtualSpoutIdentifierTest {

    /**
     * Test that two identifiers created with the same string match.
     * @throws Exception Bad identifier
     */
    @Test
    public void test_toString_and_equals() throws Exception {
        final DefaultVirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("FooBar");

        assertEquals(virtualSpoutIdentifier, new DefaultVirtualSpoutIdentifier("FooBar"));
        assertEquals(virtualSpoutIdentifier.toString(), new DefaultVirtualSpoutIdentifier("FooBar").toString());
    }

    /**
     * Test that two identifiers that are different do not match.
     * @throws Exception Bad identifier
     */
    @Test
    public void test_not_toString_and_equals() throws Exception {
        final DefaultVirtualSpoutIdentifier virtualSpoutIdentifier1 = new DefaultVirtualSpoutIdentifier("Foo");
        final DefaultVirtualSpoutIdentifier virtualSpoutIdentifier2 = new DefaultVirtualSpoutIdentifier("Bar");

        assertNotEquals(virtualSpoutIdentifier1, virtualSpoutIdentifier2);
        assertNotEquals(virtualSpoutIdentifier1.toString(), virtualSpoutIdentifier2.toString());
    }

    /**
     * Test that supplying null will throw an exception.
     */
    @Test()
    public void test_nullIdentifier() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
            new DefaultVirtualSpoutIdentifier(null)
        );
    }

    /**
     * Test that supplying an empty string will throw an exception.
     */
    @Test()
    public void test_emptyIdentifier() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
            new DefaultVirtualSpoutIdentifier("")
        );
    }
}