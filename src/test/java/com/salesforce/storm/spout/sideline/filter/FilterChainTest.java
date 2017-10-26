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

package com.salesforce.storm.spout.sideline.filter;

import com.salesforce.storm.spout.dynamic.Message;
import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test that a {@link FilterChain} processes a set of filters correctly.
 */
public class FilterChainTest {

    /**
     * Test that adding a step adds it to the chain, removing it removes it and that the has method reflects that.
     */
    @Test
    public void addStepAndRemoveStep() {
        final SidelineRequestIdentifier sidelineRequestIdentifier = new SidelineRequestIdentifier("1");

        final FilterChain filterChain = new FilterChain();

        assertFalse("There shouldn't be a step for the identifier", filterChain.hasStep(sidelineRequestIdentifier));

        filterChain.addStep(sidelineRequestIdentifier, new NumberFilter(2));

        assertTrue("There should be a step for the identifier", filterChain.hasStep(sidelineRequestIdentifier));

        filterChain.removeStep(sidelineRequestIdentifier);

        assertFalse("There shouldn't be a step for the identifier", filterChain.hasStep(sidelineRequestIdentifier));
    }

    /**
     * Test that each step of a chain is processed using a string.
     */
    @Test
    public void filter() {
        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("FakeConsumer");

        final Message message1 = new Message(
            new MessageId("foobar", 1, 0L, consumerId),
            new Values(1)
        );

        final Message message2 = new Message(
            new MessageId("foobar", 1, 0L, consumerId),
            new Values(2)
        );

        final Message message3 = new Message(
            new MessageId("foobar", 1, 0L, consumerId),
            new Values(3)
        );

        final Message message4 = new Message(
            new MessageId("foobar", 1, 0L, consumerId),
            new Values(4)
        );

        final Message message5 = new Message(
            new MessageId("foobar", 1, 0L, consumerId),
            new Values(5)
        );

        final FilterChain filterChain = new FilterChain()
            .addStep(new SidelineRequestIdentifier("1"), new NumberFilter(2))
            .addStep(new SidelineRequestIdentifier("2"), new NumberFilter(4))
            .addStep(new SidelineRequestIdentifier("3"), new NumberFilter(5))
        ;

        assertTrue("Message 2 should be fitlered", filterChain.filter(message2));
        assertTrue("Message 4 should be fitlered", filterChain.filter(message4));
        assertTrue("Message 4 should be fitlered", filterChain.filter(message5));

        assertFalse("Message 1 shouldn't be fitlered", filterChain.filter(message1));
        assertFalse("Message 3 shouldn't be fitlered", filterChain.filter(message3));
    }

    /**
     * Test that each step of a chain is processed using a string.
     */
    @Test
    public void testNegatingChain() {
        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("FakeConsumer");

        final Message message1 = new Message(
            new MessageId("foobar", 1, 0L, consumerId),
            new Values(1)
        );

        final Message message2 = new Message(
            new MessageId("foobar", 1, 0L, consumerId),
            new Values(2)
        );

        final FilterChain filterChain = new FilterChain()
            .addStep(new SidelineRequestIdentifier("1"), new NegatingFilterChainStep(new NumberFilter(2)))
        ;

        assertTrue("Message 1 should be filtered", filterChain.filter(message1));

        assertFalse("Message 2 shouldn't be filtered", filterChain.filter(message2));
    }

    /**
     * Test that we can find a step's identifier using the step.
     */
    @Test
    public void findStep() {
        final SidelineRequestIdentifier sidelineRequestIdentifier1 = new SidelineRequestIdentifier("1");
        final FilterChainStep filterChainStep1 = new NumberFilter(2);

        final SidelineRequestIdentifier sidelineRequestIdentifier2 = new SidelineRequestIdentifier("2");
        final FilterChainStep filterChainStep2 = new NumberFilter(4);

        final FilterChain filterChain = new FilterChain()
            .addStep(sidelineRequestIdentifier1, filterChainStep1)
            .addStep(sidelineRequestIdentifier2, filterChainStep2)
        ;

        assertEquals(
            "Identifier 1 does not match step 1",
            sidelineRequestIdentifier1,
            filterChain.findStep(filterChainStep1)
        );

        assertEquals(
            "Identifier 2 does not match step 2",
            sidelineRequestIdentifier2,
            filterChain.findStep(filterChainStep2)
        );
    }

    /**
     * Test that we can get a map of the filter chain steps.
     */
    @Test
    public void getSteps() {
        final SidelineRequestIdentifier sidelineRequestIdentifier1 = new SidelineRequestIdentifier("1");
        final FilterChainStep filterChainStep1 = new NumberFilter(2);

        final SidelineRequestIdentifier sidelineRequestIdentifier2 = new SidelineRequestIdentifier("2");
        final FilterChainStep filterChainStep2 = new NumberFilter(4);

        final FilterChain filterChain = new FilterChain()
            .addStep(sidelineRequestIdentifier1, filterChainStep1)
            .addStep(sidelineRequestIdentifier2, filterChainStep2)
        ;

        final Map<SidelineRequestIdentifier,FilterChainStep> steps = filterChain.getSteps();

        assertNotNull("Steps should not be null", filterChain.getSteps());

        assertTrue(
            "Identifier 1 should be in the map",
            steps.containsKey(sidelineRequestIdentifier1)
        );
        assertEquals(
            "Step 1 should match",
            filterChainStep1,
            steps.get(sidelineRequestIdentifier1)
        );

        assertTrue(
            "Identifier 2 should be in the map",
            steps.containsKey(sidelineRequestIdentifier2)
        );
        assertEquals(
            "Step 2 should match",
            filterChainStep2,
            steps.get(sidelineRequestIdentifier2)
        );
    }

    /**
     * Test that we can get a step by its identifier.
     */
    @Test
    public void getStep() {
        final SidelineRequestIdentifier sidelineRequestIdentifier1 = new SidelineRequestIdentifier("1");
        final FilterChainStep filterChainStep1 = new NumberFilter(2);

        final SidelineRequestIdentifier sidelineRequestIdentifier2 = new SidelineRequestIdentifier("2");
        final FilterChainStep filterChainStep2 = new NumberFilter(4);

        final FilterChain filterChain = new FilterChain()
            .addStep(sidelineRequestIdentifier1, filterChainStep1)
            .addStep(sidelineRequestIdentifier2, filterChainStep2)
        ;

        assertEquals(
            "Identifier 1 should yield step 1",
            filterChainStep1,
            filterChain.getStep(sidelineRequestIdentifier1)
        );

        assertEquals(
            "Identifier 2 should yield step 2",
            filterChainStep2,
            filterChain.getStep(sidelineRequestIdentifier2)
        );
    }

    private static class NumberFilter implements FilterChainStep {

        private final int number;

        NumberFilter(final int number) {
            this.number = number;
        }

        /**
         * Filter a message.
         * @param message The filter to be processed by this step of the chain.
         * @return true if the message should be filtered, false otherwise.
         */
        @Override
        public boolean filter(Message message) {
            Integer messageNumber = (Integer) message.getValues().get(0);
            // Filter them if they don't match, in other words "not" equals
            return messageNumber.equals(number);
        }
    }
}
