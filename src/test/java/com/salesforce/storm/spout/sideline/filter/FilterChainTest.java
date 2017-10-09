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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test that a {@link FilterChain} processes a set of filters correctly.
 */
public class FilterChainTest {

    /**
     * Test that each step of a chain is processed using a string.
     */
    @Test
    public void testChain() {
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
            .addStep(new SidelineRequestIdentifier(), new NumberFilter(2))
            .addStep(new SidelineRequestIdentifier(), new NumberFilter(4))
            .addStep(new SidelineRequestIdentifier(), new NumberFilter(5))
        ;

        assertTrue(filterChain.filter(message2));
        assertTrue(filterChain.filter(message4));
        assertTrue(filterChain.filter(message5));

        assertFalse(filterChain.filter(message1));
        assertFalse(filterChain.filter(message3));
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
            .addStep(new SidelineRequestIdentifier(), new NegatingFilterChainStep(new NumberFilter(2)))
        ;

        assertTrue(filterChain.filter(message1));

        assertFalse(filterChain.filter(message2));
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
