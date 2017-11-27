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

package com.salesforce.storm.spout.dynamic.buffer;

import com.google.common.collect.Sets;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.Message;
import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import org.apache.storm.tuple.Values;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

/**
 * Test that {@link ThrottledMessageBuffer} throttles messages between virtual spouts.
 */
public class ThrottledMessageBufferTest {

    /**
     * Test that we read the config properties properly.
     */
    @Test
    public void testOpen() {
        final int bufferSize = 4;
        final int throttledBufferSize = 2;
        final String regexPattern = "throttled.*";

        // Create instance & open
        ThrottledMessageBuffer buffer = createDefaultBuffer(bufferSize, throttledBufferSize, regexPattern);

        // Check properties
        assertEquals("Buffer size configured", bufferSize, buffer.getMaxBufferSize());
        assertEquals("Throttled Buffer size configured", throttledBufferSize, buffer.getThrottledBufferSize());
        assertEquals("Regex Pattern set correctly", regexPattern, buffer.getRegexPattern().pattern());
    }

    /**
     * Validates that VirtualSpoutIds that SHOULD be marked as throttled, DO.
     * And those that SHOULD NOT, DON'T
     */
    @Test
    public void testVirtualSpoutsGetMarkedAsThrottled() {
        final String regexPattern = "^Throttled.*";

        // Create instance & open
        ThrottledMessageBuffer buffer = createDefaultBuffer(10, 3, regexPattern);

        final VirtualSpoutIdentifier id1 = new DefaultVirtualSpoutIdentifier("Throttled 1");
        final VirtualSpoutIdentifier id2 = new DefaultVirtualSpoutIdentifier("Not Throttled 2");
        final VirtualSpoutIdentifier id3 = new DefaultVirtualSpoutIdentifier("Throttled 3");
        final VirtualSpoutIdentifier id4 = new DefaultVirtualSpoutIdentifier("Not Throttled 4");

        final Set<VirtualSpoutIdentifier> throttledIds = Sets.newHashSet(
            id1, id3
        );

        final Set<VirtualSpoutIdentifier> nonThrottledIds = Sets.newHashSet(
            id2, id4
        );

        // Add them
        buffer.addVirtualSpoutId(id1);
        buffer.addVirtualSpoutId(id2);
        buffer.addVirtualSpoutId(id3);
        buffer.addVirtualSpoutId(id4);

        // Validate
        assertEquals("All non throttled Ids match expected", nonThrottledIds, buffer.getNonThrottledVirtualSpoutIdentifiers());
        assertEquals("All throttled Ids match expected", throttledIds, buffer.getThrottledVirtualSpoutIdentifiers());
    }

    /**
     * Tests that we throttle/block put() calls on "Throttled" spout identifiers.
     */
    @Test
    public void testThrottling() throws InterruptedException {
        final int bufferSize = 4;
        final int throttledBufferSize = 2;
        final String regexPattern = "^Throttled.*";

        // Create instance & open
        ThrottledMessageBuffer buffer = createDefaultBuffer(bufferSize, throttledBufferSize, regexPattern);

        // Create 3 VSpout Ids
        VirtualSpoutIdentifier virtualSpoutId1 = new DefaultVirtualSpoutIdentifier("Identifier1");
        VirtualSpoutIdentifier virtualSpoutId2 = new DefaultVirtualSpoutIdentifier("Throttled Identifier 1");
        VirtualSpoutIdentifier virtualSpoutId3 = new DefaultVirtualSpoutIdentifier("Throttled Identifier 2");

        // Notify buffer of our Ids
        buffer.addVirtualSpoutId(virtualSpoutId1);
        buffer.addVirtualSpoutId(virtualSpoutId2);
        buffer.addVirtualSpoutId(virtualSpoutId3);

        Message message1 = createMessage(virtualSpoutId1, new Values("A", "B"));
        Message message2 = createMessage(virtualSpoutId1, new Values("C", "D"));
        Message message3 = createMessage(virtualSpoutId1, new Values("E", "F"));
        Message message4 = createMessage(virtualSpoutId1, new Values("G", "H"));
        // We will not be able to add this message to the buffer because we will have reached out max size
        Message message5 = createMessage(virtualSpoutId1, new Values("I", "J"));

        // Add messages, these will not be throttled because the buffer has room
        buffer.put(message1);
        buffer.put(message2);
        buffer.put(message3);
        buffer.put(message4);

        assertEquals(4, buffer.size());

        // Track whether or not we hit the timeout
        boolean timedOut = false;

        // We are going to attempt an await call, but we are actually expecting it to timeout because put() on the
        // buffer is going to block until the buffer has room.
        try {
            await()
                // The timeout here is arbitrary, we just need to prove that putting onto the buffer does not work
                .atMost(2, TimeUnit.SECONDS)
                .until(() -> {
                    try {
                        buffer.put(message5);
                    } catch (InterruptedException e) {
                        // The interruption will occur when the timeout is reached, we are just throwing an unchecked
                        // exception here to end the until.
                        throw new RuntimeException(e);
                    }
                });
        } catch (ConditionTimeoutException ex) {
            timedOut = true;
        }

        assertTrue("Timed out trying to put onto the buffer.", timedOut);

        Message resultMessage1 = buffer.poll();

        assertEquals(3, buffer.size());

        assertNotNull("First message we put is not null", message1);
        assertEquals("First message we put matches the first resulting message", message1, resultMessage1);

        // We should be able to put the message that timed out back onto the buffer now
        buffer.put(message5);

        assertEquals(4, buffer.size());

        assertEquals("Second message we put matches the first resulting message", message2, buffer.poll());
        assertEquals("Third message we put matches the first resulting message", message3, buffer.poll());
        assertEquals("Fourth message we put matches the first resulting message", message4, buffer.poll());
        assertEquals("Fifth message (the one that was blocked) we put matches the first resulting message", message5, buffer.poll());
    }

    private Message createMessage(final VirtualSpoutIdentifier virtualSpoutIdentifier, Values values) {
        return new Message(new MessageId("namespace", 1, 1, virtualSpoutIdentifier), values);
    }

    private ThrottledMessageBuffer createDefaultBuffer(final int bufferSize, final int throttledBufferSize, final String regexPattern) {
        // Build config
        Map<String, Object> config = new HashMap<>();
        config.put(ThrottledMessageBuffer.CONFIG_BUFFER_SIZE, bufferSize);
        config.put(ThrottledMessageBuffer.CONFIG_THROTTLE_BUFFER_SIZE, throttledBufferSize);
        config.put(ThrottledMessageBuffer.CONFIG_THROTTLE_REGEX_PATTERN, regexPattern);
        final AbstractConfig spoutConfig = new AbstractConfig(new ConfigDefinition(), config);

        // Create instance & open
        ThrottledMessageBuffer buffer = new ThrottledMessageBuffer();
        buffer.open(spoutConfig);

        return buffer;
    }
}