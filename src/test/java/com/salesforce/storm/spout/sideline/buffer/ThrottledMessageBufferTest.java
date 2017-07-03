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
package com.salesforce.storm.spout.sideline.buffer;

import com.salesforce.storm.spout.sideline.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.Message;
import com.salesforce.storm.spout.sideline.MessageId;
import com.salesforce.storm.spout.sideline.VirtualSpout;
import com.salesforce.storm.spout.sideline.VirtualSpoutIdentifier;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.*;

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
     * Test that we read the config properties properly.
     */
    @Test
    public void testThrottling() throws InterruptedException {
        final int bufferSize = 4;
        final int throttledBufferSize = 2;
        final String regexPattern = "^Throttled.*";

        // Create instance & open
        ThrottledMessageBuffer buffer = createDefaultBuffer(bufferSize, throttledBufferSize, regexPattern);

        // Create 3 VSpout Ids
        VirtualSpoutIdentifier vSpoutId1 = new DefaultVirtualSpoutIdentifier("Identifier1");
        VirtualSpoutIdentifier vSpoutId2 = new DefaultVirtualSpoutIdentifier("Throttled Identifier 1");
        VirtualSpoutIdentifier vSpoutId3 = new DefaultVirtualSpoutIdentifier("Throttled Identifier 2");

        // Notify buffer of our Ids
        buffer.addVirtualSpoutId(vSpoutId1);
        buffer.addVirtualSpoutId(vSpoutId2);
        buffer.addVirtualSpoutId(vSpoutId3);

        // Add messages to non-throttled
        buffer.put(createMessage(vSpoutId1));
        buffer.put(createMessage(vSpoutId1));
        buffer.put(createMessage(vSpoutId1));
        buffer.put(createMessage(vSpoutId1));

        await()
            .atMost(5, TimeUnit.SECONDS)
            .until(new Runnable() {
                @Override
                public void run() {
                    try {
                        buffer.put(createMessage(vSpoutId1));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    }

    private Message createMessage(final VirtualSpoutIdentifier virtualSpoutIdentifier) {
        return new Message(new MessageId("namespace", 1, 1, virtualSpoutIdentifier), new Values("A", "B"));
    }

    private ThrottledMessageBuffer createDefaultBuffer(final int bufferSize, final int throttledBufferSize, final String regexPattern) {
        // Build config
        Map<String, Object> config = new HashMap<>();
        config.put(ThrottledMessageBuffer.CONFIG_BUFFER_SIZE, bufferSize);
        config.put(ThrottledMessageBuffer.CONFIG_THROTTLE_BUFFER_SIZE, throttledBufferSize);
        config.put(ThrottledMessageBuffer.CONFIG_THROTTLE_REGEX_PATTERN, regexPattern);

        // Create instance & open
        ThrottledMessageBuffer buffer = new ThrottledMessageBuffer();
        buffer.open(config);

        return buffer;
    }
}