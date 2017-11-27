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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.Message;
import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import com.salesforce.storm.spout.dynamic.config.DynamicSpoutConfig;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.storm.tuple.Values;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Kind of silly.  Basically just testing a FIFO buffer.
 */
@RunWith(DataProviderRunner.class)
public class FifoBufferTest {

    /**
     * Basically just tests that this does FIFO.
     * Kind of silly.
     */
    @Test
    public void doTest() throws InterruptedException {
        // numberOfVSpoutIds * numberOfMessagesPer should be less than the configured
        // max buffer size.
        final int numberOfVSpoutIds = 5;
        final int numberOfMessagesPer = 1500;
        final int maxBufferSize = (numberOfMessagesPer * numberOfVSpoutIds) + 1;

        // Create config
        Map<String, Object> config = Maps.newHashMap();
        config.put(DynamicSpoutConfig.TUPLE_BUFFER_MAX_SIZE, maxBufferSize);
        final SpoutConfig spoutConfig = new SpoutConfig(new ConfigDefinition(), config);

        // Create buffer & open
        MessageBuffer messageBuffer = new FifoBuffer();
        messageBuffer.open(spoutConfig);

        // Keep track of our order
        final List<Message> submittedOrder = Lists.newArrayList();

        // Create random number generator
        Random random = new Random();

        // Generate messages
        for (int x = 0; x < (numberOfMessagesPer * numberOfVSpoutIds); x++) {
            // Generate source spout id
            final DefaultVirtualSpoutIdentifier sourceSpoutId = new DefaultVirtualSpoutIdentifier(
                "srcSpoutId" + random.nextInt(numberOfVSpoutIds)
            );
            final int partition = random.nextInt(10);


            Message message = new Message(
                new MessageId("my namespace", partition, x, sourceSpoutId),
                new Values("value" + x)
            );

            // Keep track of order
            submittedOrder.add(message);

            // Add to our buffer
            messageBuffer.put(message);
        }

        // Validate size
        assertEquals("Size should be known", (numberOfMessagesPer * numberOfVSpoutIds), messageBuffer.size());

        // Now pop them, order should be maintained
        for (Message originalKafkaMsg: submittedOrder) {
            final Message bufferedMsg = messageBuffer.poll();

            // Validate it
            assertNotNull("Should not be null", bufferedMsg);
            assertEquals("Objects should be the same", originalKafkaMsg, bufferedMsg);

            // Validate the contents are the same
            assertEquals(
                "Source Spout Id should be equal",
                originalKafkaMsg.getMessageId().getSrcVirtualSpoutId(), bufferedMsg.getMessageId().getSrcVirtualSpoutId()
            );
            assertEquals("partitions should be equal", originalKafkaMsg.getPartition(), bufferedMsg.getPartition());
            assertEquals("offsets should be equal", originalKafkaMsg.getOffset(), bufferedMsg.getOffset());
            assertEquals("namespace should be equal", originalKafkaMsg.getNamespace(), bufferedMsg.getNamespace());
            assertEquals("messageIds should be equal", originalKafkaMsg.getMessageId(), bufferedMsg.getMessageId());
            assertEquals("Values should be equal", originalKafkaMsg.getValues(), bufferedMsg.getValues());
        }

        // Validate that the next polls are all null
        for (int x = 0; x < 64; x++) {
            assertNull("Should be null", messageBuffer.poll());
        }
    }

    /**
     * Makes sure that we can properly parse long config values on open().
     */
    @Test
    @UseDataProvider("provideConfigObjects")
    public void testConstructorWithConfigValue(Number inputValue) throws InterruptedException {
        // Create config
        Map<String, Object> config = Maps.newHashMap();
        config.put(DynamicSpoutConfig.TUPLE_BUFFER_MAX_SIZE, inputValue);
        final SpoutConfig spoutConfig = new SpoutConfig(new ConfigDefinition(), config);

        // Create buffer
        FifoBuffer messageBuffer = new FifoBuffer();
        messageBuffer.open(spoutConfig);

        // Validate
        assertEquals("Set correct", inputValue.intValue(), ((LinkedBlockingQueue)messageBuffer.getUnderlyingQueue()).remainingCapacity());
    }

    /**
     * Provides various tuple buffer implementation.
     */
    @DataProvider
    public static Object[][] provideConfigObjects() throws InstantiationException, IllegalAccessException {
        return new Object[][]{
                // Integer
                { 200 },

                // Long
                { 2000L }
        };
    }
}