package com.salesforce.storm.spout.sideline.buffer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.Message;
import com.salesforce.storm.spout.sideline.MessageId;
import com.salesforce.storm.spout.sideline.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
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
public class FIFOBufferTest {
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
        config.put(SidelineSpoutConfig.TUPLE_BUFFER_MAX_SIZE, maxBufferSize);

        // Create buffer & open
        MessageBuffer messageBuffer = new FIFOBuffer();
        messageBuffer.open(config);

        // Keep track of our order
        final List<Message> submittedOrder = Lists.newArrayList();

        // Create random number generator
        Random random = new Random();

        // Generate messages
        for (int x=0; x<(numberOfMessagesPer * numberOfVSpoutIds); x++) {
            // Generate source spout id
            final VirtualSpoutIdentifier sourceSpoutId = new VirtualSpoutIdentifier("srcSpoutId" + random.nextInt(numberOfVSpoutIds));
            final int partition = random.nextInt(10);


            Message message = new Message(
                    new MessageId("my namespace", partition, x, sourceSpoutId),
                    new Values("value" + x));

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
            assertEquals("Source Spout Id should be equal", originalKafkaMsg.getMessageId().getSrcVirtualSpoutId(), bufferedMsg.getMessageId().getSrcVirtualSpoutId());
            assertEquals("partitions should be equal", originalKafkaMsg.getPartition(), bufferedMsg.getPartition());
            assertEquals("offsets should be equal", originalKafkaMsg.getOffset(), bufferedMsg.getOffset());
            assertEquals("namespace should be equal", originalKafkaMsg.getNamespace(), bufferedMsg.getNamespace());
            assertEquals("messageIds should be equal", originalKafkaMsg.getMessageId(), bufferedMsg.getMessageId());
            assertEquals("Values should be equal", originalKafkaMsg.getValues(), bufferedMsg.getValues());
        }

        // Validate that the next polls are all null
        for (int x=0; x<64; x++) {
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
        config.put(SidelineSpoutConfig.TUPLE_BUFFER_MAX_SIZE, inputValue);

        // Create buffer
        FIFOBuffer messageBuffer = new FIFOBuffer();
        messageBuffer.open(config);

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