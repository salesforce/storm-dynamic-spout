package com.salesforce.storm.spout.sideline.buffer;

import com.google.common.collect.Sets;
import com.salesforce.storm.spout.sideline.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.Message;
import com.salesforce.storm.spout.sideline.MessageId;
import com.salesforce.storm.spout.sideline.VirtualSpoutIdentifier;
import org.apache.storm.tuple.Values;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class RatioMessageBufferTest {
    private static final Logger logger = LoggerFactory.getLogger(RatioMessageBufferTest.class);

    /**
     * Test that we read the config properties properly.
     */
    @Test
    public void testOpen() {
        final int bufferSize = 10;
        final int throttleRatio = 3;
        final String regexPattern = "^Throttled.*";

        // Create instance & open
        RatioMessageBuffer buffer = createDefaultBuffer(bufferSize, throttleRatio, regexPattern);

        // Check properties
        assertEquals("Buffer size configured", bufferSize, buffer.getMaxBufferSize());
        assertEquals("Throttled Buffer size configured", throttleRatio, buffer.getThrottleRatio());
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
        RatioMessageBuffer buffer = createDefaultBuffer(10, 3, regexPattern);

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
     * Tests that we return messages from the poll() method based on configured ratio
     * with two VirtualSpouts -- 1 throttled and 1 not throttled.
     */
    @Test
    public void testThrottling_twoVSpouts() throws InterruptedException {
        final int bufferSize = 100;
        final int throttleRatio = 3;
        final String regexPattern = "^Throttled.*";

        // Create instance & open
        RatioMessageBuffer buffer = createDefaultBuffer(bufferSize, throttleRatio, regexPattern);

        // Create 2 VSpout Ids
        VirtualSpoutIdentifier vSpoutId1 = new DefaultVirtualSpoutIdentifier("Identifier1");
        VirtualSpoutIdentifier vSpoutId2 = new DefaultVirtualSpoutIdentifier("Throttled Identifier 2");

        // Notify buffer of our Ids
        buffer.addVirtualSpoutId(vSpoutId1);
        buffer.addVirtualSpoutId(vSpoutId2);

        // Create 10 messages for Spout 1
        final List<Message> spout1Messages = new ArrayList<>();
        for (int x=0; x<10; x++) {
            spout1Messages.add(createMessage(vSpoutId1, new Values(vSpoutId1.toString(), x)));
        }
        // Create 5 messages for Spout 2
        final List<Message> spout2Messages = new ArrayList<>();
        for (int x=0; x<5; x++) {
            spout2Messages.add(createMessage(vSpoutId2, new Values(vSpoutId2.toString(), x + 1000)));
        }

        // Because order of adding messages shouldn't make a difference.
        // Add Spout2's messages first
        for (Message message: spout2Messages) {
            buffer.put(message);
        }

        // Then add Spout1's messages second
        for (Message message: spout1Messages) {
            buffer.put(message);
        }

        // Validate that we have the expected buffer size of 10 now
        assertEquals("Should have expected number of messages in buffer",(spout1Messages.size() + spout2Messages.size()), buffer.size());

        // Now hit poll() 20 times, adding results to an arrayList
        List<Message> returnedMessages = new ArrayList<>();
        do {
            returnedMessages.add(buffer.poll());
        } while (buffer.size() > 0);

        // Now validate we have no more messages
        assertEquals("Empty buffer", 0, buffer.size());

        // Validate we got the expected results.  This is kind of painful :/
        // First one should be from vspout 2
        validateExpectedMessage(returnedMessages.get(0), false, vSpoutId2, 1000 + 0);

        // 3 from vSpout1
        validateExpectedMessage(returnedMessages.get(1), false, vSpoutId1, 0);
        validateExpectedMessage(returnedMessages.get(2), false, vSpoutId1, 1);
        validateExpectedMessage(returnedMessages.get(3), false, vSpoutId1, 2);

        // 1 from vSpout2
        validateExpectedMessage(returnedMessages.get(4), false, vSpoutId2, 1000 + 1);

        // 3 from vSpout1
        validateExpectedMessage(returnedMessages.get(5), false, vSpoutId1, 3);
        validateExpectedMessage(returnedMessages.get(6), false, vSpoutId1, 4);
        validateExpectedMessage(returnedMessages.get(7), false, vSpoutId1, 5);

        // 1 from vSpout2
        validateExpectedMessage(returnedMessages.get(8), false, vSpoutId2, 1000 + 2);

        // 3 from vSpout1
        validateExpectedMessage(returnedMessages.get(9), false, vSpoutId1, 6);
        validateExpectedMessage(returnedMessages.get(10), false, vSpoutId1, 7);
        validateExpectedMessage(returnedMessages.get(11), false, vSpoutId1, 8);

        // 1 from vSpout2
        validateExpectedMessage(returnedMessages.get(12), false, vSpoutId2, 1000 + 3);

        // Last remaining entry from vSpout1, followed by 2 nulls
        validateExpectedMessage(returnedMessages.get(13), false, vSpoutId1, 9);
        validateExpectedMessage(returnedMessages.get(14), true, null, -1);
        validateExpectedMessage(returnedMessages.get(15), true, null, -1);

        // Last remaining entry from vSpout2
        validateExpectedMessage(returnedMessages.get(16), false, vSpoutId2, 1000 + 4);
    }

    /**
     * Tests that we return messages from the poll() method based on configured ratio
     * with three VirtualSpouts -- 1 throttled and 2 not throttled.
     */
    @Test
    public void testThrottling_threeVSpouts() throws InterruptedException {
        final int bufferSize = 100;
        final int throttleRatio = 3;
        final String regexPattern = "^Throttled.*";

        // Create instance & open
        RatioMessageBuffer buffer = createDefaultBuffer(bufferSize, throttleRatio, regexPattern);

        // Create 2 VSpout Ids
        VirtualSpoutIdentifier vSpoutId1 = new DefaultVirtualSpoutIdentifier("Identifier1");
        VirtualSpoutIdentifier vSpoutId2 = new DefaultVirtualSpoutIdentifier("Throttled Identifier 2");
        VirtualSpoutIdentifier vSpoutId3 = new DefaultVirtualSpoutIdentifier("Identifier 3");

        // Notify buffer of our Ids
        buffer.addVirtualSpoutId(vSpoutId1);
        buffer.addVirtualSpoutId(vSpoutId2);
        buffer.addVirtualSpoutId(vSpoutId3);

        // Create 7 messages for Spout 1 & add to buffer
        final List<Message> spout1Messages = new ArrayList<>();
        for (int x=0; x<7; x++) {
            final Message message = createMessage(vSpoutId1, new Values(vSpoutId1.toString(), x));
            spout1Messages.add(message);
            buffer.put(message);
        }

        // Create 4 messages for Spout 2 & add to buffer
        final List<Message> spout2Messages = new ArrayList<>();
        for (int x=0; x<4; x++) {
            final Message message = createMessage(vSpoutId2, new Values(vSpoutId2.toString(), x + 100));
            spout2Messages.add(message);
            buffer.put(message);
        }

        // Create 7 messages for Spout 3 & add to buffer
        final List<Message> spout3Messages = new ArrayList<>();
        for (int x=0; x<7; x++) {
            final Message message = createMessage(vSpoutId3, new Values(vSpoutId3.toString(), x + 1000));
            spout3Messages.add(message);
            buffer.put(message);
        }

        // Validate that we have the expected buffer size of 21 now
        assertEquals("Should have expected number of messages in buffer",(spout1Messages.size() + spout2Messages.size() + spout3Messages.size()), buffer.size());

        // Now hit poll() until we're empty
        List<Message> returnedMessages = new ArrayList<>();
        do {
            returnedMessages.add(buffer.poll());
        } while (buffer.size() > 0);

        // Now validate we have no more messages
        assertEquals("Empty buffer", 0, buffer.size());

        // Validate we got the expected results.  This is kind of painful :/
        // First one should be from vspout 2, our throttled spout
        validateExpectedMessage(returnedMessages.get(0), false, vSpoutId2, 100 + 0);

        // 3 from vSpout3
        validateExpectedMessage(returnedMessages.get(1), false, vSpoutId3, 1000 + 0);
        validateExpectedMessage(returnedMessages.get(2), false, vSpoutId3, 1000 + 1);
        validateExpectedMessage(returnedMessages.get(3), false, vSpoutId3, 1000 + 2);

        // 3 from vSpout1
        validateExpectedMessage(returnedMessages.get(4), false, vSpoutId1, 0);
        validateExpectedMessage(returnedMessages.get(5), false, vSpoutId1, 1);
        validateExpectedMessage(returnedMessages.get(6), false, vSpoutId1, 2);

        // 1 vspout 2, (throttled)
        validateExpectedMessage(returnedMessages.get(7), false, vSpoutId2, 100 + 1);

        // 3 from vSpout3
        validateExpectedMessage(returnedMessages.get(8), false, vSpoutId3, 1000 + 3);
        validateExpectedMessage(returnedMessages.get(9), false, vSpoutId3, 1000 + 4);
        validateExpectedMessage(returnedMessages.get(10), false, vSpoutId3, 1000 + 5);

        // 3 from vSpout1
        validateExpectedMessage(returnedMessages.get(11), false, vSpoutId1, 3);
        validateExpectedMessage(returnedMessages.get(12), false, vSpoutId1, 4);
        validateExpectedMessage(returnedMessages.get(13), false, vSpoutId1, 5);

        // 1 vspout 2, (throttled)
        validateExpectedMessage(returnedMessages.get(14), false, vSpoutId2, 100 + 2);

        // last remaining entry from vSpout3 (plus 2 nulls)
        validateExpectedMessage(returnedMessages.get(15), false, vSpoutId3, 1000 + 6);
        validateExpectedMessage(returnedMessages.get(16), true, null, -1);
        validateExpectedMessage(returnedMessages.get(17), true, null, -1);

        // last remaining entry from vSpout1 (plus 2 nulls)
        validateExpectedMessage(returnedMessages.get(18), false, vSpoutId1, 6);
        validateExpectedMessage(returnedMessages.get(19), true, null, -1);
        validateExpectedMessage(returnedMessages.get(20), true, null, -1);

        // Last remaining entry from VSpout2 (throttled)
        validateExpectedMessage(returnedMessages.get(21), false, vSpoutId2, 100 + 3);
    }

    private void validateExpectedMessage(final Message message, final boolean isNull, final VirtualSpoutIdentifier expectedVSpoutId, final int expectedIndex) {
        // If we expected a null value
        if (isNull) {
            // validate & return
            assertNull("Message should be null", message);
            return;
        }

        // Validate
        assertNotNull("Message should not be null", message);
        assertEquals("Should be from our expected Spout", expectedVSpoutId, message.getMessageId().getSrcVirtualSpoutId());
        assertEquals("Should have 1st value", expectedVSpoutId.toString(), message.getValues().get(0));
        assertEquals("Should have 2nd value", expectedIndex, message.getValues().get(1));
    }

    private RatioMessageBuffer createDefaultBuffer(final int bufferSize, final int throttleRatio, final String regexPattern) {
        // Build config
        Map<String, Object> config = new HashMap<>();
        config.put(RatioMessageBuffer.CONFIG_BUFFER_SIZE, bufferSize);
        config.put(RatioMessageBuffer.CONFIG_THROTTLE_RATIO, throttleRatio);
        config.put(RatioMessageBuffer.CONFIG_THROTTLE_REGEX_PATTERN, regexPattern);

        // Create instance & open
        RatioMessageBuffer buffer = new RatioMessageBuffer();
        buffer.open(config);

        return buffer;
    }

    private Message createMessage(final VirtualSpoutIdentifier virtualSpoutIdentifier, Values values) {
        return new Message(new MessageId("namespace", 1, 1, virtualSpoutIdentifier), values);
    }
}