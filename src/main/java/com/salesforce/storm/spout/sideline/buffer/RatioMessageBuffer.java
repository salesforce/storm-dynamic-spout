package com.salesforce.storm.spout.sideline.buffer;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.Message;
import com.salesforce.storm.spout.sideline.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.config.SpoutConfig;
import org.apache.storm.shade.com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Prototype example of a MessageBuffer that throttles based on a ratio.
 */
public class RatioMessageBuffer implements MessageBuffer {
    private static final Logger logger = LoggerFactory.getLogger(RatioMessageBuffer.class);

    /**
     * Config option for NON-throttled buffer size.
     */
    private static final String CONFIG_THROTTLE_RATIO = "spout.coordinator.tuple_buffer.throttle_ratio";

    /**
     * Config option to define a regex pattern to match against VirtualSpoutIds.  If a VirtualSpoutId
     * matches this pattern, it will be throttled.
     */
    private static final String CONFIG_THROTTLE_REGEX_PATTERN = "spout.coordinator.tuple_buffer.throttled_spout_id_regex";

    /**
     * A Map of VirtualSpoutIds => Its own Blocking Queue.
     */
    private final Map<VirtualSpoutIdentifier, BlockingQueue<Message>> messageBuffer = new ConcurrentHashMap<>();

    // Config values around buffer sizes.
    private int maxBufferSize = 2000;

    /**
     * Defaults 5 to 1 ratio.
     */
    private int throttleRatio = 5;

    // Match everything by default
    private Pattern regexPattern = Pattern.compile(".*");

    private NextVirtualSpoutIdGenerator nextVirtualSpoutIdGenerator;

    public RatioMessageBuffer() {
    }

    /**
     * Helper method for creating a default instance.
     */
    public static RatioMessageBuffer createDefaultInstance() {
        Map<String, Object> map = Maps.newHashMap();
        map.put(SpoutConfig.TUPLE_BUFFER_MAX_SIZE, 10000);
        map.put(CONFIG_THROTTLE_RATIO, 0.5);

        RatioMessageBuffer buffer = new RatioMessageBuffer();
        buffer.open(map);

        return buffer;
    }

    @Override
    public void open(final Map spoutConfig) {
        // Setup non-throttled buffer size
        Object bufferSize = spoutConfig.get(SpoutConfig.TUPLE_BUFFER_MAX_SIZE);
        if (bufferSize != null && bufferSize instanceof Number) {
            maxBufferSize = ((Number) bufferSize).intValue();
        }

        // Setup throttled ratio
        bufferSize = spoutConfig.get(CONFIG_THROTTLE_RATIO);
        if (bufferSize != null && bufferSize instanceof Number) {
            throttleRatio = ((Number) bufferSize).intValue();
        }

        // setup regex
        String regexPatternStr = (String) spoutConfig.get(CONFIG_THROTTLE_REGEX_PATTERN);
        if (regexPatternStr != null && !regexPatternStr.isEmpty()) {
            regexPattern = Pattern.compile(regexPatternStr);
        }

        nextVirtualSpoutIdGenerator = new NextVirtualSpoutIdGenerator(throttleRatio);
    }

    /**
     * Let the Implementation know that we're adding a new VirtualSpoutId.
     * @param virtualSpoutId - Identifier of new Virtual Spout.
     */
    @Override
    public void addVirtualSpoutId(final VirtualSpoutIdentifier virtualSpoutId) {
        synchronized (messageBuffer) {
            messageBuffer.putIfAbsent(virtualSpoutId, createNewQueue());

            boolean isThrottled = regexPattern.matcher(virtualSpoutId.toString()).find();
            nextVirtualSpoutIdGenerator.addNewVirtualSpout(virtualSpoutId, isThrottled);
        }
    }

    /**
     * Let the Implementation know that we're removing/cleaning up from closing a VirtualSpout.
     * @param virtualSpoutId - Identifier of Virtual Spout to be cleaned up.
     */
    @Override
    public void removeVirtualSpoutId(final VirtualSpoutIdentifier virtualSpoutId) {
        synchronized (messageBuffer) {
            messageBuffer.remove(virtualSpoutId);
            nextVirtualSpoutIdGenerator.removeVirtualSpout(virtualSpoutId);
        }
    }

    /**
     * Put a new message onto the queue.  This method is blocking if the queue buffer is full.
     * @param message - Message to be added to the queue.
     * @throws InterruptedException - thrown if a thread is interrupted while blocked adding to the queue.
     */
    @Override
    public void put(final Message message) throws InterruptedException {
        // Grab the source virtual spoutId
        final VirtualSpoutIdentifier virtualSpoutId = message.getMessageId().getSrcVirtualSpoutId();

        // Add to correct buffer
        BlockingQueue<Message> virtualSpoutQueue = messageBuffer.get(virtualSpoutId);

        // If our queue doesn't exist
        if (virtualSpoutQueue == null) {
            // Add the virtualSpoutId
            addVirtualSpoutId(virtualSpoutId);

            // Grab a reference.
            virtualSpoutQueue = messageBuffer.get(virtualSpoutId);
        }
        // Put it.
        virtualSpoutQueue.put(message);
    }

    @Override
    public int size() {
        int total = 0;
        for (final Queue queue: messageBuffer.values()) {
            total += queue.size();
        }
        return total;
    }

    /**
     * @return - returns the next Message to be processed out of the queue.
     */
    @Override
    public Message poll() {
        return messageBuffer.get(nextVirtualSpoutIdGenerator.nextVirtualSpoutId()).poll();
    }

    /**
     * @return - return a new LinkedBlockingQueue instance with a max size of our configured buffer.
     */
    private BlockingQueue<Message> createNewQueue() {
        return new LinkedBlockingQueue<>(getMaxBufferSize());
    }


    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    private static class NextVirtualSpoutIdGenerator {
        private final int ratio;
        private final List<VirtualSpoutIdentifier> order = new ArrayList<>();
        private final Map<VirtualSpoutIdentifier, Boolean> allIds = new HashMap<>();

        /**
         * An iterator over the Keys in buffer.  Used to Round Robin through the VirtualSpouts.
         */
        private Iterator<VirtualSpoutIdentifier> consumerIdIterator = null;

        public NextVirtualSpoutIdGenerator(final int ratio) {
            this.ratio = ratio;
        }

        public void addNewVirtualSpout(final VirtualSpoutIdentifier identifier, final boolean isThrottled) {
            if (!allIds.containsKey(identifier)) {
                allIds.put(identifier, isThrottled);
                recalculateOrdering();
            }
        }

        public void removeVirtualSpout(final VirtualSpoutIdentifier identifier) {
            if (allIds.containsKey(identifier)) {
                allIds.remove(identifier);
                recalculateOrdering();
            }
        }

        private void recalculateOrdering() {
            // clear ordering
            order.clear();

            // Create new ordering, probably a better way to do this...
            for (Map.Entry<VirtualSpoutIdentifier, Boolean> entry: allIds.entrySet()) {
                // If throttled
                if (entry.getValue()) {
                    order.add(entry.getKey());
                } else {
                    // Add entries at ratio
                    for (int x = 0; x < ratio; x++) {
                        order.add(entry.getKey());
                    }
                }
            }

            // create new iterator that cycles endlessly
            consumerIdIterator = Iterators.cycle(order);
        }

        public VirtualSpoutIdentifier nextVirtualSpoutId() {
            return consumerIdIterator.next();
        }
    }
}
