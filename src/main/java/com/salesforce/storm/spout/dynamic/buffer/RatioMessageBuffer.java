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

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.Message;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
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
import java.util.regex.Pattern;

/**
 * Prototype example of a MessageBuffer that attempts to throttle via a configurable ratio on the consuming side/poll()
 * method that determines which Spout to consume the next message from.
 *
 * This implementation should be considered "experimental" at this point as no real world testing has been done
 * on it yet.
 *
 * The way this works is you define a REGEX pattern to check against VirtualSpoutIdentifiers.
 * If a VirtualSpoutIdentifier MATCHES this REGEX, then we will adjust the frequency in which messages will be returned
 * by the poll() from the throttled VirtualSpoutIds.
 *
 * Example:
 * With the following configuration
 *   - Regex pattern: /^throttle/
 *   - ThrottleRatio: 10
 *
 * VirtualSpoutId: NormalVirtualSpoutId
 * Effective ThrottleRatio: ~(1/N) where N = Number of VirtualSpouts
 * Result: Because the VirtualSpoutId does NOT match the REGEX pattern, this implementation will fall back to
 *         essentially round robbin returning entries from the poll() method for all non-throttled VirtualSpouts
 *         with equal preference.
 *
 * VirtualSpoutId: ThrottledVirtualSpoutId
 * Effective ThrottleRatio: 1/(10+N), where N = Number of VirtualSpouts.
 * Result: Because the VirtualSpoutId DOES match the REGEX pattern, this implementation will attempt to emit entries
 *         from the poll() method at a rate that is 1/10th as often as the non-throttled VirtualSpouts.
 */
public class RatioMessageBuffer implements MessageBuffer {
    private static final Logger logger = LoggerFactory.getLogger(RatioMessageBuffer.class);

    /**
     * Config option for max buffer size.
     */
    public static final String CONFIG_BUFFER_SIZE = SpoutConfig.TUPLE_BUFFER_MAX_SIZE;

    /**
     * Config option for NON-throttled buffer size.
     */
    public static final String CONFIG_THROTTLE_RATIO = "spout.coordinator.tuple_buffer.throttle_ratio";

    /**
     * Config option to define a regex pattern to match against VirtualSpoutIds.  If a VirtualSpoutId
     * matches this pattern, it will be throttled.
     */
    public static final String CONFIG_THROTTLE_REGEX_PATTERN = "spout.coordinator.tuple_buffer.throttled_spout_id_regex";

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
     * @return factory method for create an instance of the buffer.
     */
    public static RatioMessageBuffer createDefaultInstance() {
        Map<String, Object> map = Maps.newHashMap();
        map.put(SpoutConfig.TUPLE_BUFFER_MAX_SIZE, 10000);
        map.put(CONFIG_THROTTLE_RATIO, 0.5);
        final AbstractConfig spoutConfig = new AbstractConfig(new ConfigDefinition(), map);

        RatioMessageBuffer buffer = new RatioMessageBuffer();
        buffer.open(spoutConfig);

        return buffer;
    }

    @Override
    public void open(final AbstractConfig spoutConfig) {
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

            // Debug logging
            logger.debug("Added new VirtualSpoutId [{}] Throttled? {}", virtualSpoutId, isThrottled);
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
     * @return returns the next Message to be processed out of the queue.
     */
    @Override
    public Message poll() {
        final VirtualSpoutIdentifier nextIndentifier = nextVirtualSpoutIdGenerator.nextVirtualSpoutId();
        return messageBuffer.get(nextIndentifier).poll();
    }

    /**
     * @return return a new LinkedBlockingQueue instance with a max size of our configured buffer.
     */
    private BlockingQueue<Message> createNewQueue() {
        return new LinkedBlockingQueue<>(getMaxBufferSize());
    }

    /**
     * @return The configured maximum buffer size.
     */
    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    /**
     * @return The configured Regex pattern to match throttled VirtualSpoutIds against.
     */
    public Pattern getRegexPattern() {
        return regexPattern;
    }

    /**
     * @return The configured throttle ratio.
     */
    public int getThrottleRatio() {
        return throttleRatio;
    }

    /**
     * @return Set of all VirtualSpoutIds that ARE throttled.
     */
    public Set<VirtualSpoutIdentifier> getThrottledVirtualSpoutIdentifiers() {
        return nextVirtualSpoutIdGenerator.getAllThrottledVirtualSpoutIds();
    }

    /**
     * @return Set of all VirtualSpoutIds that are NOT throttled.
     */
    public Set<VirtualSpoutIdentifier> getNonThrottledVirtualSpoutIdentifiers() {
        return nextVirtualSpoutIdGenerator.getAllNonThrottledVirtualSpoutIds();
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

        /**
         * @return All tracked VirtualSpoutIdentifiers.
         */
        public Set<VirtualSpoutIdentifier> getAllVirtualSpoutIds() {
            return allIds.keySet();
        }

        /**
         * @return All tracked VirtualSpoutIdentifiers that ARE throttled.
         */
        public Set<VirtualSpoutIdentifier> getAllThrottledVirtualSpoutIds() {
            Set<VirtualSpoutIdentifier> throttledVirtualSpoutIds = new HashSet<>();
            for (Map.Entry<VirtualSpoutIdentifier, Boolean> entry : allIds.entrySet()) {
                if (entry.getValue()) {
                    throttledVirtualSpoutIds.add(entry.getKey());
                }
            }
            return throttledVirtualSpoutIds;
        }

        /**
         * @return All tracked VirtualSpoutIdentifiers that are NOT throttled.
         */
        public Set<VirtualSpoutIdentifier> getAllNonThrottledVirtualSpoutIds() {
            Set<VirtualSpoutIdentifier> notThrottledVirtualSpoutIds = new HashSet<>();
            for (Map.Entry<VirtualSpoutIdentifier, Boolean> entry : allIds.entrySet()) {
                if (!entry.getValue()) {
                    notThrottledVirtualSpoutIds.add(entry.getKey());
                }
            }
            return notThrottledVirtualSpoutIds;
        }
    }
}
