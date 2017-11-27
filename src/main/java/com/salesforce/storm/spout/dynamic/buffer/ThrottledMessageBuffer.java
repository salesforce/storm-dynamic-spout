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

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.Message;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import com.salesforce.storm.spout.dynamic.config.DynamicSpoutConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Prototype ThrottledMessageBuffer based on blocking the producer/poll() method.
 *
 * This implementation should be considered "experimental" at this point as no real world testing has been done
 * on it yet.
 *
 * The way this works is you define a REGEX pattern to check against VirtualSpoutIdentifiers.
 * If a VirtualSpoutIdentifier MATCHES this REGEX, then we will enforce a lower buffer size for that Spout.
 *
 * Example:
 * With the following configuration
 *   - Regex pattern: /^throttle/
 *   - MaxBufferSize: 100
 *   - ThrottledBufferSize: 10
 *
 * VirtualSpoutId: NormalVirtualSpoutId
 * Effective BufferSize: 100
 * Result: Because the VirtualSpoutId does NOT match the REGEX pattern, we will enforce a buffer size limit of 100
 *         on this Spout.  This spout will be able to add up to 100 entries into the buffer, after that following
 *         put() calls will block until items are removed from the buffer.
 *
 * VirtualSpoutId: ThrottledVirtualSpoutId
 * Effective BufferSize: 10
 * Result: Because the VirtualSpoutId DOES match the REGEX pattern, we will enforce a buffer size limit of 10 on this
 *         spout.  This spout will be able to add up to 10 entries into the buffer, after that following put() calls
 *         will block until items are removed from the buffer.
 */
public class ThrottledMessageBuffer implements MessageBuffer {
    private static final Logger logger = LoggerFactory.getLogger(ThrottledMessageBuffer.class);

    /**
     * Config option for NON-throttled buffer size.
     */
    public static final String CONFIG_BUFFER_SIZE = DynamicSpoutConfig.TUPLE_BUFFER_MAX_SIZE;

    /**
     * Config option for throttled buffer size.
     */
    public static final String CONFIG_THROTTLE_BUFFER_SIZE = "spout.coordinator.tuple_buffer.throttled_buffer_size";

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
    private int throttledBufferSize = 200;

    // Match everything by default
    private Pattern regexPattern = Pattern.compile(".*");

    /**
     * An iterator over the Keys in buffer.  Used to Round Robin through the VirtualSpouts.
     */
    private Iterator<VirtualSpoutIdentifier> consumerIdIterator = null;

    public ThrottledMessageBuffer() {
    }

    /**
     * Helper method for creating a default instance.
     * @return factory method for create an instance of the buffer.
     */
    public static ThrottledMessageBuffer createDefaultInstance() {
        Map<String, Object> map = Maps.newHashMap();
        map.put(DynamicSpoutConfig.TUPLE_BUFFER_MAX_SIZE, 10000);
        map.put(CONFIG_THROTTLE_BUFFER_SIZE, 10);
        final SpoutConfig spoutConfig = new SpoutConfig(new ConfigDefinition(), map);

        ThrottledMessageBuffer buffer = new ThrottledMessageBuffer();
        buffer.open(spoutConfig);

        return buffer;
    }

    @Override
    public void open(final SpoutConfig spoutConfig) {
        // Setup non-throttled buffer size
        Object bufferSize = spoutConfig.get(DynamicSpoutConfig.TUPLE_BUFFER_MAX_SIZE);
        if (bufferSize != null && bufferSize instanceof Number) {
            maxBufferSize = ((Number) bufferSize).intValue();
        }

        // Setup throttled buffer size
        bufferSize = spoutConfig.get(CONFIG_THROTTLE_BUFFER_SIZE);
        if (bufferSize != null && bufferSize instanceof Number) {
            throttledBufferSize = ((Number) bufferSize).intValue();
        }

        // setup regex
        String regexPatternStr = (String) spoutConfig.get(CONFIG_THROTTLE_REGEX_PATTERN);
        if (regexPatternStr != null && !regexPatternStr.isEmpty()) {
            regexPattern = Pattern.compile(regexPatternStr);
        }
    }

    /**
     * Let the Implementation know that we're adding a new VirtualSpoutId.
     * @param virtualSpoutId - Identifier of new Virtual Spout.
     */
    @Override
    public void addVirtualSpoutId(final VirtualSpoutIdentifier virtualSpoutId) {
        synchronized (messageBuffer) {
            messageBuffer.putIfAbsent(virtualSpoutId, createBuffer(virtualSpoutId));
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
            // Attempt to put it
            messageBuffer.putIfAbsent(virtualSpoutId, createBuffer(virtualSpoutId));

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
        // If its null, or we hit the end, reset it.
        if (consumerIdIterator == null || !consumerIdIterator.hasNext()) {
            consumerIdIterator = messageBuffer.keySet().iterator();
        }

        // Try every buffer until we hit the end.
        Message returnMsg = null;
        while (returnMsg == null && consumerIdIterator.hasNext()) {

            // Advance iterator
            final VirtualSpoutIdentifier nextConsumerId = consumerIdIterator.next();

            // Find our buffer
            final BlockingQueue<Message> queue = messageBuffer.get(nextConsumerId);

            // We missed?
            if (queue == null) {
                logger.debug("Non-existent queue found, resetting iterator.");
                consumerIdIterator = messageBuffer.keySet().iterator();
                continue;
            }
            returnMsg = queue.poll();
        }
        return returnMsg;
    }

    /**
     * @return return a new LinkedBlockingQueue instance with a max size of our configured buffer.
     */
    private BlockingQueue<Message> createNewThrottledQueue() {
        return new LinkedBlockingQueue<>(getThrottledBufferSize());
    }

    /**
     * @return return a new LinkedBlockingQueue instance with a max size of our configured buffer.
     */
    private BlockingQueue<Message> createNewNonThrottledQueue() {
        return new LinkedBlockingQueue<>(getMaxBufferSize());
    }

    /**
     * @return The configured buffer size for non-throttled VirtualSpouts.
     */
    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    /**
     * @return The configured buffer size for throttled VirtualSpouts.
     */
    public int getThrottledBufferSize() {
        return throttledBufferSize;
    }

    /**
     * @return The configured Regex pattern to match throttled VirtualSpoutIds against.
     */
    public Pattern getRegexPattern() {
        return regexPattern;
    }

    /**
     * Internal method used *ONLY* within tests.  Hacky implementation -- could have race-conditions in other use-cases.
     * @return Set of all VirtualSpoutIds that ARE throttled.
     */
    Set<VirtualSpoutIdentifier> getThrottledVirtualSpoutIdentifiers() {
        Set<VirtualSpoutIdentifier> throttledVirtualSpoutIds = new HashSet<>();

        for (Map.Entry<VirtualSpoutIdentifier, BlockingQueue<Message>> entry: messageBuffer.entrySet()) {
            BlockingQueue<Message> queue = entry.getValue();
            if (queue.remainingCapacity() + queue.size() == getThrottledBufferSize()) {
                throttledVirtualSpoutIds.add(entry.getKey());
            }
        }
        return throttledVirtualSpoutIds;
    }

    /**
     * Internal method used *ONLY* within tests.  Hacky implementation -- could have race-conditions in other use-cases.
     * @return Set of all VirtualSpoutIds that are NOT throttled.
     */
    Set<VirtualSpoutIdentifier> getNonThrottledVirtualSpoutIdentifiers() {
        Set<VirtualSpoutIdentifier> nonThrottledVirtualSpoutIds = new HashSet<>();

        for (Map.Entry<VirtualSpoutIdentifier, BlockingQueue<Message>> entry: messageBuffer.entrySet()) {
            BlockingQueue<Message> queue = entry.getValue();
            if (queue.remainingCapacity() + queue.size() > getThrottledBufferSize()) {
                nonThrottledVirtualSpoutIds.add(entry.getKey());
            }
        }
        return nonThrottledVirtualSpoutIds;
    }

    private BlockingQueue<Message> createBuffer(final VirtualSpoutIdentifier virtualSpoutIdentifier) {
        // Match VirtualSpoutId against our regex pattern
        final Matcher matches = regexPattern.matcher(virtualSpoutIdentifier.toString());

        // If we match it
        if (matches.find()) {
            // Debug logging
            logger.debug("Added new VirtualSpoutId [{}] Throttled? {}", virtualSpoutIdentifier, true);

            // Create and return throttled queue.
            return createNewThrottledQueue();
        }
        // Debug logging
        logger.debug("Added new VirtualSpoutId [{}] Throttled? {}", virtualSpoutIdentifier, false);

        // Otherwise non-throttled.
        return createNewNonThrottledQueue();
    }
}
