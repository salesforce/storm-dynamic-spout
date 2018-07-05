/*
 * Copyright (c) 2018, Salesforce.com, Inc.
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

package com.salesforce.storm.spout.dynamic;

import com.salesforce.storm.spout.dynamic.buffer.FifoBuffer;
import com.salesforce.storm.spout.dynamic.buffer.MessageBuffer;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests the MessageBus service layer, responsible for passing messages across threads between
 * DynamicSpout and VirtualSpouts.
 */
public class MessageBusTest {

    /**
     * Test reporting errors from multiple threads.
     */
    @Test
    public void testReportingErrors() throws InterruptedException {
        // Create limits
        final int numThreads = 5;
        final int numExceptionsPerThread = 100;
        final int totalExceptions = (numThreads * numExceptionsPerThread);

        // Create bus
        final MessageBus messageBus = new MessageBus(mock(MessageBuffer.class));

        // Run in multiple threads
        final CountDownLatch latch = new CountDownLatch(numThreads);

        final Thread[] threads = new Thread[numThreads];
        for (int threadCount = 0; threadCount < numThreads; threadCount++) {
            threads[threadCount] = new Thread(() -> {
                latch.countDown();
                for (int counter = 0; counter < numExceptionsPerThread; counter++) {
                    messageBus.publishError(new Exception("Exception:" + counter));
                }
            });
            threads[threadCount].start();
        }

        // Wait for threads to wrap up.
        for (final Thread thread : threads) {
            thread.join();
        }

        // retrieve em
        final List<Throwable> foundThrowables = new ArrayList<>();
        for (int counter = 0; counter < totalExceptions + 75; counter++) {
            final Throwable nextError = messageBus.nextReportedError();
            if (nextError != null) {
                foundThrowables.add(nextError);
            }
        }

        // Validate
        assertEquals("Should have entries", totalExceptions, foundThrowables.size());
    }

    /**
     * Test publishing messages from multiple threads.
     */
    @Test
    public void testPublishMessages() throws InterruptedException {
        // Create limits
        final int numThreads = 5;
        final int numMessagesPerThread = 100;
        final int totalMessages = numMessagesPerThread * numThreads;

        // Create bus
        final MessageBus messageBus = new MessageBus(FifoBuffer.createDefaultInstance());

        // Run in multiple threads
        final CountDownLatch latch = new CountDownLatch(numThreads);

        final Thread[] threads = new Thread[numThreads];
        for (int threadCount = 0; threadCount < numThreads; threadCount++) {
            final String vspoutIdStr = "VSpout" + threadCount;
            threads[threadCount] = new Thread(() -> {
                final VirtualSpoutIdentifier vspoutId = new DefaultVirtualSpoutIdentifier(vspoutIdStr);
                latch.countDown();
                for (int counter = 0; counter < numMessagesPerThread; counter++) {
                    final Message message = new Message(new MessageId("", 1, counter, vspoutId), new Values());
                    try {
                        messageBus.publishMessage(message);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }
            });
            threads[threadCount].start();
        }

        // Wait for threads to wrap up.
        for (final Thread thread : threads) {
            thread.join();
        }

        // Validate
        final Map<VirtualSpoutIdentifier, Set<Long>> vSpoutToOffset = new HashMap<>();
        for (int counter = 0; counter < totalMessages + 25; counter++) {
            final Message message = messageBus.nextMessage();
            if (message == null) {
                continue;
            }
            VirtualSpoutIdentifier identifier = message.getMessageId().getSrcVirtualSpoutId();
            if (!vSpoutToOffset.containsKey(identifier)) {
                vSpoutToOffset.put(identifier, new HashSet<>());
            }
            vSpoutToOffset.get(identifier).add(message.getMessageId().getOffset());
        }

        assertEquals("Should have 5 vspoutIds", numThreads, vSpoutToOffset.size());
        for (Set<Long> entries : vSpoutToOffset.values()) {
            assertEquals("SHould have 100 messages", numMessagesPerThread, entries.size());
        }
    }

    /**
     * Test acking messages from multiple threads.
     */
    @Test
    public void testAcking() throws InterruptedException {
        // Create limits
        final int numThreads = 5;
        final int numAcksPerThread = 100;

        // Create bus
        final MessageBus messageBus = new MessageBus(mock(MessageBuffer.class));

        // Register virtualSpoutIds first
        for (int spoutCounter = 0; spoutCounter < numThreads; spoutCounter++) {
            final VirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("VSpout" + spoutCounter);
            messageBus.registerVirtualSpout(virtualSpoutIdentifier);
        }

        // Run in multiple threads
        final CountDownLatch latch = new CountDownLatch(numThreads + 1);

        final Thread publisherThread = new Thread(() -> {
            latch.countDown();
            // Insert a bunch of acks from a single thread.
            for (int spoutCounter = 0; spoutCounter < numThreads; spoutCounter++) {
                final VirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("VSpout" + spoutCounter);
                for (int ackCounter = 0; ackCounter < numAcksPerThread; ackCounter++) {
                    final MessageId messageId = new MessageId("", 0, ackCounter, virtualSpoutIdentifier);
                    messageBus.ack(messageId);
                }
            }
        });
        publisherThread.start();

        final Map<VirtualSpoutIdentifier, Set<Long>> resultMap = new ConcurrentHashMap<>();

        // Create consumer threads
        final Thread[] threads = new Thread[numThreads];
        for (int threadCount = 0; threadCount < numThreads; threadCount++) {
            final String vspoutIdStr = "VSpout" + threadCount;
            threads[threadCount] = new Thread(() -> {
                final VirtualSpoutIdentifier vspoutId = new DefaultVirtualSpoutIdentifier(vspoutIdStr);
                final Set<Long> offsets = new HashSet<>();
                latch.countDown();
                do {
                    final MessageId messageId = messageBus.getAckedMessage(vspoutId);
                    if (messageId == null) {
                        continue;
                    }
                    offsets.add(messageId.getOffset());
                } while (offsets.size() < numAcksPerThread);
                resultMap.put(vspoutId, offsets);
            });
            threads[threadCount].start();
        }

        // Wait for threads to wrap up.
        for (final Thread thread : threads) {
            thread.join();
        }

        // Validate
        assertEquals("Should have 5 vspoutIds", numThreads, resultMap.size());
        for (int spoutCounter = 0; spoutCounter < numThreads; spoutCounter++) {
            final VirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("VSpout" + spoutCounter);
            assertEquals("Should have 100 entries", numAcksPerThread, resultMap.get(virtualSpoutIdentifier).size());
        }
    }

    /**
     * Test Failing messages from multiple threads.
     */
    @Test
    public void testFailing() throws InterruptedException {
        // Create limits
        final int numThreads = 5;
        final int numFailsPerThread = 100;

        // Create bus
        final MessageBus messageBus = new MessageBus(mock(MessageBuffer.class));

        // Register virtualSpoutIds first
        for (int spoutCounter = 0; spoutCounter < numThreads; spoutCounter++) {
            final VirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("VSpout" + spoutCounter);
            messageBus.registerVirtualSpout(virtualSpoutIdentifier);
        }

        // Run in multiple threads
        final CountDownLatch latch = new CountDownLatch(numThreads + 1);

        final Thread publisherThread = new Thread(() -> {
            latch.countDown();
            // Insert a bunch of acks from a single thread.
            for (int spoutCounter = 0; spoutCounter < numThreads; spoutCounter++) {
                final VirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("VSpout" + spoutCounter);
                for (int ackCounter = 0; ackCounter < numFailsPerThread; ackCounter++) {
                    final MessageId messageId = new MessageId("", 0, ackCounter, virtualSpoutIdentifier);
                    messageBus.fail(messageId);
                }
            }
        });
        publisherThread.start();

        final Map<VirtualSpoutIdentifier, Set<Long>> resultMap = new ConcurrentHashMap<>();

        // Create consumer threads
        final Thread[] threads = new Thread[numThreads];
        for (int threadCount = 0; threadCount < numThreads; threadCount++) {
            final String vspoutIdStr = "VSpout" + threadCount;
            threads[threadCount] = new Thread(() -> {
                final VirtualSpoutIdentifier vspoutId = new DefaultVirtualSpoutIdentifier(vspoutIdStr);
                final Set<Long> offsets = new HashSet<>();
                latch.countDown();
                do {
                    final MessageId messageId = messageBus.getFailedMessage(vspoutId);
                    if (messageId == null) {
                        continue;
                    }
                    offsets.add(messageId.getOffset());
                } while (offsets.size() < numFailsPerThread);
                resultMap.put(vspoutId, offsets);
            });
            threads[threadCount].start();
        }

        // Wait for threads to wrap up.
        for (final Thread thread : threads) {
            thread.join();
        }

        // Validate
        assertEquals("Should have 5 vspoutIds", numThreads, resultMap.size());
        for (int spoutCounter = 0; spoutCounter < numThreads; spoutCounter++) {
            final VirtualSpoutIdentifier virtualSpoutIdentifier = new DefaultVirtualSpoutIdentifier("VSpout" + spoutCounter);
            assertEquals("Should have 100 entries", numFailsPerThread, resultMap.get(virtualSpoutIdentifier).size());
        }
    }

    /**
     * Test unregister removes entries.
     */
    @Test
    public void testUnregister() {
        final MessageBuffer mockBuffer = mock(MessageBuffer.class);
        final MessageBus messageBus = new MessageBus(mockBuffer);

        final VirtualSpoutIdentifier vspoutId = new DefaultVirtualSpoutIdentifier("id");

        // Register
        messageBus.registerVirtualSpout(vspoutId);
        verify(mockBuffer, times(1)).addVirtualSpoutId(vspoutId);

        // Unregister
        messageBus.unregisterVirtualSpout(vspoutId);
        verify(mockBuffer, times(1)).removeVirtualSpoutId(vspoutId);
    }

    /**
     * Validates nothing bad happens when we attempt to ack a messageId for an unregistered VirtualSpoutId.
     */
    @Test
    public void testAckWithUnknownVirtualSpoutId() {
        final MessageBuffer mockBuffer = mock(MessageBuffer.class);
        final MessageBus messageBus = new MessageBus(mockBuffer);

        final VirtualSpoutIdentifier vspoutId = new DefaultVirtualSpoutIdentifier("id");
        final MessageId messageId = new MessageId("Topic", 1, 123L, vspoutId);

        // Call ack when our VSpout hasn't been registered.
        messageBus.ack(messageId);
    }

    /**
     * Validates nothing bad happens when we attempt to fail a messageId for an unregistered VirtualSpoutId.
     */
    @Test
    public void testFailWithUnknownVirtualSpoutId() {
        final MessageBuffer mockBuffer = mock(MessageBuffer.class);
        final MessageBus messageBus = new MessageBus(mockBuffer);

        final VirtualSpoutIdentifier vspoutId = new DefaultVirtualSpoutIdentifier("id");
        final MessageId messageId = new MessageId("Topic", 1, 123L, vspoutId);

        // Call fail when our VSpout hasn't been registered.
        messageBus.fail(messageId);
    }

    /**
     * Validates nothing bad happens when we attempt to fail a messageId for an unregistered VirtualSpoutId.
     */
    @Test
    public void testUnregisterWithUnknownVirtualSpoutId() {
        final MessageBuffer mockBuffer = mock(MessageBuffer.class);
        final MessageBus messageBus = new MessageBus(mockBuffer);

        final VirtualSpoutIdentifier vspoutId = new DefaultVirtualSpoutIdentifier("id");

        // Call unregister when our VSpout hasn't been registered.
        messageBus.unregisterVirtualSpout(vspoutId);
    }
}