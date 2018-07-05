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

/**
 * Facade in front of MessageBus reducing available scope down to only the methods
 * that should be available to virtual spouts.
 */
public interface VirtualSpoutMessageBus {

    /**
     * Registers a new VirtualSpout with the bus.
     * @param virtualSpoutIdentifier identifier to register
     */
    void registerVirtualSpout(final VirtualSpoutIdentifier virtualSpoutIdentifier);

    /**
     * Publish a new message onto the bus.
     * Depending on the implementation, this *may* be blocking.
     * @param message message to publish
     * @throws InterruptedException if the operation is interrupted
     */
    void publishMessage(final Message message) throws InterruptedException;

    /**
     * Get the number of message on the bus.
     * @return number of message on the bus
     */
    int messageSize();

    /**
     * Publish an error to the bus.
     * @param throwable error to publish
     */
    void publishError(final Throwable throwable);

    /**
     * Get next acked messageId for the given VirtualSpout.
     * This method should never block, but instead return NULL if none exists.
     *
     * @param virtualSpoutIdentifier Identifier to retrieve the next messageId for.
     * @return next available MessageId, or NULL
     */
    MessageId getAckedMessage(final VirtualSpoutIdentifier virtualSpoutIdentifier);

    /**
     * Get next failed messageId for the given VirtualSpout.
     * This method should never block, but instead return a NULL if none exists.
     *
     * @param virtualSpoutIdentifier Identifier to retrieve the next messageId for.
     * @return next available MessageId, or NULL
     */
    MessageId getFailedMessage(final VirtualSpoutIdentifier virtualSpoutIdentifier);

    /**
     * Called to unregister a VirtualSpout.
     * @param virtualSpoutIdentifier identifier to unregister.
     */
    void unregisterVirtualSpout(final VirtualSpoutIdentifier virtualSpoutIdentifier);
}
