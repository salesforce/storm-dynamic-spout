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

package com.salesforce.storm.spout.dynamic.retry;

import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;

import java.util.Map;

/**
 * Interface for Tracking Failed messages.  By creating an implementation of this interface
 * you can control how the DynamicSpout deals with tuples that have failed within the topology.
 */
public interface RetryManager {

    /**
     * Initialization.
     * @param spoutConfig spout configuration.
     */
    void open(AbstractConfig spoutConfig);

    /**
     * Called for MessageId's that have failed to process within the Topology.
     * @param messageId id corresponding to the Message that has failed.
     */
    void failed(MessageId messageId);

    /**
     * Called for MessageId's that have successfully finished processing.
     * @param messageId id corresponding to the Message that has finished processing.
     */
    void acked(MessageId messageId);
    
    /**
     * Expected to return a MessageId of a previously failed Message that we want to replay.
     * If the implementation has no such MessageId that it wants to replay yet, simply return null.
     * @return id of a Message that we want to replay, or null if it has none to replay.
     */
    MessageId nextFailedMessageToRetry();

    /**
     * The Spout will call this for any MessageId that has failed to be processed by the topology.
     * If the implementation will want to retry this MessageId in the future it should return true here.
     * If the implementation returns false, the Spout will clean up its state around that Message, mark it
     * has having been completed successfully, and will be unable to replay it later.
     *
     * @param messageId id the spout wants to know if it will want to be replayed in the future.
     * @return true if the message should be retried again in the future. False otherwise.
     */
    boolean retryFurther(MessageId messageId);
}