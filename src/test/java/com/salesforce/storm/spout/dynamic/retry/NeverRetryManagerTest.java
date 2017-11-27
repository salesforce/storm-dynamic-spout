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
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Test over the No Retry Failed Msg manager.
 */
public class NeverRetryManagerTest {

    /**
     * Mostly for lame test coverage.
     */
    @Test
    public void testShouldReEmitMsg() {
        // Create instance.
        NeverRetryManager retryManager = new NeverRetryManager();

        final DefaultVirtualSpoutIdentifier consumerId1 = new DefaultVirtualSpoutIdentifier("ConsumerId1");
        final DefaultVirtualSpoutIdentifier consumerId2 = new DefaultVirtualSpoutIdentifier("ConsumerId2");
        final DefaultVirtualSpoutIdentifier consumerId3 = new DefaultVirtualSpoutIdentifier("ConsumerId3");
        final DefaultVirtualSpoutIdentifier consumerId4 = new DefaultVirtualSpoutIdentifier("ConsumerId4");

        retryManager.open(new SpoutConfig(new ConfigDefinition(), new HashMap<>()));

        // retryFurther always returns false
        assertFalse(retryManager.retryFurther(new MessageId("MyTopic1", 1, 1L, consumerId1)));
        assertFalse(retryManager.retryFurther(new MessageId("MyTopic2", 2, 2L, consumerId2)));
        assertFalse(retryManager.retryFurther(new MessageId("MyTopic3", 3, 3L, consumerId3)));
        assertFalse(retryManager.retryFurther(new MessageId("MyTopic4", 4, 4L, consumerId4)));

        // Call fail
        retryManager.failed(new MessageId("MyTopic1", 1, 1L, consumerId1));
        retryManager.failed(new MessageId("MyTopic2", 2, 2L, consumerId2));
        retryManager.failed(new MessageId("MyTopic3", 3, 3L, consumerId3));
        retryManager.failed(new MessageId("MyTopic4", 4, 4L, consumerId4));

        // nextFailedMessageToRetry should always return null.
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());

        // Call acked started
        retryManager.acked(new MessageId("MyTopic1", 1, 1L, consumerId1));
        retryManager.acked(new MessageId("MyTopic2", 2, 2L, consumerId2));
        retryManager.acked(new MessageId("MyTopic3", 3, 3L, consumerId3));
        retryManager.acked(new MessageId("MyTopic4", 4, 4L, consumerId4));
    }
}