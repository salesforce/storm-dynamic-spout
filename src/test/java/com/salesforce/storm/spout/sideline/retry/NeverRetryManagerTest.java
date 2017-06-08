package com.salesforce.storm.spout.sideline.retry;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.MessageId;
import com.salesforce.storm.spout.sideline.DefaultVirtualSpoutIdentifier;
import org.junit.Test;

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

        retryManager.open(Maps.newHashMap());

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