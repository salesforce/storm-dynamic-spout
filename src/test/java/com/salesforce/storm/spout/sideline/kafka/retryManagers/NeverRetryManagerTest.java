package com.salesforce.storm.spout.sideline.kafka.retryManagers;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.TupleMessageId;
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
        retryManager.open(Maps.newHashMap());

        // retryFurther always returns false
        assertFalse(retryManager.retryFurther(new TupleMessageId("MyTopic1", 1, 1L, "ConsumerId1")));
        assertFalse(retryManager.retryFurther(new TupleMessageId("MyTopic2", 2, 2L, "ConsumerId2")));
        assertFalse(retryManager.retryFurther(new TupleMessageId("MyTopic3", 3, 3L, "ConsumerId3")));
        assertFalse(retryManager.retryFurther(new TupleMessageId("MyTopic4", 4, 4L, "ConsumerId4")));

        // Call fail
        retryManager.failed(new TupleMessageId("MyTopic1", 1, 1L, "ConsumerId1"));
        retryManager.failed(new TupleMessageId("MyTopic2", 2, 2L, "ConsumerId2"));
        retryManager.failed(new TupleMessageId("MyTopic3", 3, 3L, "ConsumerId3"));
        retryManager.failed(new TupleMessageId("MyTopic4", 4, 4L, "ConsumerId4"));

        // nextFailedMessageToRetry should always return null.
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());
        assertNull(retryManager.nextFailedMessageToRetry());

        // Call acked started
        retryManager.acked(new TupleMessageId("MyTopic1", 1, 1L, "ConsumerId1"));
        retryManager.acked(new TupleMessageId("MyTopic2", 2, 2L, "ConsumerId2"));
        retryManager.acked(new TupleMessageId("MyTopic3", 3, 3L, "ConsumerId3"));
        retryManager.acked(new TupleMessageId("MyTopic4", 4, 4L, "ConsumerId4"));
    }
}