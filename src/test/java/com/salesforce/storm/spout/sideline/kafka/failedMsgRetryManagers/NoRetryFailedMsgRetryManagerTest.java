package com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers;

import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test over the No Retry Failed Msg manager.
 */
public class NoRetryFailedMsgRetryManagerTest {

    /**
     * Mostly for lame test coverage.
     */
    @Test
    public void testShouldReEmitMsg() {
        // Create instance.
        FailedMsgRetryManager retryManager = new NoRetryFailedMsgRetryManager();
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

        // Call retry started
        retryManager.retryStarted(new TupleMessageId("MyTopic1", 1, 1L, "ConsumerId1"));
        retryManager.retryStarted(new TupleMessageId("MyTopic2", 2, 2L, "ConsumerId2"));
        retryManager.retryStarted(new TupleMessageId("MyTopic3", 3, 3L, "ConsumerId3"));
        retryManager.retryStarted(new TupleMessageId("MyTopic4", 4, 4L, "ConsumerId4"));

        // Call acked started
        retryManager.acked(new TupleMessageId("MyTopic1", 1, 1L, "ConsumerId1"));
        retryManager.acked(new TupleMessageId("MyTopic2", 2, 2L, "ConsumerId2"));
        retryManager.acked(new TupleMessageId("MyTopic3", 3, 3L, "ConsumerId3"));
        retryManager.acked(new TupleMessageId("MyTopic4", 4, 4L, "ConsumerId4"));
    }
}