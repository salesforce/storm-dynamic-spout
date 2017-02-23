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
     * This should always return false.
     */
    @Test
    public void testShouldReEmitMsg() {
        // Create instance.
        FailedMsgRetryManager retryManager = new NoRetryFailedMsgRetryManager();
        retryManager.prepare(Maps.newHashMap());

        assertFalse(retryManager.retryFurther(new TupleMessageId("MyTopic1", 1, 1L, "ConsumerId1")));
        assertFalse(retryManager.retryFurther(new TupleMessageId("MyTopic2", 2, 2L, "ConsumerId2")));
        assertFalse(retryManager.retryFurther(new TupleMessageId("MyTopic3", 3, 3L, "ConsumerId3")));
        assertFalse(retryManager.retryFurther(new TupleMessageId("MyTopic4", 4, 4L, "ConsumerId4å")));
    }
}