package com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers;

import com.salesforce.storm.spout.sideline.TupleMessageId;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Interface for Tracking Failed messages.
 */
public interface FailedMsgRetryManager extends Serializable {

    /**
     * Initialization.
     */
    void prepare(Map stormConfig);

    /**
     * Message corresponding to the offset failed in kafka spout.
     * @param messageId
     */
    void failed(TupleMessageId messageId);

    /**
     * Message corresponding to the offset, was acked to kafka spout.
     * @param messageId
     */
    void acked(TupleMessageId messageId);

    /**
     * Message corresponding to the offset, has been re-emitted and under transit.
     * @param messageId
     */
    void retryStarted(TupleMessageId messageId);

    /**
     * The offset of message, which is to be re-emitted. Spout will fetch messages starting from this offset
     * and resend them, except completed messages.
     * @return
     */
    TupleMessageId nextFailedMessageToRetry();

    /**
     * @param messageId
     * @return True if the message corresponding to the offset should be emitted NOW. False otherwise.
     */
    boolean shouldReEmitMsg(TupleMessageId messageId);

    /**
     * Spout will clean up the state for this offset if false is returned.
     * @param offset
     * @return True if the message will be retried again. False otherwise.
     */
    boolean retryFurther(Long offset);

    /**
     * Clear any offsets before kafkaOffset. These offsets are no longer available in kafka.
     * @param messageId
     * @return Set of offsets removed.
     */
    Set<TupleMessageId> clearOffsetsBefore(TupleMessageId messageId);
}