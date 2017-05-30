package com.salesforce.storm.spout.sideline.retry;

import com.salesforce.storm.spout.sideline.MessageId;

import java.util.Map;

/**
 * Interface for Tracking Failed messages.  By creating an implementation of this interface
 * you can control how SidelineSpout deals with tuples that have failed within the topology.
 */
public interface RetryManager {

    /**
     * Initialization.
     */
    void open(Map spoutConfig);

    /**
     * Called for MessageId's that have failed to process within the Topology.
     * @param messageId - MessageId corresponding to the Message that has failed.
     */
    void failed(MessageId messageId);

    /**
     * Called for MessageId's that have successfully finished processing.
     * @param messageId - MessageId corresponding to the Message that has finished processing.
     */
    void acked(MessageId messageId);
    
    /**
     * Expected to return a MessageId of a previously failed Message that we want to replay.
     * If the implementation has no such MessageId that it wants to replay yet, simply return null.
     * @return - MessageId of a Message that we want to replay, or null if it has none to replay.
     */
    MessageId nextFailedMessageToRetry();

    /**
     * The Spout will call this for any MessageId that has failed to be processed by the topology.
     * If the implementation will want to retry this MessageId in the future it should return true here.
     * If the implementation returns false, the Spout will clean up its state around that Message, mark it
     * has having been completed successfully, and will be unable to replay it later.
     *
     * @param messageId - The MessageId the spout wants to know if it will want to be replayed in the future.
     * @return True if the message should be retried again in the future. False otherwise.
     */
    boolean retryFurther(MessageId messageId);
}