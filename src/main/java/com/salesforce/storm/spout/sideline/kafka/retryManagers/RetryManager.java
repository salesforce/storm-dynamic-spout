package com.salesforce.storm.spout.sideline.kafka.retryManagers;

import com.salesforce.storm.spout.sideline.TupleMessageId;

import java.util.Map;

/**
 * Interface for Tracking Failed messages.  By creating an implementation of this interface
 * you can control how SidelineSpout deals with tuples that have failed within the topology.
 */
public interface RetryManager {

    /**
     * Initialization.
     */
    void open(Map stormConfig);

    /**
     * Called for TupleMessageId's that have failed to process within the Topology.
     * @param messageId - MessageId corresponding to the KafkaMessage that has failed.
     */
    void failed(TupleMessageId messageId);

    /**
     * Called for TupleMessageId's that have successfully finished processing.
     * @param messageId - MessageId corresponding to the KafkaMessage that has finished processing.
     */
    void acked(TupleMessageId messageId);
    
    /**
     * Expected to return a TupleMessageId of a previously failed KafkaMessage that we want to replay.
     * If the implementation has no such TupleMessageId that it wants to replay yet, simply return null.
     * @return - TupleMessageId of a KafkaMessage that we want to replay, or null if it has none to replay.
     */
    TupleMessageId nextFailedMessageToRetry();

    /**
     * The Spout will call this for any TupleMessageId that has failed to be processed by the topology.
     * If the implementation will want to retry this TupleMessageId in the future it should return true here.
     * If the implementation returns false, the Spout will clean up its state around that KafkaMessage, mark it
     * has having been completed successfully, and will be unable to replay it later.
     *
     * @param messageId - The TupleMessageId the spout wants to know if it will want to be replayed in the future.
     * @return True if the message should be retried again in the future. False otherwise.
     */
    boolean retryFurther(TupleMessageId messageId);
}