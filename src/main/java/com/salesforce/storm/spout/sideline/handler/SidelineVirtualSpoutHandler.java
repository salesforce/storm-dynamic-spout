package com.salesforce.storm.spout.sideline.handler;

import com.salesforce.storm.spout.sideline.ConsumerPartition;
import com.salesforce.storm.spout.sideline.DelegateSpout;
import com.salesforce.storm.spout.sideline.SidelineVirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.consumer.Consumer;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for sideline virtual spouts.
 */
public class SidelineVirtualSpoutHandler implements VirtualSpoutHandler {

    // Logger for logging
    private static final Logger logger = LoggerFactory.getLogger(SidelineVirtualSpoutHandler.class);

    /**
     * Handler for when a sideline spout is completed.  When a sideline spout completes clean up the metadata for the
     * given sideline request.
     * @param virtualSpout Virtual Spout instance that is completing..
     */
    @Override
    public void onVirtualSpoutCompletion(DelegateSpout virtualSpout) {
        try {
            // Get the specific sideline request from the virtual spout's id
            final SidelineRequestIdentifier sidelineRequestIdentifier = ((SidelineVirtualSpoutIdentifier) virtualSpout.getVirtualSpoutId()).getSidelineRequestIdentifier();

            // We can only do this if we have a starting state and a sideline request identifier
            if (sidelineRequestIdentifier != null && virtualSpout.getStartingState() != null) {
                // Clean up sideline request
                for (final ConsumerPartition consumerPartition : virtualSpout.getStartingState().getConsumerPartitions()) {
                    final Consumer consumer = virtualSpout.getConsumer();
                    final PersistenceAdapter persistenceAdapter = consumer.getPersistenceAdapter();

                    persistenceAdapter.clearSidelineRequest(
                        sidelineRequestIdentifier,
                        consumerPartition.partition()
                    );
                }
            }
        } catch (Exception ex) {
            logger.error("I was unable to completion the virtual spout for {} {}", virtualSpout.getVirtualSpoutId(), ex);
        }
    }
}
