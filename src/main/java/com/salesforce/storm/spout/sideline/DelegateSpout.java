package com.salesforce.storm.spout.sideline;

import com.salesforce.storm.spout.sideline.consumer.ConsumerState;
import com.salesforce.storm.spout.sideline.consumer.Consumer;

/**
 * A virtualized spout that is run from within the DyanmicSpout by the SpoutCoordinator.
 */
public interface DelegateSpout {

    /**
     * Open the spout.
     */
    void open();

    /**
     * Close the spout.
     */
    void close();

    /**
     * Get the next message from the spout.
     * @return Message to pass along to Storm.
     */
    Message nextTuple();

    /**
     * Acknowledge a message that came from this spout.
     * @param msgId Message id to acknowledge.
     */
    void ack(Object msgId);

    /**
     * Fail a message that from this spout.
     * @param msgId Message id to fail.
     */
    void fail(Object msgId);

    /**
     * Get this spout's unique identifier.
     * @return A unique VirtualSpoutIdentifier instance.
     */
    VirtualSpoutIdentifier getVirtualSpoutId();

    /**
     * Flush the state of the current spout.
     */
    void flushState();

    /**
     * Request that the current spout stop running.
     */
    void requestStop();

    /**
     * Has the current spout been told to stop?
     * @return True if it has, false if it has not.
     */
    boolean isStopRequested();

    /**
     * Get the current ConsumerState from this spout's Consumer instance.
     * @return Current ConsumerState.
     */
    ConsumerState getCurrentState();

    /**
     * Get the starting ConsumerState that this spout was spun up with.
     * @return Starting ConsumerState.
     */
    ConsumerState getStartingState();

    /**
     * Get the ending ConsumerState that this spout was spun up with.
     * @return Ending ConsumerState.
     */
    ConsumerState getEndingState();

    /**
     * Used by SpoutPartitionProgressMonitor to find the max lag of any partitions in the consumer for the current
     * virtual spout.
     * @todo This should be revisited, this feels out of place in the interface.
     * @return Max lag.
     */
    double getMaxLag();

    /**
     * Get the number of filters applied to spout's filter chain. Used for metrics in the spout monitor.
     * @todo Should we drop this metric? This feels out of place in the interface.
     * @return Number of filters applied.
     */
    int getNumberOfFiltersApplied();

    /**
     * Get the Consumer this spout is using to pull messages from.
     * @return Consumer instance.
     */
    Consumer getConsumer();
}
