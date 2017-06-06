package com.salesforce.storm.spout.sideline.buffer;

import com.salesforce.storm.spout.sideline.Message;
import com.salesforce.storm.spout.sideline.VirtualSpoutIdentifier;

import java.util.Map;

/**
 * This interface defines an abstraction around essentially a concurrent queue.
 * Abstracting this instead of using directly a queue object allows us to do things like
 * implement a "fairness" algorithm on the poll() method for pulling off of the queue.
 * Using a straight ConcurrentQueue would give us FIFO semantics (see {@link FIFOBuffer})
 * but with an abstraction we could implement round robin (see {@link RoundRobinBuffer}) across
 * VirtualSpouts or any scheduling algorithm that we'd like.
 */
public interface MessageBuffer {

    /**
     * Called prior to utilizing the instance.
     * @param spoutConfig - a copy of the storm topology config.
     */
    void open(Map spoutConfig);

    /**
     * Let the Implementation know that we're adding a new VirtualSpoutId.
     * @param virtualSpoutId - Identifier of new Virtual Spout.
     */
    void addVirtualSpoutId(final VirtualSpoutIdentifier virtualSpoutId);

    /**
     * Let the Implementation know that we're removing/cleaning up from closing a VirtualSpout.
     * @param virtualSpoutId - Identifier of Virtual Spout to be cleaned up.
     */
    void removeVirtualSpoutId(final VirtualSpoutIdentifier virtualSpoutId);

    /**
     * Put a new message onto the queue.  This method is blocking if the queue buffer is full.
     * @param message - Message to be added to the queue.
     * @throws InterruptedException - thrown if a thread is interrupted while blocked adding to the queue.
     */
    void put(final Message message) throws InterruptedException;

    /**
     * @return - return the size of the buffer.
     */
    int size();

    /**
     * @return - returns the next Message to be processed out of the queue.
     */
    Message poll();
}
