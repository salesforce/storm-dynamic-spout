package com.salesforce.storm.spout.dynamic;

import com.salesforce.storm.spout.dynamic.exception.SpoutAlreadyExistsException;
import com.salesforce.storm.spout.dynamic.exception.SpoutDoesNotExistException;

import java.util.Map;
import java.util.Optional;

/**
 * Facade in front of Coordinator reducing available scope.
 *
 * These are all of the methods available to the Spout instance.
 */
public interface SpoutCoordinator {
    /**
     * Open the coordinator and begin spinning up virtual spout threads.
     * @param config topology configuration.
     */
    void open(final Map<String, Object> config);

    /**
     * @return Returns any errors that should be reported up to the topology.
     */
    Optional<Throwable> getErrors();

    /**
     * @return Returns the next available Message to be emitted into the topology.
     */
    Optional<Message> nextMessage();

    /**
     * Stop managed spouts, calling this should shut down and finish the coordinator's spouts.
     */
    void close();

    /**
     * Acks a tuple on the spout that it belongs to.
     * @param id Tuple message id to ack
     */
    void ack(final MessageId id);

    /**
     * Fails a tuple on the spout that it belongs to.
     * @param id Tuple message id to fail
     */
    void fail(final MessageId id);

    /**
     * Add a new VirtualSpout to the coordinator, this will get picked up by the coordinator's monitor, opened and
     * managed with teh other currently running spouts.
     * @param spout New delegate spout
     * @throws SpoutAlreadyExistsException if a spout already exists with the same VirtualSpoutIdentifier.
     */
    void addVirtualSpout(final DelegateSpout spout) throws SpoutAlreadyExistsException;

    /**
     * Remove a new VirtualSpout from the coordinator. This will signal to the monitor to request that the VirtualSpout
     * be stopped and ultimately removed.
     *
     * This method will blocked until the VirtualSpout has completely stopped.
     *
     * @param virtualSpoutIdentifier identifier of the VirtualSpout to be removed.
     * @throws SpoutDoesNotExistException If no VirtualSpout exists with the VirtualSpoutIdentifier.
     */
    void removeVirtualSpout(final VirtualSpoutIdentifier virtualSpoutIdentifier) throws SpoutDoesNotExistException;

    /**
     * Check if a given spout already exists in the spout coordinator.
     * @param spoutIdentifier spout identifier to check the coordinator for.
     * @return true when the spout exists, false when it does not.
     */
    boolean hasVirtualSpout(final VirtualSpoutIdentifier spoutIdentifier);
}
