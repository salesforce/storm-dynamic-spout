/**
 * Copyright (c) 2017, Salesforce.com, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 *   disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * * Neither the name of Salesforce.com nor the names of its contributors may be used to endorse or promote products
 *   derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
 * USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.salesforce.storm.spout.dynamic.coordinator;

import com.salesforce.storm.spout.dynamic.Tools;
import com.salesforce.storm.spout.dynamic.VirtualSpoutMessageBus;
import com.salesforce.storm.spout.dynamic.VirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.DelegateSpout;
import com.salesforce.storm.spout.dynamic.VirtualSpout;
import com.salesforce.storm.spout.dynamic.metrics.MetricsRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Monitors and manages the lifecycle of virtual spouts.
 */
public class SpoutMonitor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(SpoutMonitor.class);

    /**
     * How often our monitor thread will output a status report, in milliseconds.
     */
    private static final long REPORT_STATUS_INTERVAL_MS = 60_000;

    /**
     * The executor service that we submit VirtualSpouts to run within.
     */
    private final ThreadPoolExecutor executor;

    /**
     * This Queue contains requests to our thread to fire up new VirtualSpouts.
     * Instances are taken off of this queue and put into the ExecutorService's task queue.
     */
    private final Queue<DelegateSpout> newSpoutQueue;

    /**
     * Routes messages in a ThreadSafe manner from VirtualSpouts to the SpoutCoordinator.
     */
    private final VirtualSpoutMessageBus virtualSpoutMessageBus;

    /**
     * This latch allows the SpoutCoordinator to block on start up until its initial
     * set of VirtualSpout instances have started.
     */
    private final CountDownLatch latch;

    /**
     * Used to get the System time, allows easy mocking of System clock in tests.
     */
    private final Clock clock;

    /**
     * Storm topology configuration.
     */
    private final Map<String, Object> topologyConfig;

    /**
     * Metrics Recorder implementation for collecting metrics.
     */
    private final MetricsRecorder metricsRecorder;

    /**
     * Used to track metric for number of failed virtual spouts.
     */
    private final AtomicInteger failedTaskCounter = new AtomicInteger(0);

    /**
     * Calculates progress of {@link VirtualSpout} instances.
     */
    private SpoutPartitionProgressMonitor spoutPartitionProgressMonitor;

    private final Map<VirtualSpoutIdentifier, SpoutRunner> spoutRunners = new ConcurrentHashMap<>();

    /**
     * Flag used to determine if we should stop running or not.
     */
    private boolean keepRunning = true;

    /**
     * The last timestamp of a status report.
     */
    private long lastStatusReport = 0;

    /**
     * Constructor.
     * @param newSpoutQueue Queue monitored for new Spouts that should be started.
     * @param virtualSpoutMessageBus ThreadSafe message bus for passing messages between DynamicSpout and VirtualSpouts.
     * @param latch Latch to allow startup synchronization.
     * @param clock Which clock instance to use, allows injecting a mock clock.
     * @param topologyConfig Storm topology config.
     * @param metricsRecorder MetricRecorder implementation for recording metrics.
     */
    public SpoutMonitor(
        final Queue<DelegateSpout> newSpoutQueue,
        final VirtualSpoutMessageBus virtualSpoutMessageBus,
        final CountDownLatch latch,
        final Clock clock,
        final Map<String, Object> topologyConfig,
        final MetricsRecorder metricsRecorder
    ) {
        this.newSpoutQueue = newSpoutQueue;
        this.virtualSpoutMessageBus = virtualSpoutMessageBus;
        this.latch = latch;
        this.clock = clock;
        this.topologyConfig = Tools.immutableCopy(topologyConfig);
        this.metricsRecorder = metricsRecorder;

        /*
         * Create our executor service with a fixed thread size.
         * Its configured to:
         *   - Time out idle threads after 1 minute
         *   - Keep at most 0 idle threads alive (after timing out).
         *   - Maximum of SPOUT_RUNNER_THREAD_POOL_SIZE threads running concurrently.
         *   - Use essentially an unbounded task queue.
         */
        this.executor = new ThreadPoolExecutor(
            // Number of idle threads to keep around
            getMaxConcurrentVirtualSpouts(),
            // Maximum number of threads to utilize
            getMaxConcurrentVirtualSpouts(),
            // How long to keep idle threads around for before closing them
            1L, TimeUnit.MINUTES,
            // Task input queue
            new LinkedBlockingQueue<>()
        );
    }

    /**
     * Check if the spout monitor contains a spout by the given identifier.
     * @param virtualSpoutIdentifier Identifier of the spout we're looking for.
     * @return Whether or not a spout with that identifier exists.
     */
    public boolean hasVirtualSpout(final VirtualSpoutIdentifier virtualSpoutIdentifier) {
        // Synchronized on new spout queue
        synchronized (newSpoutQueue) {
            for (final DelegateSpout spout : newSpoutQueue) {
                if (spout.getVirtualSpoutId().equals(virtualSpoutIdentifier)) {
                    return true;
                }
            }
            return spoutRunners.containsKey(virtualSpoutIdentifier);
        }
    }

    /**
     * Signals to a VirtualSpout to stop, ultimately removing it from the monitor.
     * @param virtualSpoutIdentifier identifier of the VirtualSpout instance to request stopped.
     */
    public void removeVirtualSpout(final VirtualSpoutIdentifier virtualSpoutIdentifier) {
        if (!hasVirtualSpout(virtualSpoutIdentifier)) {
            throw new IllegalArgumentException(
                "VirtualSpout " + virtualSpoutIdentifier + " does not exist in the SpoutMonitor."
            );
        }

        // Request the spout stop
        spoutRunners.get(virtualSpoutIdentifier).requestStop();
    }

    @Override
    public void run() {
        try {
            // Rename our thread.
            Thread.currentThread().setName("SpoutMonitor");

            // Start monitoring loop.
            while (keepRunning()) {
                // Look for new VirtualSpouts that need to be started
                startNewSpoutTasks();

                // Periodically report status
                reportStatus();

                // Pause for a period before checking for more spouts
                try {
                    Thread.sleep(getMonitorThreadIntervalMs());
                } catch (final InterruptedException ex) {
                    logger.warn("Thread interrupted, shutting down...");
                    return;
                }
            }
            logger.warn("Spout monitor is ceasing to run due to shutdown request...");
        } catch (final Exception ex) {
            // We handle restarting spout monitor in the coordinator, which is who monitors this thread.
            // Lets report the error
            reportError(ex);

            // Log it.
            logger.error("SpoutMonitor threw an exception {}", ex.getMessage(), ex);

            // And bubble it up
            throw ex;
        }
    }

    /**
     * Submit any new spout tasks as they show up.
     */
    private void startNewSpoutTasks() {
        // Synchronized access to this queue.
        synchronized (newSpoutQueue) {
            // Look for new spouts to start.
            for (DelegateSpout spout; (spout = getNewSpoutQueue().poll()) != null; ) {
                logger.info("Preparing thread for spout {}", spout.getVirtualSpoutId());

                final VirtualSpoutIdentifier virtualSpoutIdentifier = spout.getVirtualSpoutId();

                final SpoutRunner spoutRunner = new SpoutRunner(
                    spout,
                    getVirtualSpoutMessageBus(),
                    latch,
                    getClock(),
                    getTopologyConfig()
                );

                // This maps VirtualSpout Ids to SpoutRunner Instances.
                // Really we only use this to keep track of which VirtualSpoutIds we have running.
                spoutRunners.put(spout.getVirtualSpoutId(), spoutRunner);

                // Run as a CompletableFuture
                final CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(spoutRunner, getExecutor());

                // Handle when this spout completes.
                // It can either complete successfully, or via some kind of error, we'll handle both here.
                completableFuture.handle((final Void result, final Throwable exception) -> {
                    // If we got passed an exception
                    if (exception != null) {
                        // We failed via some kind of error, lets report it
                        if (exception instanceof CompletionException) {
                            // CompletionException is a wrapper, lets report the cause.
                            reportError(exception.getCause());
                        } else {
                            // I have a feeling all exceptions will be of type CompletionException.
                            reportError(exception);
                        }

                        // Log error
                        logger.error(
                            "An exception has occurred in the SpoutRunner for {} {}",
                            virtualSpoutIdentifier,
                            exception
                        );

                        // Increment failed task counter
                        incrementFailedTaskCounter();
                    } else {
                        // Log that we completed successfully.
                        logger.info("{} seems to have finished, cleaning up", virtualSpoutIdentifier);
                    }

                    // And cleanup, Remove from spoutInstances
                    spoutRunners.remove(virtualSpoutIdentifier);

                    // We have no value to return
                    return null;
                });
            }
        }
    }

    /**
     * This method will periodically show a status report to our logger interface.
     */
    private void reportStatus() {
        // Get current time
        final long now = getClock().millis();

        // Set initial value if none set.
        if (lastStatusReport == 0) {
            lastStatusReport = now;
            return;
        }

        // Only report once every 60 seconds.
        if ((now - lastStatusReport) <= REPORT_STATUS_INTERVAL_MS) {
            // Do nothing.
            return;
        }
        // Update timestamp
        lastStatusReport = now;

        // Show a status report
        logger.info(
            "Active Tasks: {}, Queued Tasks: {}, ThreadPool Size: {}/{}, Completed Tasks: {}, Errored Tasks: {}, Total Tasks Submitted: {}",
            executor.getActiveCount(),
            executor.getQueue().size(),
            executor.getPoolSize(),
            executor.getMaximumPoolSize(),
            executor.getCompletedTaskCount(),
            getNumberOfFailedTasks(),
            executor.getTaskCount()
        );
        logger.info("MessageBuffer size: {}, Running VirtualSpoutIds: {}",
            getVirtualSpoutMessageBus().messageSize(), spoutRunners.keySet());

        // Report to metrics record
        getMetricsRecorder().assignValue(getClass(), "bufferSize", getVirtualSpoutMessageBus().messageSize());
        getMetricsRecorder().assignValue(getClass(), "running", executor.getActiveCount());
        getMetricsRecorder().assignValue(getClass(), "queued", executor.getQueue().size());
        getMetricsRecorder().assignValue(getClass(), "errored", getNumberOfFailedTasks());
        getMetricsRecorder().assignValue(getClass(), "completed", executor.getCompletedTaskCount());
        getMetricsRecorder().assignValue(getClass(), "poolSize", executor.getPoolSize());

        // Loop through spouts instances
        try {
            // Loop thru all of them to get virtualSpout Ids.
            for (final SpoutRunner spoutRunner : spoutRunners.values()) {
                final DelegateSpout spout = spoutRunner.getSpout();

                // This shouldn't be possible, but better safe then sorry!
                if (spout == null) {
                    logger.error("We have a null spout in the runner, panic!");
                    continue;
                }

                // Report the spout's consumer's progress on it's partitions, note that it's possible to attempt reporting
                // the status before the spout is fully opened.
                getSpoutPartitionProgressMonitor().reportStatus(
                    spout
                );

                // Report how many filters are applied on this virtual spout.
                getMetricsRecorder().assignValue(
                    VirtualSpout.class,
                    spout.getVirtualSpoutId() + ".number_filters_applied", spout.getNumberOfFiltersApplied()
                );
            }
        } catch (final Throwable throwable) {
            // report the error up.
            reportError(throwable);

            // Log the exception
            logger.error("Caught exception during status checks {}", throwable.getMessage(), throwable);
            spoutPartitionProgressMonitor = null;
        }
    }

    /**
     * Call this method to indicate that we want to stop all running {@link VirtualSpout} instances as well
     * as finish running our monitor thread.
     */
    public void close() {
        keepRunning = false;

        // Ask the executor to shut down, this will prevent it from
        // accepting/starting new tasks.
        executor.shutdown();

        // Empty our run queue, this will prevent queued virtual spouts from getting started
        // during the shutdown process.
        executor.getQueue().clear();

        // Stop spoutPartitionProgressMonitor
        if (spoutPartitionProgressMonitor != null) {
            spoutPartitionProgressMonitor = null;
        }

        // Loop through our runners and request stop on each
        for (final SpoutRunner spoutRunner : spoutRunners.values()) {
            spoutRunner.requestStop();
        }

        // Wait for the executor to cleanly shut down
        try {
            logger.info("Waiting a maximum of {} ms for thread pool to shutdown", getMaxTerminationWaitTimeMs());
            executor.awaitTermination(getMaxTerminationWaitTimeMs(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException ex) {
            logger.error("Interrupted while stopping: {}", ex.getMessage(), ex);
        }

        // If we didn't shut down cleanly
        if (!executor.isTerminated()) {
            // Force a shut down
            logger.warn("Forcing unclean shutdown of thread pool");
            executor.shutdownNow();
        }

        // Clear our our internal state.
        spoutRunners.clear();
    }

    /**
     * @return - return the number of spout runner instances.
     *           *note* it doesn't mean all of these are actually running, some may be queued.
     */
    public int getTotalSpouts() {
        return spoutRunners.size();
    }

    /**
     * @return - Our system clock instance.
     */
    Clock getClock() {
        return clock;
    }

    /**
     * @return - Storm topology configuration.
     */
    Map<String, Object> getTopologyConfig() {
        return topologyConfig;
    }

    /**
     * @return - how often our monitor thread should run through its maintenance loop, in milliseconds.
     */
    long getMonitorThreadIntervalMs() {
        return ((Number) getTopologyConfig().get(SpoutConfig.MONITOR_THREAD_INTERVAL_MS)).longValue();
    }

    /**
     * @return - the maximum amount of time we'll wait for spouts to terminate before forcing them to stop, in milliseconds.
     */
    long getMaxTerminationWaitTimeMs() {
        return ((Number) getTopologyConfig().get(SpoutConfig.MAX_SPOUT_SHUTDOWN_TIME_MS)).longValue();
    }

    /**
     * @return - the maximum amount of concurrently running VirtualSpouts we'll start.
     */
    int getMaxConcurrentVirtualSpouts() {
        return ((Number) getTopologyConfig().get(SpoutConfig.MAX_CONCURRENT_VIRTUAL_SPOUTS)).intValue();
    }

    /**
     * Get the spout partition progress monitor so we can track and report spout progress.
     * @return the spout partition progress monitor.
     */
    private SpoutPartitionProgressMonitor getSpoutPartitionProgressMonitor() {
        if (spoutPartitionProgressMonitor == null) {
            // Create consumer monitor instance
            spoutPartitionProgressMonitor = new SpoutPartitionProgressMonitor(getMetricsRecorder());
        }
        return spoutPartitionProgressMonitor;
    }

    /**
     * @return ThreadSafe message bus for passing messages between DynamicSpout and VirtualSpouts.
     */
    private VirtualSpoutMessageBus getVirtualSpoutMessageBus() {
        return virtualSpoutMessageBus;
    }

    /**
     * Adds an error to the reported errors queue.  These will get pushed up and reported
     * to the topology.
     * @param throwable The error to be reported.
     */
    private void reportError(final Throwable throwable) {
        getVirtualSpoutMessageBus().publishError(throwable);
    }

    /**
     * @return Spouts metrics recorder.
     */
    MetricsRecorder getMetricsRecorder() {
        return metricsRecorder;
    }

    /**
     * @return ThreadPoolExecutor Service that runs VirtualSpout instances.
     */
    ThreadPoolExecutor getExecutor() {
        return executor;
    }

    /**
     * @return The new spout queue.
     */
    Queue<DelegateSpout> getNewSpoutQueue() {
        return newSpoutQueue;
    }

    /**
     * @return How many VirtualSpout tasks have terminated abnormally.
     */
    int getNumberOfFailedTasks() {
        return failedTaskCounter.get();
    }

    /**
     * Increments the failed task counter metric.
     * @return The new number of failed tasks.
     */
    private int incrementFailedTaskCounter() {
        return failedTaskCounter.incrementAndGet();
    }

    /**
     * Flag to know if we should keep running, or shut down.
     * @return - true if we should keep running, false if we should be stopping/stopped.
     */
    boolean keepRunning() {
        return keepRunning && !Thread.interrupted();
    }
}
