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

package com.salesforce.storm.spout.sideline.trigger.example;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.salesforce.storm.spout.dynamic.FactoryManager;
import com.salesforce.storm.spout.dynamic.Tools;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.filter.FilterChainStep;
import com.salesforce.storm.spout.dynamic.persistence.zookeeper.CuratorFactory;
import com.salesforce.storm.spout.dynamic.persistence.zookeeper.CuratorHelper;
import com.salesforce.storm.spout.sideline.handler.SidelineController;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineTrigger;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Sideline trigger that uses Zookeeper watches for triggering start and stop requests.
 *
 * This class serves as a reference implementation of a {@link SidelineTrigger} that you can use out of the box.
 */
public class ZookeeperWatchTrigger implements SidelineTrigger {

    /**
     * Logger for logging logs.
     */
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperWatchTrigger.class);

    /**
     * Has the trigger been opened.
     */
    private boolean isOpen = false;

    /**
     * Sideline controller for performing sideline actions.
     */
    private SidelineController sidelineController;

    /**
     * Curator curator for Zookeeper.
     */
    private CuratorFramework curator;

    /**
     * Curator helper for Zookeeper.
     */
    private CuratorHelper curatorHelper;

    /**
     * Builder for taking data off of a {@link TriggerEvent} and turning it into a {@link FilterChainStep}.
     */
    private FilterChainStepBuilder filterChainStepBuilder;

    /**
     * All of the caches that we set up, so that we can stop them when we close the trigger.
     */
    private final List<PathChildrenCache> caches = new ArrayList<>();

    private final Set<SidelineRequest> sidelineRequests = new HashSet<>();

    /**
     * JSON parser.
     */
    private static final Gson gson = new GsonBuilder()
        .setDateFormat("yyyy-MM-dd HH:mm:ss")
        .create();

    /**
     * Set the {@link SidelineController} on this trigger instance.
     * @param sidelineController sideline controller instance.
     */
    @Override
    public void setSidelineController(final SidelineController sidelineController) {
        this.sidelineController = sidelineController;
    }

    /**
     * Open the trigger, connect to Zookeeper and setup our watches.
     * @param spoutConfig spout configuration
     */
    @Override
    public void open(final AbstractConfig spoutConfig) {
        if (isOpen) {
            // If this happens something is configured wrong, so we're going to kill the topology violently at this point
            logger.error("Trigger already opened!");
            throw new RuntimeException("Trigger is already opened, it should not be opened a second time - something is wrong!");
        }

        logger.info("Opening {}", this.getClass());

        Preconditions.checkArgument(
            spoutConfig.hasNonNullValue(Config.ZK_ROOTS),
            "One or more roots must be configured in Zookeeper to watch for events."
        );

        curator = CuratorFactory.createNewCuratorInstance(
            spoutConfig.stripKeyPrefix(Config.PREFIX),
            getClass().getSimpleName()
        );

        curatorHelper = new CuratorHelper(curator);

        filterChainStepBuilder = FactoryManager.createNewInstance(
            spoutConfig.getString(Config.FILTER_CHAIN_STEP_BUILDER_CLASS)
        );

        final List<String> roots = spoutConfig.getList(Config.ZK_ROOTS);

        // Starting and stopping triggers fire off at almost the exact same time so we need to do this here rather
        // than after all of our other setup occurs.
        isOpen = true;

        for (final String root : roots) {
            try {
                // Check if this path exists
                if (curator.checkExists().forPath(root) == null) {
                    logger.warn("Configured root {} does not exist", root);

                    // Attempt to create the root if it does not exist.
                    curator
                        .create()
                        .creatingParentsIfNeeded()
                        .forPath(root, "".getBytes());
                } else {
                    // Load the existing requests from it
                    final List<String> sidelineRequests = curator.getChildren().forPath(root);

                    for (final String sidelineRequest : sidelineRequests) {
                        final byte[] data = curator.getData().forPath(root + "/" + sidelineRequest);
                        final TriggerEvent triggerEvent = getTriggerEvent(data);

                        logger.info("Loading existing TriggerEvent {}", triggerEvent);

                        handleSidelining(root + "/" + sidelineRequest, triggerEvent);
                    }
                }

                logger.info("Creating cache for {}", root);

                final SidelineTriggerWatch watch = new SidelineTriggerWatch();

                // Now setup our watch so that we see future changes as they come through
                final PathChildrenCache cache = new PathChildrenCache(curator, root, true);
                cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
                cache.getListenable().addListener(watch);

                // Block the process until we have received our initialization event.
                while (!watch.isInitialized()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {
                        logger.error("Interrupted while waiting for the listener to initialize.");
                    }
                }

                caches.add(cache);
            } catch (Exception ex) {
                logger.error("Error creating PathChildrenCache for {} {}", root, ex);
            }
        }
    }

    /**
     * Close the trigger.
     */
    @Override
    public void close() {
        // Close each of the caches that we originally opened
        for (PathChildrenCache cache : caches) {
            try {
                cache.close();
            } catch (IOException ex) {
                logger.error("Unable to close cache {}", ex);
            }
        }

        if (curator != null) {
            curator.close();
            curator = null;
            curatorHelper = null;
        }

        isOpen = false;
    }

    /**
     * Using a trigger event process a START/STOP sideline request.
     * @param triggerEvent trigger event.
     */
    private void handleSidelining(final String path, final TriggerEvent triggerEvent) {
        if (triggerEvent == null) {
            logger.warn("Received a null TriggerEvent");
            return;
        }

        if (triggerEvent.isProcessed()) {
            logger.info("TriggerEvent has already been processed, skipping. {}", triggerEvent);
            return;
        }

        final SidelineRequest sidelineRequest = buildSidelineRequest(triggerEvent);

        if (sidelineRequest == null) {
            logger.error("Unable to build SidelineRequest from TriggerEvent {}", triggerEvent);
            return;
        }

        // Track all of the sideline requests we've built from TriggerEvent's
        if (!sidelineRequests.contains(sidelineRequest)) {
            sidelineRequests.add(sidelineRequest);
        }

        if (triggerEvent.getType().equals(SidelineType.START)) {
            logger.info("Starting sideline request {} from event {}", sidelineRequest, triggerEvent);
            sidelineController.startSidelining(sidelineRequest);
        }

        if (triggerEvent.getType().equals(SidelineType.STOP)) {
            logger.info("Stopping sideline request {} from event {}", sidelineRequest, triggerEvent);
            sidelineController.stopSidelining(sidelineRequest);
        }

        // Write the trigger event back to its path and flip the processed bit to true
        curatorHelper.writeJson(path, new TriggerEvent(
            triggerEvent.getType(),
            triggerEvent.getData(),
            triggerEvent.getCreatedAt(),
            triggerEvent.getCreatedBy(),
            triggerEvent.getDescription(),
            // Explicit set this as processed
            true,
            // Update the updated at date to right now
            LocalDateTime.now()
        ));
    }

    /**
     * Build a sideline request from a trigger event (what came from Zookeeper).
     * @param triggerEvent The trigger event
     * @return A sideline request that can be used to start of stop sidelining
     */
    private SidelineRequest buildSidelineRequest(final TriggerEvent triggerEvent) {
        try {
            final FilterChainStep step = filterChainStepBuilder.build(triggerEvent.getData());

            final SidelineRequest sidelineRequest = new SidelineRequest(
                generateSidelineRequestIdentifier(triggerEvent, step),
                step
            );

            logger.info("Creating a sideline request with id {} and step {}", sidelineRequest.id, sidelineRequest.step);

            return sidelineRequest;
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException ex) {
            logger.error("Unable to generate an identifier for this request, cowardly refusing to proceed! {}", ex);
            return null;
        }
    }

    /**
     * Using a filter chain step, make some JSON out of it and then hash it to create an idempotent identifier.
     * @param triggerEvent Trigger event that contains metadata about the request
     * @param step The FilterChainStep that we are going to generate the request from
     * @return A sideline request identifier for the filter chain step
     * @throws NoSuchAlgorithmException Your java install is whack yo, it's missing MD5, for realz???
     */
    private SidelineRequestIdentifier generateSidelineRequestIdentifier(
        final TriggerEvent triggerEvent,
        final FilterChainStep step
    ) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        final String json = gson.toJson(step);

        final StringBuilder identifier = new StringBuilder(
            DatatypeConverter.printHexBinary(MessageDigest.getInstance("MD5").digest(json.getBytes("UTF-8")))
        );

        // If we were provided a date time in the event, append the time stamp of that event to the identifier
        if (triggerEvent.getCreatedAt() != null) {
            identifier.append("-");
            identifier.append(
                triggerEvent.getCreatedAt().atZone(ZoneOffset.UTC).toInstant().toEpochMilli()
            );
        }

        return new SidelineRequestIdentifier(identifier.toString());
    }

    /**
     * Create a trigger event from the provided data.
     * @return Trigger event
     */
    private TriggerEvent getTriggerEvent(final byte[] data) {
        final String json = new String(data, Charset.forName("UTF-8"));
        return getTriggerEventFromJson(json);
    }

    /**
     * Parse a trigger event from some JSON.
     * @param json JSON to parse
     * @return Trigger event
     */
    private TriggerEvent getTriggerEventFromJson(final String json) {
        try {
            return gson.fromJson(json, TriggerEvent.class);
        } catch (Exception e) {
            logger.error("Unable to parse trigger event {} {}", json, e);
            return null;
        }
    }

    Set<SidelineRequest> getSidelineRequests() {
        return this.sidelineRequests;
    }

    /**
     * Watch implementation for the sideline trigger node in Zookeeper.
     */
    private class SidelineTriggerWatch implements PathChildrenCacheListener {

        /**
         * Whether or not the initialization event has been received for this listener.
         */
        private boolean isInitialized = false;

        /**
         * Receives events for this node cache and handles them.
         * @param client curator for interacting with zookeeper.
         * @param event specific event from the node path being watched.
         * @throws Exception most likely something is wrong with the zookeeper connection.
         */
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            logger.info("Received cache event {}", event);

            if (event == null) {
                logger.warn("Received a null event, this shouldn't happen!");
                return;
            }

            if (event.getType() == null) {
                logger.warn("Received an event, but there was no type, this shouldn't happen!");
                return;
            }

            switch (event.getType()) {
                case INITIALIZED:
                    isInitialized = true;
                    break;
                case CHILD_ADDED:
                case CHILD_UPDATED:
                    if (isInitialized) {
                        // Refresh the event from zookeeper, so we have the most current copy
                        final TriggerEvent triggerEvent = getTriggerEvent(event.getData().getData());

                        handleSidelining(event.getData().getPath(), triggerEvent);
                    }
                    break;
                case CHILD_REMOVED:
                case CONNECTION_SUSPENDED:
                case CONNECTION_RECONNECTED:
                case CONNECTION_LOST:
                    break;
                default:
                    logger.info("Unidentified event {}", event);
                    break;
            }
        }

        public boolean isInitialized() {
            return isInitialized;
        }
    }
}
