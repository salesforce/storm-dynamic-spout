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

package com.salesforce.storm.spout.sideline.handler;

import com.google.common.base.Preconditions;
import com.salesforce.storm.spout.dynamic.ConsumerPartition;
import com.salesforce.storm.spout.dynamic.DelegateSpout;
import com.salesforce.storm.spout.dynamic.FactoryManager;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.handler.VirtualSpoutHandler;
import com.salesforce.storm.spout.dynamic.consumer.Consumer;
import com.salesforce.storm.spout.sideline.SidelineVirtualSpoutIdentifier;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.config.SidelineConfig;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Handler for sideline virtual spouts.
 */
public class SidelineVirtualSpoutHandler implements VirtualSpoutHandler {

    // Logger for logging
    private static final Logger logger = LoggerFactory.getLogger(SidelineVirtualSpoutHandler.class);

    private PersistenceAdapter persistenceAdapter;

    @Override
    public void open(final AbstractConfig spoutConfig) {
        final String persistenceAdapterClass = spoutConfig.getString(SidelineConfig.PERSISTENCE_ADAPTER_CLASS);

        Preconditions.checkArgument(
            persistenceAdapterClass != null && !persistenceAdapterClass.isEmpty(),
            "Sideline persistence adapter class is required"
        );

        persistenceAdapter = FactoryManager.createNewInstance(
            persistenceAdapterClass
        );
        persistenceAdapter.open(spoutConfig);
    }

    /**
     * Handler for when a sideline spout is completed.  When a sideline spout completes clean up the metadata for the
     * given sideline request.
     * @param virtualSpout Virtual Spout instance that is completing..
     */
    @Override
    public void onVirtualSpoutCompletion(DelegateSpout virtualSpout) {
        try {
            // Get the specific sideline request from the virtual spout's id
            final SidelineRequestIdentifier sidelineRequestIdentifier =
                ((SidelineVirtualSpoutIdentifier) virtualSpout.getVirtualSpoutId()).getSidelineRequestIdentifier();

            // We can only do this if we have a starting state and a sideline request identifier
            if (sidelineRequestIdentifier != null && virtualSpout.getStartingState() != null) {
                // Clean up sideline request
                for (final ConsumerPartition consumerPartition : virtualSpout.getStartingState().getConsumerPartitions()) {
                    persistenceAdapter.clearSidelineRequest(
                        sidelineRequestIdentifier,
                        consumerPartition
                    );
                }
            }
        } catch (Exception ex) {
            logger.error("I was unable to completion the virtual spout for {} {}", virtualSpout.getVirtualSpoutId(), ex);
        }
    }

    /**
     * Close the handler.
     */
    @Override
    public void close() {
        if (getPersistenceAdapter() != null) {
            getPersistenceAdapter().close();
        }
        persistenceAdapter = null;
    }

    /**
     * Get the persistence adapter, only use this for tests!
     * @return persistence adapter.
     */
    PersistenceAdapter getPersistenceAdapter() {
        return persistenceAdapter;
    }
}
