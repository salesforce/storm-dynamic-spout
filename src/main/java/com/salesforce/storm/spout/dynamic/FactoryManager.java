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

package com.salesforce.storm.spout.dynamic;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.SpoutConfig;
import com.salesforce.storm.spout.dynamic.consumer.Consumer;
import com.salesforce.storm.spout.dynamic.handler.SpoutHandler;
import com.salesforce.storm.spout.dynamic.handler.VirtualSpoutHandler;
import com.salesforce.storm.spout.dynamic.retry.RetryManager;
import com.salesforce.storm.spout.dynamic.metrics.MetricsRecorder;
import com.salesforce.storm.spout.dynamic.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.dynamic.buffer.MessageBuffer;

import java.io.Serializable;
import java.util.Map;

/**
 * Handles creating instances of specific interface implementations, based off of
 * our configuration.
 *
 * Methods are marked Synchronized because the FactoryManager instance is shared between threads, but
 * its methods are rarely invoked after the spout is initial started up, so it shouldn't present much of
 * a problem w/ contention.
 */
public class FactoryManager implements Serializable {

    /**
     * Holds our configuration so we know what classes to create instances of.
     */
    private final AbstractConfig spoutConfig;

    /**
     * Constructor.
     * @param spoutConfig Spout config.
     */
    public FactoryManager(final AbstractConfig spoutConfig) {
        // Keep reference to config.
        this.spoutConfig = spoutConfig;
    }

    /**
     * @return returns a new instance of the configured RetryManager.
     */
    public RetryManager createNewFailedMsgRetryManagerInstance() {
        return createNewInstanceFromConfig(
            SpoutConfig.RETRY_MANAGER_CLASS
        );
    }

    /**
     * @return returns a new instance of the configured persistence manager.
     */
    public PersistenceAdapter createNewPersistenceAdapterInstance() {
        return createNewInstanceFromConfig(
            SpoutConfig.PERSISTENCE_ADAPTER_CLASS
        );
    }

    /**
     * @return returns a new instance of the configured Metrics Recorder manager.
     */
    public MetricsRecorder createNewMetricsRecorder() {
        return createNewInstanceFromConfig(
            SpoutConfig.METRICS_RECORDER_CLASS
        );
    }

    /**
     * @return returns a new instance of the configured MessageBuffer interface.
     */
    public MessageBuffer createNewMessageBufferInstance() {
        return createNewInstanceFromConfig(
            SpoutConfig.TUPLE_BUFFER_CLASS
        );
    }

    /**
     * @return returns a new instance of the configured Consumer interface.
     */
    public Consumer createNewConsumerInstance() {
        return createNewInstanceFromConfig(
            SpoutConfig.CONSUMER_CLASS
        );
    }

    /**
     * Create an instance of the configured SpoutHandler.
     * @return Instance of a SpoutHandler
     */
    public SpoutHandler createSpoutHandler() {
        return createNewInstanceFromConfig(
            SpoutConfig.SPOUT_HANDLER_CLASS
        );
    }

    /**
     * Create an instance of the configured VirtualSpoutHandler.
     * @return Instance of a VirtualSpoutHandler
     */
    public synchronized VirtualSpoutHandler createVirtualSpoutHandler() {
        return createNewInstanceFromConfig(
            SpoutConfig.VIRTUAL_SPOUT_HANDLER_CLASS
        );
    }

    private synchronized <T> T createNewInstanceFromConfig(final String configKey) {
        Preconditions.checkState(
            spoutConfig.hasNonNullValue(configKey),
            "Class has not been specified for configuration key " + configKey
        );

        return createNewInstance(
            (String) spoutConfig.get(configKey)
        );
    }

    /**
     * Utility method for instantiating new instance from a package/class name.
     * @param classStr Fully qualified classname.
     * @param <T> Instance you are creating.
     * @return Newly created instance.
     */
    public static synchronized <T> T createNewInstance(String classStr) {
        if (Strings.isNullOrEmpty(classStr)) {
            throw new IllegalStateException("Missing class name!");
        }

        try {
            Class<? extends T> clazz = (Class<? extends T>) Class.forName(classStr);
            return clazz.newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }
}
