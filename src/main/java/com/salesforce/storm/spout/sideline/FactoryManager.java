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
package com.salesforce.storm.spout.sideline;

import com.google.common.base.Strings;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.consumer.Consumer;
import com.salesforce.storm.spout.sideline.handler.SpoutHandler;
import com.salesforce.storm.spout.sideline.handler.VirtualSpoutHandler;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Deserializer;
import com.salesforce.storm.spout.sideline.retry.RetryManager;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.persistence.PersistenceAdapter;
import com.salesforce.storm.spout.sideline.buffer.MessageBuffer;

import java.io.Serializable;
import java.util.Map;

/**
 * Handles creating instances of specific interface implementations, based off of
 * our configuration.
 * Methods are marked Synchronized because the FactoryManager instance is shared between threads, but
 * its methods are rarely invoked after the spout is initial started up, so it shouldn't present much of
 * a problem w/ contention.
 */
public class FactoryManager implements Serializable {

    /**
     * Holds our configuration so we know what classes to create instances of.
     */
    private final Map spoutConfig;

    /**
     * Class instance of our Deserializer.
     */
    private transient Class<? extends Deserializer> deserializerClass;

    /**
     * Class instance of our RetryManager.
     */
    private transient Class<? extends RetryManager> failedMsgRetryManagerClass;

    /**
     * Class instance of our PersistenceAdapter.
     */
    private transient Class<? extends PersistenceAdapter> persistenceAdapterClass;

    /**
     * Class instance of our Metrics Recorder.
     */
    private transient Class<? extends MetricsRecorder> metricsRecorderClass;

    /**
     * Class instance of our Tuple Buffer.
     */
    private transient Class<? extends MessageBuffer> messageBufferClass;

    /**
     * Class instance of our Consumer.
     */
    private transient Class<? extends Consumer> consumerClass;

    /**
     * Class instance of our SpoutHandler.
     */
    private transient Class<? extends SpoutHandler> spoutHandlerClass;

    /**
     * Class instance of our VirtualSpoutHandler.
     */
    private transient Class<? extends VirtualSpoutHandler> virtualSpoutHandlerClass;


    /**
     * Constructor.
     * @param spoutConfig Spout config.
     */
    public FactoryManager(Map<String, Object> spoutConfig) {
        // Create immutable copy of configuration.
        this.spoutConfig = Tools.immutableCopy(spoutConfig);
    }

    /**
     * @return returns a new instance of the configured deserializer.
     */
    public synchronized Deserializer createNewDeserializerInstance() {
        if (deserializerClass == null) {
            final String classStr = (String) spoutConfig.get(SidelineSpoutConfig.DESERIALIZER_CLASS);
            if (Strings.isNullOrEmpty(classStr)) {
                throw new IllegalStateException("Missing required configuration: " + SidelineSpoutConfig.DESERIALIZER_CLASS);
            }

            try {
                deserializerClass = (Class<? extends Deserializer>) Class.forName(classStr);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return deserializerClass.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return returns a new instance of the configured RetryManager.
     */
    public synchronized RetryManager createNewFailedMsgRetryManagerInstance() {
        if (failedMsgRetryManagerClass == null) {
            String classStr = (String) spoutConfig.get(SidelineSpoutConfig.RETRY_MANAGER_CLASS);
            if (Strings.isNullOrEmpty(classStr)) {
                throw new IllegalStateException("Missing required configuration: " + SidelineSpoutConfig.TUPLE_BUFFER_CLASS);
            }

            try {
                failedMsgRetryManagerClass = (Class<? extends RetryManager>) Class.forName(classStr);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return failedMsgRetryManagerClass.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return returns a new instance of the configured persistence manager.
     */
    public synchronized PersistenceAdapter createNewPersistenceAdapterInstance() {
        if (persistenceAdapterClass == null) {
            final String classStr = (String) spoutConfig.get(SidelineSpoutConfig.PERSISTENCE_ADAPTER_CLASS);
            if (Strings.isNullOrEmpty(classStr)) {
                throw new IllegalStateException("Missing required configuration: " + SidelineSpoutConfig.PERSISTENCE_ADAPTER_CLASS);
            }

            try {
                persistenceAdapterClass = (Class<? extends PersistenceAdapter>) Class.forName(classStr);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return persistenceAdapterClass.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return returns a new instance of the configured Metrics Recorder manager.
     */
    public synchronized MetricsRecorder createNewMetricsRecorder() {
        if (metricsRecorderClass == null) {
            String classStr = (String) spoutConfig.get(SidelineSpoutConfig.METRICS_RECORDER_CLASS);
            if (Strings.isNullOrEmpty(classStr)) {
                throw new IllegalStateException("Missing required configuration: " + SidelineSpoutConfig.METRICS_RECORDER_CLASS);
            }

            try {
                metricsRecorderClass = (Class<? extends MetricsRecorder>) Class.forName(classStr);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return metricsRecorderClass.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return returns a new instance of the configured MessageBuffer interface.
     */
    public synchronized MessageBuffer createNewMessageBufferInstance() {
        if (messageBufferClass == null) {
            String classStr = (String) spoutConfig.get(SidelineSpoutConfig.TUPLE_BUFFER_CLASS);
            if (Strings.isNullOrEmpty(classStr)) {
                throw new IllegalStateException("Missing required configuration: " + SidelineSpoutConfig.TUPLE_BUFFER_CLASS);
            }

            try {
                messageBufferClass = (Class<? extends MessageBuffer>) Class.forName(classStr);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return messageBufferClass.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return returns a new instance of the configured Consumer interface.
     */
    public synchronized Consumer createNewConsumerInstance() {
        if (consumerClass == null) {
            String classStr = (String) spoutConfig.get(SidelineSpoutConfig.CONSUMER_CLASS);
            if (Strings.isNullOrEmpty(classStr)) {
                throw new IllegalStateException("Missing required configuration: " + SidelineSpoutConfig.CONSUMER_CLASS);
            }

            try {
                consumerClass = (Class<? extends Consumer>) Class.forName(classStr);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return consumerClass.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create an instance of the configured SpoutHandler.
     * @return Instance of a SpoutHandler
     */
    public synchronized SpoutHandler createSpoutHandler() {
        if (spoutHandlerClass == null) {
            String classStr = (String) spoutConfig.get(SidelineSpoutConfig.SPOUT_HANDLER_CLASS);
            if (Strings.isNullOrEmpty(classStr)) {
                throw new IllegalStateException("Missing required configuration: " + SidelineSpoutConfig.SPOUT_HANDLER_CLASS);
            }

            try {
                spoutHandlerClass = (Class<? extends SpoutHandler>) Class.forName(classStr);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return spoutHandlerClass.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create an instance of the configured VirtualSpoutHandler.
     * @return Instance of a VirtualSpoutHandler
     */
    public synchronized VirtualSpoutHandler createVirtualSpoutHandler() {
        if (virtualSpoutHandlerClass == null) {
            String classStr = (String) spoutConfig.get(SidelineSpoutConfig.VIRTUAL_SPOUT_HANDLER_CLASS);
            if (Strings.isNullOrEmpty(classStr)) {
                throw new IllegalStateException("Missing required configuration: " + SidelineSpoutConfig.VIRTUAL_SPOUT_HANDLER_CLASS);
            }

            try {
                virtualSpoutHandlerClass = (Class<? extends VirtualSpoutHandler>) Class.forName(classStr);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return virtualSpoutHandlerClass.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }
}
