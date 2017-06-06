package com.salesforce.storm.spout.sideline;

import com.google.common.base.Strings;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.consumer.Consumer;
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
     * @return returns a new instance of the configured MessageBuffer interface.
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

}
