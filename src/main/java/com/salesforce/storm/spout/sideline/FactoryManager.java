package com.salesforce.storm.spout.sideline;

import com.google.common.base.Strings;
import com.salesforce.storm.spout.sideline.config.SidelineSpoutConfig;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Deserializer;
import com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers.DefaultFailedMsgRetryManager;
import com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers.FailedMsgRetryManager;
import com.salesforce.storm.spout.sideline.metrics.MetricsRecorder;
import com.salesforce.storm.spout.sideline.persistence.PersistenceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * Handles creating instances of specific interface implementations, based off of
 * our configuration.
 */
public class FactoryManager implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(FactoryManager.class);

    /**
     * Holds our configuration so we know what classes to create instances of.
     */
    private final Map topologyConfig;

    /**
     * Class instance of our Deserializer.
     */
    private transient Class<? extends Deserializer> deserializerClass;

    /**
     * Class instance of our FailedMsgRetryManager.
     */
    private transient Class<? extends FailedMsgRetryManager> failedMsgRetryManagerClass;

    /**
     * Class instance of our PersistenceManager.
     */
    private transient Class<? extends PersistenceManager> persistenceManagerClass;

    /**
     * Class instance of our Metrics Recorder.
     */
    private transient Class<? extends MetricsRecorder> metricsRecorderClass;

    public FactoryManager(Map topologyConfig) {
        this.topologyConfig = topologyConfig;
    }

    /**
     * @return returns a new instance of the configured deserializer.
     */
    public Deserializer createNewDeserializerInstance() {
        if (deserializerClass == null) {
            final String classStr = (String) topologyConfig.get(SidelineSpoutConfig.DESERIALIZER_CLASS);
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
     * @return returns a new instance of the configured FailedMsgRetryManager.
     */
    public FailedMsgRetryManager createNewFailedMsgRetryManagerInstance() {
        if (failedMsgRetryManagerClass == null) {
            String classStr = (String) topologyConfig.get(SidelineSpoutConfig.FAILED_MSG_RETRY_MANAGER_CLASS);
            if (Strings.isNullOrEmpty(classStr)) {
                logger.warn("Missing required configuration {} defaulting to using {}", SidelineSpoutConfig.FAILED_MSG_RETRY_MANAGER_CLASS, DefaultFailedMsgRetryManager.class.getSimpleName());
                classStr = DefaultFailedMsgRetryManager.class.getName();
            }

            try {
                failedMsgRetryManagerClass = (Class<? extends FailedMsgRetryManager>) Class.forName(classStr);
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
    public PersistenceManager createNewPersistenceManagerInstance() {
        if (persistenceManagerClass == null) {
            final String classStr = (String) topologyConfig.get(SidelineSpoutConfig.PERSISTENCE_MANAGER_CLASS);
            if (Strings.isNullOrEmpty(classStr)) {
                throw new IllegalStateException("Missing required configuration: " + SidelineSpoutConfig.PERSISTENCE_MANAGER_CLASS);
            }

            try {
                persistenceManagerClass = (Class<? extends PersistenceManager>) Class.forName(classStr);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            return persistenceManagerClass.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return returns a new instance of the configured Metrics Recorder manager.
     */
    public MetricsRecorder createNewMetricsRecorder() {
        if (metricsRecorderClass == null) {
            String classStr = (String) topologyConfig.get(SidelineSpoutConfig.METRICS_RECORDER_CLASS);
            if (Strings.isNullOrEmpty(classStr)) {
                // Use default value
                classStr = "com.salesforce.storm.spout.sideline.metrics.StormRecorder";
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
}
