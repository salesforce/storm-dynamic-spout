package com.salesforce.storm.spout.sideline.kafka.failedMsgRetryManagers;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Aims to test the performance of various Retry Managers.
 */
public class FailedMsgRetryManagerPerformanceTest {
    private static final Logger logger = LoggerFactory.getLogger(FailedMsgRetryManagerPerformanceTest.class);

    /**
     * Disabled for now.
     */
    public void runTest() throws InterruptedException {
        // Create instance with default settings
        FailedMsgRetryManager retryManager = new DefaultFailedMsgRetryManager();
        retryManager.open(Maps.newHashMap());

        // Do warm up
        logger.info("WARMING UP");
        doTest2(retryManager);

        // Now start test
        logger.info("STARTING TEST");
        retryManager = new DefaultFailedMsgRetryManager();
        retryManager.open(Maps.newHashMap());
        doTest2(retryManager);
    }

    public void doTest2(FailedMsgRetryManager retryManager) throws InterruptedException {
        logger.info("Starting to test {}", retryManager.getClass().getSimpleName());

        // Define test parameters
        final long numberOfTuples = 30000;
        final String topicName = "MyTopic";
        final int partition = 0;
        final String consumerId = "MyConsumer";

        // Add msgs
        logger.info("Starting to add {} failed msgs", numberOfTuples);
        final long startTupleAddTime = System.currentTimeMillis();
        for (long x=0; x<numberOfTuples; x++) {
            // Create TupleMessageId
            final TupleMessageId tupleMessageId = new TupleMessageId(topicName, partition, x, consumerId);
            retryManager.failed(tupleMessageId);
        }
        for (long x=0; x<numberOfTuples; x++) {
            // Create TupleMessageId
            final TupleMessageId tupleMessageId = new TupleMessageId(topicName, partition, x, consumerId);
            retryManager.failed(tupleMessageId);
        }
        logger.info("Finished in {} ms", (System.currentTimeMillis() - startTupleAddTime));

        // Sleep for 1 sec
        Thread.sleep(1000);

        // Now ask for next failed
        logger.info("Trying to get tuples back");
        final long startNextFailedTime = System.currentTimeMillis();
        final List<TupleMessageId> returnedTuples = Lists.newArrayList();
        do {
            final TupleMessageId tupleMessageId = retryManager.nextFailedMessageToRetry();
            if (tupleMessageId == null) {
                continue;
            }
            returnedTuples.add(tupleMessageId);
        } while (returnedTuples.size() < numberOfTuples);
        logger.info("Finished in {} ms", (System.currentTimeMillis() - startNextFailedTime));
    }
}
