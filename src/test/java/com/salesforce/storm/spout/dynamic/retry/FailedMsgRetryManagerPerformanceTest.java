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

package com.salesforce.storm.spout.dynamic.retry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.storm.spout.dynamic.MessageId;
import com.salesforce.storm.spout.dynamic.DefaultVirtualSpoutIdentifier;
import com.salesforce.storm.spout.dynamic.config.AbstractConfig;
import com.salesforce.storm.spout.dynamic.config.ConfigDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

/**
 * Aims to test the performance of various Retry Managers.
 */
public class FailedMsgRetryManagerPerformanceTest {
    private static final Logger logger = LoggerFactory.getLogger(FailedMsgRetryManagerPerformanceTest.class);

    /**
     * Run a bunch of retries.
     *
     * Disabled for now.
     */
    public void runTest() throws InterruptedException {
        // Create instance with default settings
        RetryManager retryManager = new DefaultRetryManager();
        retryManager.open(new AbstractConfig(new ConfigDefinition(), new HashMap<>()));

        // Do warm up
        logger.info("WARMING UP");
        doTest2(retryManager);

        // Now start test
        logger.info("STARTING TEST");
        retryManager = new DefaultRetryManager();
        retryManager.open(new AbstractConfig(new ConfigDefinition(), new HashMap<>()));
        doTest2(retryManager);
    }

    private void doTest2(RetryManager retryManager) throws InterruptedException {
        logger.info("Starting to test {}", retryManager.getClass().getSimpleName());

        // Define test parameters
        final long numberOfTuples = 30000;
        final String topicName = "MyTopic";
        final int partition = 0;
        final DefaultVirtualSpoutIdentifier consumerId = new DefaultVirtualSpoutIdentifier("MyConsumer");

        // Add msgs
        logger.info("Starting to add {} failed msgs", numberOfTuples);
        final long startTupleAddTime = System.currentTimeMillis();
        for (long x = 0; x < numberOfTuples; x++) {
            // Create MessageId
            final MessageId messageId = new MessageId(topicName, partition, x, consumerId);
            retryManager.failed(messageId);
        }
        for (long x = 0; x < numberOfTuples; x++) {
            // Create MessageId
            final MessageId messageId = new MessageId(topicName, partition, x, consumerId);
            retryManager.failed(messageId);
        }
        logger.info("Finished in {} ms", (System.currentTimeMillis() - startTupleAddTime));

        // Sleep for 1 sec
        Thread.sleep(1000);

        // Now ask for next failed
        logger.info("Trying to get tuples back");
        final long startNextFailedTime = System.currentTimeMillis();
        final List<MessageId> returnedTuples = Lists.newArrayList();
        do {
            final MessageId messageId = retryManager.nextFailedMessageToRetry();
            if (messageId == null) {
                continue;
            }
            returnedTuples.add(messageId);
        }
        while (returnedTuples.size() < numberOfTuples);
        logger.info("Finished in {} ms", (System.currentTimeMillis() - startNextFailedTime));
    }
}
