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

package com.salesforce.storm.spout.dynamic.utils;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates and stands up an internal test kafka server to be shared across test cases within the same test class.
 *
 * Example within your Test class.
 *
 *   &#064;ClassRule
 *   public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();
 *
 * Within your test case method:
 *   sharedKafkaTestResource.getKafkaTestServer()...
 */
public class SharedKafkaTestResource extends ExternalResource {
    private static final Logger logger = LoggerFactory.getLogger(SharedKafkaTestResource.class);

    /**
     * Our internal Kafka Test Server instance.
     */
    private KafkaTestServer kafkaTestServer = null;

    /**
     * Here we stand up an internal test kafka and zookeeper service.
     * Once for all tests that use this shared resource.
     */
    protected void before() throws Exception {
        logger.info("Starting kafka test server");
        if (kafkaTestServer != null) {
            throw new IllegalStateException("Unknown State!  Kafka Test Server already exists!");
        }
        // Setup kafka test server
        kafkaTestServer = new KafkaTestServer();
        kafkaTestServer.start();
    }

    /**
     * Here we shut down the internal test kafka and zookeeper services.
     */
    protected void after() {
        logger.info("Shutting down kafka test server");

        // Close out kafka test server if needed
        if (kafkaTestServer == null) {
            return;
        }
        try {
            kafkaTestServer.shutdown();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        kafkaTestServer = null;
    }

    /**
     * @return Shared Kafka Test server instance.
     */
    public KafkaTestServer getKafkaTestServer() {
        return kafkaTestServer;
    }
}
