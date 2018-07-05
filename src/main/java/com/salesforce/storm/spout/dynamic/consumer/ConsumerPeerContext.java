/*
 * Copyright (c) 2018, Salesforce.com, Inc.
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

package com.salesforce.storm.spout.dynamic.consumer;

/**
 * This class presents information to a Consumer about how many total instances of it are running, along with
 * the unique instance number the consumer is.
 */
public class ConsumerPeerContext {
    /**
     * This represents how many consumer instances are running.
     * This should always be AT LEAST 1.
     */
    private final int totalInstances;

    /**
     * This defines what instance number the current instance is out of the total number of instances.
     * This number is 0 indexed.
     *
     * Example, if there are 10 instances total, this will have a value ranging from [0-9]
     */
    private final int instanceNumber;

    /**
     * Constructor.
     * @param totalInstances Total number of consumer instances running.
     * @param instanceNumber The instance number.
     */
    public ConsumerPeerContext(int totalInstances, int instanceNumber) {
        this.totalInstances = totalInstances;
        this.instanceNumber = instanceNumber;
    }

    /**
     * Get the total number of consumer instances.
     * @return total number of consumer instances.
     */
    public int getTotalInstances() {
        return totalInstances;
    }

     /**
      * Get the instance number for the current instance.
      *
      * This is take out of the total number of instances. Indexing begins at 0.
      *
     * @return instance number for the current instance.
     */
    public int getInstanceNumber() {
        return instanceNumber;
    }

    @Override
    public String toString() {
        return "ConsumerPeerContext{"
            + "totalInstances=" + totalInstances
            + ", instanceNumber=" + instanceNumber
            + '}';
    }
}
