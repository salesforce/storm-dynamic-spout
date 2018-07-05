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

package com.salesforce.storm.spout.dynamic.exception;

import com.salesforce.storm.spout.dynamic.DelegateSpout;

/**
 * Thrown when attempting to add a spout to the coordinator and that spout already exists. This doesn't necessarily
 * mean that the instance is the same, but that the identifier on the instance matches one that the coordinator was
 * already running.
 */
public class SpoutAlreadyExistsException extends RuntimeException {

    /**
     * The spout with an identifier that already exists in the coordinator.
     */
    private final DelegateSpout spout;

    /**
     * Thrown when attempting to add a spout to the coordinator and that spout already exists.
     * @param message specific message about the already existing spout.
     * @param spout specific spout that appears to already exist in the coordinator.
     */
    public SpoutAlreadyExistsException(String message, DelegateSpout spout) {
        super(message);
        this.spout = spout;
    }

    /**
     * Spout that caused this exception to be thrown.
     * @return spout that caused this exception to be thrown.
     */
    public DelegateSpout getSpout() {
        return spout;
    }
}
