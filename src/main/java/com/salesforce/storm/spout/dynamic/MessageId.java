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

package com.salesforce.storm.spout.dynamic;

import java.time.Clock;

/**
 * This object is used as the MessageId for Tuples emitted to the Storm topology.
 */
public final class MessageId {
    /**
     * Class Properties.
     */
    private final String namespace;
    private final int partition;
    private final long offset;
    private final VirtualSpoutIdentifier srcVirtualSpoutId;
    private final long timestamp;

    /**
     * Cached HashCode.
     */
    private int hash = 0;

    /**
     * Constructor.
     * @param namespace - the namespace this tuple came from.
     * @param partition - the partition this tuple came from.
     * @param offset - the offset this tuple came from.
     * @param srcVirtualSpoutId - the VirtualSpout's identifier this tuple came from.
     */
    public MessageId(final String namespace, final int partition, final long offset, final VirtualSpoutIdentifier srcVirtualSpoutId) {
        this.namespace = namespace;
        this.partition = partition;
        this.offset = offset;
        this.srcVirtualSpoutId = srcVirtualSpoutId;
        this.timestamp = Clock.systemUTC().millis();
    }
    
    public String getNamespace() {
        return namespace;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public VirtualSpoutIdentifier getSrcVirtualSpoutId() {
        return srcVirtualSpoutId;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        MessageId that = (MessageId) other;

        if (getPartition() != that.getPartition()) {
            return false;
        }
        if (getOffset() != that.getOffset()) {
            return false;
        }
        if (!getNamespace().equals(that.getNamespace())) {
            return false;
        }
        return getSrcVirtualSpoutId().equals(that.getSrcVirtualSpoutId());
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            int result = getNamespace().hashCode();
            result = 31 * result + getPartition();
            result = 31 * result + (int) (getOffset() ^ (getOffset() >>> 32));
            result = 31 * result + getSrcVirtualSpoutId().hashCode();
            hash = result;
        }
        return hash;
    }

    @Override
    public String toString() {
        final long diff = Clock.systemUTC().millis() - timestamp;

        return "MessageId{"
                + "namespace='" + namespace + '\''
                + ", partition=" + partition
                + ", offset=" + offset
                + ", srcVirtualSpoutId='" + srcVirtualSpoutId + '\''
                + ", timestamp='" + timestamp + " (" + diff + ")" + '\''
                + '}';
    }
}
