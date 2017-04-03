package com.salesforce.storm.spout.sideline.persistence;

import com.salesforce.storm.spout.sideline.kafka.ConsumerState;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequestIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;

public class SidelinePayload {

    public final SidelineType type;
    public final SidelineRequestIdentifier id;
    public final SidelineRequest request;
    public final Long startingOffset;
    public final Long endingOffset;

    SidelinePayload(
        final SidelineType type,
        final SidelineRequestIdentifier id,
        final SidelineRequest request,
        final Long startingOffset,
        final Long endingOffset
    ) {
        this.type = type;
        this.id = id;
        this.request = request;
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
    }
}
