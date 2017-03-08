package com.salesforce.storm.spout.sideline.persistence;

import com.salesforce.storm.spout.sideline.kafka.consumerState.ConsumerState;
import com.salesforce.storm.spout.sideline.trigger.SidelineIdentifier;
import com.salesforce.storm.spout.sideline.trigger.SidelineRequest;
import com.salesforce.storm.spout.sideline.trigger.SidelineType;

public class SidelinePayload {

    public final SidelineType type;
    public final SidelineIdentifier id;
    public final SidelineRequest request;
    public final ConsumerState startingState;
    public final ConsumerState endingState;

    SidelinePayload(
        final SidelineType type,
        final SidelineIdentifier id,
        final SidelineRequest request,
        final ConsumerState startingState,
        final ConsumerState endingState
    ) {
        this.type = type;
        this.id = id;
        this.request = request;
        this.startingState = startingState;
        this.endingState = endingState;
    }
}
