package com.salesforce.storm.spout.sideline.kafka;

import com.salesforce.storm.spout.sideline.KafkaMessage;

public interface DelegateSidelineSpout {

    void open();

    void close();

    void activate();

    void deactivate();

    KafkaMessage nextTuple();

    void ack(Object msgId);

    void fail(Object msgId);

    boolean isFinished();

    void setFinished(boolean finished);

    String getConsumerId();
}
