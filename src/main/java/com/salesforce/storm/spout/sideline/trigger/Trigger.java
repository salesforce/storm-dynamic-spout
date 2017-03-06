package com.salesforce.storm.spout.sideline.trigger;

import java.util.Map;

public interface Trigger {

    void open(Map config);

    void close();
}
