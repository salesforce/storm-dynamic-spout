package com.salesforce.storm.spout.sideline.trigger;

import java.io.Serializable;
import java.util.Map;

public interface Trigger extends Serializable {

    void open(Map config);

    void close();
}
