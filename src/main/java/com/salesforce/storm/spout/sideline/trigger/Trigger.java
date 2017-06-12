package com.salesforce.storm.spout.sideline.trigger;

import java.io.Serializable;
import java.util.Map;

public interface Trigger extends Serializable {

    default void open(Map config) {}

    default void close() {}
}
