package com.salesforce.storm.spout.sideline;

public class SpoutNotOpenedException extends RuntimeException {

    public SpoutNotOpenedException() {
        super("Spout has not yet been opened!");
    }
}
