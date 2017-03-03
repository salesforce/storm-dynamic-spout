package com.salesforce.storm.spout.sideline.metrics;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Tells the metrics recorder that this is a scoped metric, meaning it has a second level of granularity in its
 * key for tracking metrics by.
 */
@Target({ FIELD })
@Retention(RUNTIME)
public @interface Scoped {
}
