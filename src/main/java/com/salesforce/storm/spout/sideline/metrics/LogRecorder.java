package com.salesforce.storm.spout.sideline.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.concurrent.Callable;

/**
 * A wrapper for recording metrics in Storm
 *
 * Learn more about Storm metrics here: http://storm.apache.org/releases/1.0.1/Metrics.html
 *
 * Use this as an instance variable on your bolt, make sure to create it inside of prepareBolt()
 * and pass it down stream to any classes that need to track metrics in your application.
 *
 */
public class LogRecorder implements MetricsRecorder {

    private static final Logger logger = LoggerFactory.getLogger(LogRecorder.class);


    @Override
    public void count(Class sourceClass, String metricName) {
        count(sourceClass, metricName, 1);
    }

    @Override
    public void count(Class sourceClass, String metricName, long incrementBy) {
        logger.info("[COUNTER] {} + {}", generateKey(sourceClass, metricName), incrementBy);
    }

    @Override
    public void average(Class sourceClass, String metricName, Object value) {
        logger.info("[AVERAGE] {} + {}", generateKey(sourceClass, metricName), value);
    }

    @Override
    public <T> T timer(Class sourceClass, String metricName, Callable<T> callable) throws Exception {
        // Wrap in timing
        final long start = Clock.systemUTC().millis();
        T result = callable.call();
        final long end = Clock.systemUTC().millis();

        // Update
        timer(sourceClass, metricName, (end - start));

        // return result.
        return (T) result;
    }

    @Override
    public void timer(Class sourceClass, String metricName, long timeInMs) throws Exception {
        logger.info("[TIMER] {} + {}", generateKey(sourceClass, metricName), timeInMs);
    }

    private String generateKey(Class sourceClass, String metricName) {
        return new StringBuilder(sourceClass.getSimpleName())
                .append(".")
                .append(metricName)
                .toString();
    }
}
