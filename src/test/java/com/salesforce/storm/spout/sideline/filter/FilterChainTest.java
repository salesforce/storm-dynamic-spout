package com.salesforce.storm.spout.sideline.filter;

import com.salesforce.storm.spout.sideline.KafkaMessage;
import com.salesforce.storm.spout.sideline.TupleMessageId;
import com.salesforce.storm.spout.sideline.filter.FilterChain;
import com.salesforce.storm.spout.sideline.filter.FilterChainStep;
import com.salesforce.storm.spout.sideline.trigger.SidelineIdentifier;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import static org.junit.Assert.*;

public class FilterChainTest {

    /**
     * Test that each step of a chain is processed using a string
     */
    @Test
    public void testChain() {
        final KafkaMessage message = new KafkaMessage(
            new TupleMessageId("foobar", 1, 0L, "FakeConsumer"),
            new Values("Foo")
        );

        final FilterChain filterChain = new FilterChain()
            .addStep(new SidelineIdentifier(), new StepOne())
            .addStep(new SidelineIdentifier(), new StepTwo())
            .addStep(new SidelineIdentifier(), new StepThree())
        ;

        assertTrue(filterChain.filter(message));
    }

    public static class StepOne implements FilterChainStep {

        public boolean filter(KafkaMessage message) {
            return true;
        }
    }

    public static class StepTwo implements FilterChainStep {

        public boolean filter(KafkaMessage message) {
            if (message.getValues().get(0).equals("Hello WorldBar")) {
                return false;
            }
            return true;
        }
    }

    public static class StepThree implements FilterChainStep {

        public boolean filter(KafkaMessage message) {
            return true;
        }
    }
}
