package com.salesforce.storm.spout.sideline.kafka.deserializer.compat;

import com.google.common.collect.Lists;
import com.salesforce.storm.spout.sideline.kafka.deserializer.Deserializer;
import org.apache.storm.shade.com.google.common.base.Charsets;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Test for compatibility layer to Storm-Kafka scheme.
 */
public class AbstractSchemeTest {

    /**
     * Silly test to go from String -> byte[] -> ByteBuffer -> String.
     * Heh.
     */
    @Test
    public void testDeserialize() {
        final String value = "This Is My Text\uD83D\uDC7B";
        final byte[] valueBytes = value.getBytes(Charsets.UTF_8);

        // Pull implementation into a Deserializer obj.
        final Deserializer myScheme = new MyScheme();

        // Attempt to deserialize
        final Values myValues = myScheme.deserialize("TopicName", 2, 2222L, "Key".getBytes(Charsets.UTF_8), valueBytes);

        // Validate
        assertNotNull(myValues);
        assertEquals("Should have 1 entry", 1, myValues.size());
        assertEquals("Should be our value", value, myValues.get(0));
    }

    /**
     * Silly test to go from [] -> [] -> [] -> "".
     * Heh.
     */
    @Test
    public void testDeserializeWithEmptyByteArray() {
        final byte[] inputNullBytes = new byte[0];

        // Pull implementation into a Deserializer obj.
        final Deserializer myScheme = new MyScheme();

        // Attempt to deserialize
        final Values myValues = myScheme.deserialize("TopicName", 2, 2222L, "Key".getBytes(Charsets.UTF_8), inputNullBytes);

        // Validate
        assertNotNull(myValues);
        assertEquals("Should have 1 entry", 1, myValues.size());
        assertEquals("Should be our value", "", myValues.get(0));
    }

    /**
     * If we return null, return null.
     */
    @Test
    public void testDeserializeReturnsNull() {
        // Create implementation, and force abstract implementation to return null.
        final MyScheme myScheme = new MyScheme();
        myScheme.setReturnNull(true);

        // Attempt to deserialize
        final Values myValues = myScheme.deserialize("TopicName", 2, 2222L, "Key".getBytes(Charsets.UTF_8), "value".getBytes(Charsets.UTF_8));

        // Validate
        assertNull("Should pass the null thru", myValues);
    }

    /**
     * Test Implementation.
     */
    private static class MyScheme extends AbstractScheme {

        private boolean returnNull = false;

        @Override
        public List<Object> deserialize(ByteBuffer ser) {
            // Hack to force a return value
            if (returnNull) {
                return null;
            }

            // Probably a better way to do this juggling.
            if (ser == null) {
                return null;
            }
            ser.rewind();
            byte[] bytes = new byte[ser.remaining()];
            ser.get(bytes, 0, bytes.length);

            return Lists.newArrayList(new String(bytes, Charsets.UTF_8));
        }

        @Override
        public Fields getOutputFields() {
            return new Fields("MyFields", "Here", "For", "You");
        }

        public void setReturnNull(boolean returnNull) {
            this.returnNull = returnNull;
        }
    }
}