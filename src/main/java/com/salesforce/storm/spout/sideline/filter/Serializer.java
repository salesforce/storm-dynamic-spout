package com.salesforce.storm.spout.sideline.filter;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.List;

public class Serializer {

    private static final Logger logger = LoggerFactory.getLogger(Serializer.class);

    public static List<FilterChainStep> deserialize(final String value) {
        try {
            final byte[] data = Base64.getDecoder().decode(value);
            final ObjectInputStream objectInputStream = new ObjectInputStream(
                new ByteArrayInputStream(data)
            );
            List<FilterChainStep> steps = (List<FilterChainStep>) objectInputStream.readObject();
            objectInputStream.close();
            return steps;
        } catch (Exception ex) {
            // IOException or ClassNotFoundException most likely
            logger.error("{}", ex);
            return Lists.newArrayList();
        }
    }

    public static String serialize(List<FilterChainStep> steps) {
        try {
            final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            final ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(steps);
            objectOutputStream.close();
            return Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());
        } catch (Exception ex) {
            // IOException most likely
            logger.error("{}", ex);
            return "";
        }
    }
}
