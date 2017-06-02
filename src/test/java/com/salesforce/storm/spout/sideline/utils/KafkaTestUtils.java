package com.salesforce.storm.spout.sideline.utils;

import com.google.common.collect.Lists;
import com.salesforce.storm.spout.sideline.kafka.KafkaTestServer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *
 */
public class KafkaTestUtils {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTestUtils.class);

    // The embedded Kafka server to interact with.
    private final KafkaTestServer kafkaTestServer;

    public KafkaTestUtils(KafkaTestServer kafkaTestServer) {
        this.kafkaTestServer = kafkaTestServer;
    }

    /**
     * Produce some records into the defined kafka namespace.
     *
     * @param numberOfRecords - how many records to produce
     * @param topicName - the namespace name to produce into.
     * @param partitionId - the partition to produce into.
     * @return List of ProducedKafkaRecords.
     */
    public List<ProducedKafkaRecord<byte[], byte[]>> produceRecords(final int numberOfRecords, final String topicName, final int partitionId) {
        // This holds the records we produced
        List<ProducerRecord<byte[], byte[]>> producedRecords = Lists.newArrayList();

        // This holds futures returned
        List<Future<RecordMetadata>> producerFutures = Lists.newArrayList();

        KafkaProducer producer = kafkaTestServer.getKafkaProducer("org.apache.kafka.common.serialization.ByteArraySerializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        for (int x=0; x<numberOfRecords; x++) {
            // Construct key and value
            long timeStamp = Clock.systemUTC().millis();
            String key = "key" + timeStamp;
            String value = "value" + timeStamp;

            // Construct filter
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, partitionId, key.getBytes(Charsets.UTF_8), value.getBytes(Charsets.UTF_8));
            producedRecords.add(record);

            // Send it.
            producerFutures.add(producer.send(record));
        }

        // Publish to the namespace and close.
        producer.flush();
        logger.info("Produce completed");
        producer.close();

        // Loop thru the futures, and build KafkaRecord objects
        List<ProducedKafkaRecord<byte[], byte[]>> kafkaRecords = Lists.newArrayList();
        try {
            for (int x=0; x<numberOfRecords; x++) {
                final RecordMetadata metadata = producerFutures.get(x).get();
                final ProducerRecord producerRecord = producedRecords.get(x);

                kafkaRecords.add(ProducedKafkaRecord.newInstance(metadata, producerRecord));
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return kafkaRecords;
    }
}
