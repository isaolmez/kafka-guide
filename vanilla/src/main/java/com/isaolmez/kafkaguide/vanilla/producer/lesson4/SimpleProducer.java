package com.isaolmez.kafkaguide.vanilla.producer.lesson4;

import java.time.LocalTime;
import java.util.Properties;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class SimpleProducer {

    public void waitForLingerMs() {
        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("key.serializer", StringSerializer.class.getName());
        kafkaProperties.setProperty("value.serializer", StringSerializer.class.getName());
        kafkaProperties.setProperty("client.id", "batch-client1");
        kafkaProperties.setProperty("linger.ms", "10000");
        kafkaProperties.setProperty("batch.size", "10");

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProperties)) {

            try {
                log.info("Send initiate time: {}", LocalTime.now());
                IntStream.range(0, 1)
                        .forEach(i -> {
                            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", getKey(),
                                    getValue());
                            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                                log.info("Send finish time: {}", LocalTime.now());
                            });
                        });
            } catch (RuntimeException e) {
                log.info("Error occurred when sending message", e);
            }
        }
    }

    public void waitForLingerMsThenSendWhenBatchSize() {
        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("key.serializer", StringSerializer.class.getName());
        kafkaProperties.setProperty("value.serializer", StringSerializer.class.getName());
        kafkaProperties.setProperty("client.id", "batch-client1");
        kafkaProperties.setProperty("linger.ms", "10000");
        kafkaProperties.setProperty("batch.size", "10");

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProperties)) {

            try {
                log.info("Send initiate time: {}", LocalTime.now());
                IntStream.range(0, 10)
                        .forEach(i -> {
                            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", getKey(),
                                    getValue());
                            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                                log.info("Send finish time: {}", LocalTime.now());
                            });
                        });
            } catch (RuntimeException e) {
                log.info("Error occurred when sending message", e);
            }
        }
    }

    private String getKey() {
        return String.valueOf(System.currentTimeMillis());
    }

    private String getValue() {
        return String.format("%s-%s", getClass().getSimpleName(), LocalTime.now().toString());
    }

}
