package com.isaolmez.kafkaguide.vanilla.producer.lesson10;

import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class SimpleProducer {

    public void sendWithHeaders() {
        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("key.serializer", StringSerializer.class.getName());
        kafkaProperties.setProperty("value.serializer", StringSerializer.class.getName());
        kafkaProperties.setProperty("client.id", "partitioner-client1");

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProperties)) {

            try {
                final ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", getKey(),
                        getValue());
                producerRecord.headers().add("Category", "Testing".getBytes(StandardCharsets.UTF_8));
                kafkaProducer.send(producerRecord);
            } catch (RuntimeException e) {
                log.info("Error occurred when sending message", e);
            }

            kafkaProducer.flush();
        }
    }

    private String getKey() {
        return String.valueOf(System.currentTimeMillis());
    }

    private String getValue() {
        return String.format("%s-%s", getClass().getSimpleName(), LocalTime.now().toString());
    }
}
