package com.isaolmez.kafkaguide.vanilla.producer.lesson3;

import java.time.LocalTime;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class SimpleProducer {

    public void send() {
        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("key.serializer", StringSerializer.class.getName());
        kafkaProperties.setProperty("value.serializer", StringSerializer.class.getName());
        kafkaProperties.setProperty("client.id", "idempotent-client1");
        kafkaProperties.setProperty("acks", "all");
        kafkaProperties.setProperty("retries", "5");
        kafkaProperties.setProperty("max.in.flight.requests.per.connection", "2");
        kafkaProperties.setProperty("enable.idempotence", "true");

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProperties)) {

            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", getKey(),
                    getValue());
            try {
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
