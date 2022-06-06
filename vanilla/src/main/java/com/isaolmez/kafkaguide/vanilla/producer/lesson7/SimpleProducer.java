package com.isaolmez.kafkaguide.vanilla.producer.lesson7;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class SimpleProducer {

    public void sendWithCustomSerializer() {
        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("key.serializer", StringSerializer.class.getName());
        kafkaProperties.setProperty("value.serializer", PersonSerializer.class.getName());
        kafkaProperties.setProperty("client.id", "serializer-client1");

        try (KafkaProducer<String, Person> kafkaProducer = new KafkaProducer<>(kafkaProperties)) {

            try {
                final Person person = Person.builder()
                        .firstName("john")
                        .lastName("doe")
                        .age(22)
                        .build();
                final ProducerRecord<String, Person> producerRecord = new ProducerRecord<>("test", getKey(),
                        person);
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
}
