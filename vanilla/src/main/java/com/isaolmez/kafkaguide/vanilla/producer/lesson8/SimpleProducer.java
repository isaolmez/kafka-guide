package com.isaolmez.kafkaguide.vanilla.producer.lesson8;

import com.isaolmez.kafkaguide.vanilla.avro.models.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class SimpleProducer {

    public void sendWithAvro() {
        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        kafkaProperties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        kafkaProperties.setProperty("client.id", "serializer-client1");
        kafkaProperties.setProperty("schema.registry.url","");

        try (KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(kafkaProperties)) {

            try {
                final Customer customer = Customer.newBuilder()
                        .setFirstName("john")
                        .setLastName("doe")
                        .setAge(22)
                        .build();
                final ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>("test", getKey(),
                        customer);
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
