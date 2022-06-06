package com.isaolmez.kafkaguide.vanilla.producer.lesson11;

import java.time.LocalTime;
import java.util.Properties;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

@Slf4j
public class SimpleProducer {

    public void sendWithInterceptor() {
        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("key.serializer", StringSerializer.class.getName());
        kafkaProperties.setProperty("value.serializer", StringSerializer.class.getName());
        kafkaProperties.setProperty("client.id", "interceptor-client1");
        kafkaProperties.setProperty("interceptor.classes", CountingProducerInterceptor.class.getName());

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProperties)) {

            try {
                IntStream.range(0, 10)
                        .forEach(i -> {
                            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", getKey(),
                                    getValue());
                            kafkaProducer.send(producerRecord);
                        });
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
