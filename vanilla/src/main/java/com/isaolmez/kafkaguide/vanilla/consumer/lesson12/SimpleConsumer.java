package com.isaolmez.kafkaguide.vanilla.consumer.lesson12;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class SimpleConsumer {

    public void consumeWithWakeUp() {

        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
        kafkaProperties.setProperty("value.deserializer", StringDeserializer.class.getName());
        kafkaProperties.setProperty("group.id", UUID.randomUUID().toString());
        kafkaProperties.setProperty("client.id", UUID.randomUUID().toString());
        kafkaProperties.setProperty("auto.offset.reset", "earliest");
        kafkaProperties.setProperty("max.poll.records", "1");
        kafkaProperties.setProperty("enable.auto.commit", "false");

        final Thread mainThread = Thread.currentThread();
        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProperties)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Starting exit...");
                kafkaConsumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    log.error("Error occurred.", e);
                }
            }));
            kafkaConsumer.subscribe(List.of("test"));

            while (true) {
                final ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                log.info("Consumer records count: {}", consumerRecords.count());
                for (final ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("Consumer record: {}", consumerRecord);
                }

                kafkaConsumer.commitAsync();
                sleep();
            }
        }
    }

    private void sleep() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
