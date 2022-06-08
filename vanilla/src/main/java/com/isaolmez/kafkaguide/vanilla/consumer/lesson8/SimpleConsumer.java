package com.isaolmez.kafkaguide.vanilla.consumer.lesson8;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class SimpleConsumer {


    public void consumeWithSpecificOffsets() {
        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
        kafkaProperties.setProperty("value.deserializer", StringDeserializer.class.getName());
        kafkaProperties.setProperty("group.id", UUID.randomUUID().toString());
        kafkaProperties.setProperty("client.id", UUID.randomUUID().toString());
        kafkaProperties.setProperty("auto.offset.reset", "earliest");
        kafkaProperties.setProperty("max.poll.records", "1");
        kafkaProperties.setProperty("enable.auto.commit", "false");

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProperties)) {
            kafkaConsumer.subscribe(List.of("test"));
            final Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
            int count = 0;
            while (true) {
                final ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                log.info("Consumer records count: {}", consumerRecords.count());
                for (final ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("Consumer record: {}", consumerRecord);
                    offsetMap.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                            new OffsetAndMetadata(consumerRecord.offset() + 1, "no metadata"));
                    if (count % 10 == 0) {
                        log.info("Committing offsets: {}", offsetMap);
                        kafkaConsumer.commitAsync();
                    }

                    count++;
                }

                sleep();
            }
        }
    }

    private void sleep() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
