package com.isaolmez.kafkaguide.vanilla.consumer.lesson11;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class SimpleConsumer {

    public void consumeWithSeekToBeginning() {
        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
        kafkaProperties.setProperty("value.deserializer", StringDeserializer.class.getName());
        kafkaProperties.setProperty("group.id", UUID.randomUUID().toString());
        kafkaProperties.setProperty("client.id", UUID.randomUUID().toString());
        kafkaProperties.setProperty("auto.offset.reset", "latest");
        kafkaProperties.setProperty("max.poll.records", "1");
        kafkaProperties.setProperty("enable.auto.commit", "false");
        final AtomicBoolean seeked = new AtomicBoolean();

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProperties)) {
            kafkaConsumer.subscribe(List.of("test"), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    if (!seeked.get()) {
                        log.info("Seeking to the end");
                        kafkaConsumer.seekToBeginning(partitions);
                        seeked.set(true);
                    }
                }
            });

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

    public void consumeWithSeekToEnd() {
        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
        kafkaProperties.setProperty("value.deserializer", StringDeserializer.class.getName());
        kafkaProperties.setProperty("group.id", UUID.randomUUID().toString());
        kafkaProperties.setProperty("client.id", UUID.randomUUID().toString());
        kafkaProperties.setProperty("auto.offset.reset", "latest");
        kafkaProperties.setProperty("max.poll.records", "1");
        kafkaProperties.setProperty("enable.auto.commit", "false");
        final AtomicBoolean seeked = new AtomicBoolean();

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProperties)) {
            kafkaConsumer.subscribe(List.of("test"), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    if (!seeked.get()) {
                        log.info("Seeking to the end");
                        kafkaConsumer.seekToEnd(partitions);
                        seeked.set(true);
                    }
                }
            });

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

    public void consumeWithSeekToTime() {
        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
        kafkaProperties.setProperty("value.deserializer", StringDeserializer.class.getName());
        kafkaProperties.setProperty("group.id", UUID.randomUUID().toString());
        kafkaProperties.setProperty("client.id", UUID.randomUUID().toString());
        kafkaProperties.setProperty("auto.offset.reset", "latest");
        kafkaProperties.setProperty("max.poll.records", "1");
        kafkaProperties.setProperty("enable.auto.commit", "false");
        final AtomicBoolean seeked = new AtomicBoolean();

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProperties)) {
            kafkaConsumer.subscribe(List.of("test"), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    if (!seeked.get()) {
                        log.info("Seeking to the time");
                        final long oneHourAgo = Instant.now().minus(1, ChronoUnit.HOURS).getEpochSecond()*1000;
                        final Map<TopicPartition, Long> partitionToTime = partitions.stream()
                                .collect(Collectors.toMap(Function.identity(), p -> oneHourAgo));
                        final Map<TopicPartition, OffsetAndTimestamp> partitionToOffset = kafkaConsumer.offsetsForTimes(
                                partitionToTime);
                        for (final Entry<TopicPartition, OffsetAndTimestamp> entry : partitionToOffset.entrySet()) {
                            log.info("Seeking to the offset: {}", entry);
                            kafkaConsumer.seek(entry.getKey(), entry.getValue().offset());
                        }

                        seeked.set(true);
                    }
                }
            });

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
