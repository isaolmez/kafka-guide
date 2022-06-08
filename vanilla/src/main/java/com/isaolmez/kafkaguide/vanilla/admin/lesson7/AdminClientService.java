package com.isaolmez.kafkaguide.vanilla.admin.lesson7;

import com.isaolmez.kafkaguide.vanilla.admin.AdminClientHelper;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class AdminClientService {

    public void listOffsets() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            try {
                final ConsumerGroupListing consumerGroupListing = adminClient.listConsumerGroups().valid().get()
                        .stream()
                        .filter(ConsumerGroupListing::isSimpleConsumerGroup)
                        .findFirst()
                        .get();

                final Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = adminClient.listConsumerGroupOffsets(
                        consumerGroupListing.groupId()).partitionsToOffsetAndMetadata().get();

                final Map<TopicPartition, OffsetSpec> offsetSpecMap = topicPartitionOffsetAndMetadataMap.keySet()
                        .stream()
                        .collect(Collectors.toMap(Function.identity(), topicPartition -> OffsetSpec.earliest()));

                final Map<TopicPartition, ListOffsetsResultInfo> latestOffsets = adminClient.listOffsets(
                        offsetSpecMap).all().get();
                log.info("Latest offsets: {}", latestOffsets);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void alterConsumerOffsets() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            try {
                final ConsumerGroupListing consumerGroupListing = adminClient.listConsumerGroups().valid().get()
                        .stream()
                        .filter(ConsumerGroupListing::isSimpleConsumerGroup)
                        .findFirst()
                        .get();

                final Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = adminClient.listConsumerGroupOffsets(
                        consumerGroupListing.groupId()).partitionsToOffsetAndMetadata().get();

                final Map<TopicPartition, OffsetSpec> offsetSpecMap = topicPartitionOffsetAndMetadataMap.keySet()
                        .stream()
                        .collect(Collectors.toMap(Function.identity(), topicPartition -> OffsetSpec.earliest()));

                final Map<TopicPartition, ListOffsetsResultInfo> latestOffsets = adminClient.listOffsets(offsetSpecMap)
                        .all().get();

                final Map<TopicPartition, OffsetAndMetadata> resetOffsets = latestOffsets.entrySet().stream()
                        .collect(Collectors.toMap(Entry::getKey,
                                entry -> new OffsetAndMetadata(entry.getValue().offset())));

                adminClient.alterConsumerGroupOffsets(consumerGroupListing.groupId(), resetOffsets).all().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
