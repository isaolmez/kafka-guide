package com.isaolmez.kafkaguide.vanilla.admin.lesson6;

import com.isaolmez.kafkaguide.vanilla.admin.AdminClientHelper;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class AdminClientService {

    public void listConsumerGroups() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            try {
                final Collection<ConsumerGroupListing> consumerGroupListings = adminClient.listConsumerGroups().all()
                        .get();
                consumerGroupListings.stream()
                        .forEach(consumerGroupListing -> log.info("Consumer group: {}", consumerGroupListing));
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void listConsumerGroupsWithValid() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            try {
                final Collection<ConsumerGroupListing> consumerGroupListings = adminClient.listConsumerGroups().valid()
                        .get();
                consumerGroupListings.stream()
                        .forEach(consumerGroupListing -> log.info("Consumer group: {}", consumerGroupListing));
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void describeConsumerGroups() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            try {
                final List<String> consumerGroupIds = adminClient.listConsumerGroups().valid().get()
                        .stream()
                        .map(ConsumerGroupListing::groupId)
                        .collect(Collectors.toList());
                final Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap = adminClient.describeConsumerGroups(
                        consumerGroupIds).all().get();
                log.info("Consumer group descriptions: {}", consumerGroupDescriptionMap);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void listConsumerGroupOffsets() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            try {
                final ConsumerGroupListing consumerGroupListing = adminClient.listConsumerGroups().valid().get()
                        .stream()
                        .filter(ConsumerGroupListing::isSimpleConsumerGroup)
                        .findFirst()
                        .get();

                final Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = adminClient.listConsumerGroupOffsets(
                        consumerGroupListing.groupId()).partitionsToOffsetAndMetadata().get();
                log.info("Topic partition offsets: {}", topicPartitionOffsetAndMetadataMap);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
