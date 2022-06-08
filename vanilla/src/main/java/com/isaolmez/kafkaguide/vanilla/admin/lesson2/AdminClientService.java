package com.isaolmez.kafkaguide.vanilla.admin.lesson2;

import com.isaolmez.kafkaguide.vanilla.admin.AdminClientHelper;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicCollection.TopicNameCollection;

@Slf4j
public class AdminClientService {

    public void createTopic() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            try {
                final String topicName = "test" + UUID.randomUUID();
                final NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
                adminClient.createTopics(List.of(newTopic)).all().get();
                final Set<String> topicNames = adminClient.listTopics().names().get();
                log.info("Topic names: {}", topicNames);
                if (!topicNames.contains(topicName)) {
                    throw new IllegalStateException("Cannot find the new topic");
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void deleteTopic() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            try {
                final Set<String> topicNames = adminClient.listTopics().names().get();
                log.info("Topic names: {}", topicNames);
                final List<String> testTopics = topicNames.stream()
                        .filter(topicName -> topicName.startsWith("test"))
                        .filter(topicName -> !topicName.equals("test"))
                        .collect(Collectors.toList());
                final TopicNameCollection topicNameCollection = TopicCollection.ofTopicNames(testTopics);
                adminClient.deleteTopics(topicNameCollection).all().get();

                final Set<String> finalTopicNames = adminClient.listTopics().names().get();
                log.info("Final topic names: {}", finalTopicNames);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void deleteTopicWithOptions() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            try {
                final Set<String> topicNames = adminClient.listTopics().names().get();
                log.info("Topic names: {}", topicNames);
                final List<String> testTopics = topicNames.stream()
                        .filter(topicName -> topicName.startsWith("test"))
                        .filter(topicName -> !topicName.equals("test"))
                        .collect(Collectors.toList());
                final TopicNameCollection topicNameCollection = TopicCollection.ofTopicNames(testTopics);
                final DeleteTopicsOptions deleteTopicsOptions = new DeleteTopicsOptions();
                deleteTopicsOptions.retryOnQuotaViolation(true);
                deleteTopicsOptions.timeoutMs(1000);
                adminClient.deleteTopics(topicNameCollection, deleteTopicsOptions).all().get();

                final Set<String> finalTopicNames = adminClient.listTopics().names().get();
                log.info("Final topic names: {}", finalTopicNames);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
