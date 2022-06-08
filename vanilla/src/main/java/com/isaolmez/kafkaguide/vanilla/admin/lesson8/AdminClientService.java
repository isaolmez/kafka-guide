package com.isaolmez.kafkaguide.vanilla.admin.lesson8;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;

@Slf4j
public class AdminClientService {

    public void mockAdminClient() {
        Node broker = new Node(0, "localhost", 9092);
        final AdminClient adminClient = new MockAdminClient(Collections.singletonList(broker), broker);

        try {
            final Set<String> topicNames = adminClient.listTopics().names().get();
            log.info("Initial topics: {}", topicNames);

            final NewTopic newTopic = new NewTopic("mock", 1, (short) 1);
            adminClient.createTopics(List.of(newTopic)).all().get();

            final Set<String> finalTopicNames = adminClient.listTopics().names().get();
            log.info("Final topics: {}", finalTopicNames);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        adminClient.close();
    }
}
