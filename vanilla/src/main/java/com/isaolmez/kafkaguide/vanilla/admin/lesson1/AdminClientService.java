package com.isaolmez.kafkaguide.vanilla.admin.lesson1;

import com.isaolmez.kafkaguide.vanilla.admin.AdminClientHelper;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;

@Slf4j
public class AdminClientService {

    public void listTopicsWithNames() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            final ListTopicsResult listTopicsResult = adminClient.listTopics();
            try {
                final Set<String> topicNames = listTopicsResult.names().get();
                log.info("Topics: {}", topicNames);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void listTopicsWithDetails() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            final ListTopicsResult listTopicsResult = adminClient.listTopics();
            try {
                final Collection<TopicListing> topicListings = listTopicsResult.listings().get();
                log.info("Topics: {}", topicListings);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void listTopicsWithOptions() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            final ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
            listTopicsOptions.listInternal(false);
            listTopicsOptions.timeoutMs(1000);
            final ListTopicsResult listTopicsResult = adminClient.listTopics(listTopicsOptions);
            try {
                final Collection<TopicListing> topicListings = listTopicsResult.listings().get();
                log.info("Topics: {}", topicListings);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
