package com.isaolmez.kafkaguide.vanilla.admin.lesson4;

import com.isaolmez.kafkaguide.vanilla.admin.AdminClientHelper;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.TopicDescription;

@Slf4j
public class AdminClientService {

    public void describeTopic() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            try {
                final Map<String, TopicDescription> topicDescriptionMap = adminClient.describeTopics(List.of("test"))
                        .all().get();
                log.info("Topic description map: {}", topicDescriptionMap);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
