package com.isaolmez.kafkaguide.vanilla.admin.lesson5;

import com.isaolmez.kafkaguide.vanilla.admin.AdminClientHelper;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.TopicConfig;

@Slf4j
public class AdminClientService {

    public void describeConfig() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            try {
                final ConfigResource topicResource = new ConfigResource(Type.TOPIC, "test");
                final Config config = adminClient.describeConfigs(List.of(topicResource))
                        .all()
                        .get()
                        .get(topicResource);
                log.info("Topic config: {}", config);

                final List<ConfigEntry> nonDefaultConfigEntries = config.entries().stream()
                        .filter(configEntry -> !configEntry.isDefault())
                        .collect(Collectors.toList());
                log.info("Non-default config entries: {}", nonDefaultConfigEntries);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void alterConfig() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            try {
                final ConfigResource topicResource = new ConfigResource(Type.TOPIC, "test");
                final Config config = adminClient.describeConfigs(List.of(topicResource))
                        .all()
                        .get()
                        .get(topicResource);
                log.info("Topic config: {}", config);

                final ConfigEntry configEntry = new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG,
                        TopicConfig.CLEANUP_POLICY_DELETE);
                final Map<ConfigResource, Collection<AlterConfigOp>> alterConfigOperations = new HashMap<>();
                final AlterConfigOp alterConfigOperation = new AlterConfigOp(configEntry, OpType.SET);
                alterConfigOperations.put(topicResource, List.of(alterConfigOperation));
                adminClient.incrementalAlterConfigs(alterConfigOperations).all().get();

                final Config alteredConfig = adminClient.describeConfigs(List.of(topicResource))
                        .all()
                        .get()
                        .get(topicResource);
                log.info("Altered topic config: {}", alteredConfig);

                final List<ConfigEntry> nonDefaultConfigEntries = config.entries().stream()
                        .filter(entry -> !entry.isDefault())
                        .collect(Collectors.toList());
                log.info("Non-default config entries: {}", nonDefaultConfigEntries);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
