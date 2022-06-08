package com.isaolmez.kafkaguide.vanilla.admin;

import java.time.Duration;
import java.util.Properties;
import java.util.function.Consumer;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

@Slf4j
@UtilityClass
public class AdminClientHelper {

    public static void runWithAdminClient(Consumer<AdminClient> adminClientOperation) {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        final AdminClient adminClient = AdminClient.create(props);
        adminClientOperation.accept(adminClient);
        adminClient.close(Duration.ofSeconds(30));
    }
}
