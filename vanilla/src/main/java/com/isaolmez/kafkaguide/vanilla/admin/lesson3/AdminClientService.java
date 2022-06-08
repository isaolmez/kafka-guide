package com.isaolmez.kafkaguide.vanilla.admin.lesson3;

import com.isaolmez.kafkaguide.vanilla.admin.AdminClientHelper;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.common.Node;

@Slf4j
public class AdminClientService {

    public void describeCluster() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            try {
                final Collection<Node> nodes = adminClient.describeCluster().nodes().get();
                log.info("Cluster nodes: {}", nodes);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void describeClusterWithOptions() {
        AdminClientHelper.runWithAdminClient(adminClient -> {
            try {
                final DescribeClusterOptions describeClusterOptions = new DescribeClusterOptions();
                describeClusterOptions.includeAuthorizedOperations(true);
                describeClusterOptions.timeoutMs(1000);
                final Collection<Node> nodes = adminClient.describeCluster(describeClusterOptions).nodes().get();
                log.info("Cluster nodes: {}", nodes);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
