package com.isaolmez.kafkaguide.vanilla.producer.lesson9;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class CustomPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
            Object value, byte[] valueBytes,
            Cluster cluster) {
        final List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        final int numPartitions = partitions.size();
        // Send to the last partition
        if (keyBytes == null) {
            return numPartitions - 1;
        }

        // Other records will get hashed to the rest of the partitions
        return Math.abs(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void close() {
    }
}