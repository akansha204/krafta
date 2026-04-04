package com.krafta.coord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class ClusterCoordService {

    private static class TopicMetadata {
        private final String topic;
        private final int partitionCount;
        private final Map<Integer, Integer> ownerByPartition;

        private TopicMetadata(String topic, int partitionCount, Map<Integer, Integer> ownerByPartition) {
            this.topic = topic;
            this.partitionCount = partitionCount;
            this.ownerByPartition = ownerByPartition;
        }
    }

    private static class BrokerRegistration {
        private final int brokerId;
        private final String endpoint;
        private final long sessionTimeoutMs;
        private long lastHeartbeatMs;

        private BrokerRegistration(int brokerId, String endpoint, long sessionTimeoutMs, long lastHeartbeatMs) {
            this.brokerId = brokerId;
            this.endpoint = endpoint;
            this.sessionTimeoutMs = sessionTimeoutMs;
            this.lastHeartbeatMs = lastHeartbeatMs;
        }
    }

    public record ClusterMetadata(
            Map<Integer, String> aliveBrokers,
            Map<String, TopicPlacement> topics
    ) {
    }

    private final Map<Integer, BrokerRegistration> brokers = new HashMap<>();
    private final Map<String, TopicMetadata> topics = new HashMap<>();

    public synchronized void registerBroker(
            int brokerId,
            String endpoint,
            long sessionTimeoutMs,
            long nowMs
    ) {
        if (sessionTimeoutMs <= 0) {
            throw new IllegalArgumentException("sessionTimeoutMs must be > 0");
        }
        brokers.put(brokerId, new BrokerRegistration(brokerId, endpoint, sessionTimeoutMs, nowMs));
    }

    public synchronized void brokerHeartbeat(int brokerId, long nowMs) {
        BrokerRegistration broker = brokers.get(brokerId);
        if (broker == null) {
            throw new IllegalStateException("Unknown broker: " + brokerId);
        }
        broker.lastHeartbeatMs = nowMs;
    }

    public synchronized List<Integer> expireTimedOutBrokers(long nowMs) {
        List<Integer> expired = new ArrayList<>();
        for (BrokerRegistration broker : brokers.values()) {
            if (nowMs - broker.lastHeartbeatMs > broker.sessionTimeoutMs) {
                expired.add(broker.brokerId);
            }
        }

        for (int brokerId : expired) {
            brokers.remove(brokerId);
        }
        return expired;
    }

    public synchronized void createTopicStaticPlacement(String topic, int partitionCount) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be > 0");
        }
        if (topics.containsKey(topic)) {
            throw new IllegalArgumentException("Topic already exists: " + topic);
        }

        List<Integer> alive = sortedAliveBrokerIds();
        if (alive.isEmpty()) {
            throw new IllegalStateException("No alive brokers to place partitions");
        }

        Map<Integer, Integer> ownerByPartition = new TreeMap<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            int ownerBrokerId = alive.get(partition % alive.size());
            ownerByPartition.put(partition, ownerBrokerId);
        }

        topics.put(topic, new TopicMetadata(topic, partitionCount, ownerByPartition));
    }

    public synchronized int ownerBroker(String topic, int partition) {
        TopicMetadata metadata = topics.get(topic);
        if (metadata == null) {
            throw new IllegalStateException("Unknown topic: " + topic);
        }
        Integer owner = metadata.ownerByPartition.get(partition);
        if (owner == null) {
            throw new IllegalArgumentException("Unknown partition " + partition + " for topic " + topic);
        }
        return owner;
    }

    public synchronized ClusterMetadata metadataSnapshot() {
        Map<Integer, String> aliveBrokers = new TreeMap<>();
        for (BrokerRegistration broker : brokers.values()) {
            aliveBrokers.put(broker.brokerId, broker.endpoint);
        }

        Map<String, TopicPlacement> topicPlacements = new TreeMap<>();
        for (TopicMetadata topic : topics.values()) {
            topicPlacements.put(
                    topic.topic,
                    new TopicPlacement(topic.partitionCount, Map.copyOf(topic.ownerByPartition))
            );
        }
        return new ClusterMetadata(aliveBrokers, topicPlacements);
    }

    public synchronized Optional<TopicPlacement> topicPlacement(String topic) {
        TopicMetadata metadata = topics.get(topic);
        if (metadata == null) {
            return Optional.empty();
        }
        return Optional.of(new TopicPlacement(metadata.partitionCount, Map.copyOf(metadata.ownerByPartition)));
    }

    private List<Integer> sortedAliveBrokerIds() {
        List<Integer> brokerIds = new ArrayList<>(brokers.keySet());
        brokerIds.sort(Comparator.naturalOrder());
        return brokerIds;
    }

    public record TopicPlacement(int partitionCount, Map<Integer, Integer> ownerByPartition) {
    }
}
