package com.krafta.consumer;

import com.krafta.broker.Broker;
import com.krafta.coord.CoordService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumerGroup {
    private final String groupId;
    private final String topic;
    private final Broker broker;
    private final CoordService coordService;
    private final long sessionTimeoutMs;
    private final List<Consumer> consumersList;
    private final Map<Integer, Consumer> partitionAssignment;

    public ConsumerGroup(
            String groupId,
            String topic,
            Broker broker,
            CoordService coordService,
            long sessionTimeoutMs,
            List<Consumer> consumersList,
            Map<Integer, Consumer> partitionAssignment
    ) {
        this.groupId = groupId;
        this.topic = topic;
        this.broker = broker;
        this.coordService = coordService;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.consumersList = consumersList;
        this.partitionAssignment = (partitionAssignment != null )? new HashMap<>(partitionAssignment) : new HashMap<>();
    }

    public CoordService.AssignmentSnapshot registerConsumer(Consumer consumer, long nowMs) throws Exception {
        if (!consumersList.contains(consumer)) {
            consumersList.add(consumer);
        }

        CoordService.AssignmentSnapshot snapshot = coordService.joinConsumer(
                groupId,
                consumer.getMemberId(),
                topic,
                broker.getPartitionCount(topic),
                sessionTimeoutMs,
                nowMs
        );
        applySnapshot(snapshot);
        return snapshot;
    }

    public CoordService.AssignmentSnapshot unregisterConsumer(String memberId) throws Exception {
        consumersList.removeIf(consumer -> consumer.getMemberId().equals(memberId));
        CoordService.AssignmentSnapshot snapshot = coordService.leaveConsumer(groupId, memberId);
        applySnapshot(snapshot);
        return snapshot;
    }

    public CoordService.AssignmentSnapshot heartbeat(String memberId, long nowMs) throws Exception {
        CoordService.AssignmentSnapshot snapshot = coordService.heartbeat(groupId, memberId, nowMs);
        applySnapshot(snapshot);
        return snapshot;
    }

    public CoordService.AssignmentSnapshot expireTimedOutMembers(long nowMs) throws Exception {
        Map<String, CoordService.AssignmentSnapshot> changed = coordService.expireTimedOutMembers(nowMs);
        CoordService.AssignmentSnapshot snapshot = changed.getOrDefault(groupId, coordService.snapshot(groupId));
        applySnapshot(snapshot);
        return snapshot;
    }

    public CoordService.AssignmentSnapshot rebalanceNow() throws Exception {
        CoordService.AssignmentSnapshot snapshot = coordService.snapshot(groupId);
        applySnapshot(snapshot);
        return snapshot;
    }

    public Map<Integer, Consumer> getPartitionAssignment() {
        return Map.copyOf(partitionAssignment);
    }

    private void applySnapshot(CoordService.AssignmentSnapshot snapshot) throws Exception {
        if (snapshot.assignments().isEmpty()) {
            partitionAssignment.clear();
            for (Consumer consumer : consumersList) {
                consumer.clearAssignments();
            }
            return;
        }

        partitionAssignment.clear();

        for (Consumer consumer : consumersList) {
            List<Integer> assigned = snapshot.assignments().getOrDefault(consumer.getMemberId(), List.of());
            consumer.syncFromCoordinator(coordService, groupId, snapshot.generation(), assigned);
            for (int partition : assigned) {
                partitionAssignment.put(partition, consumer);
            }
        }

        for (Map.Entry<Integer, Consumer> entry : partitionAssignment.entrySet()) {
            System.out.println("Assigned: " + entry.getKey() + " -> Consumer " + consumersList.indexOf(entry.getValue()));
        }
    }
}
