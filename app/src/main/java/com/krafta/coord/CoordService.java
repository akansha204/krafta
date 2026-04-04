package com.krafta.coord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class CoordService {

    private static class GroupState {
        private String topic;
        private int partitionCount;
        private long generation;
        private final Map<String, MemberState> members = new HashMap<>();
        private final Map<String, List<Integer>> assignments = new HashMap<>();
        private final Map<Integer, String> ownershipRegistry = new HashMap<>();
        private final Map<Integer, Long> offsetRegistry = new HashMap<>();
    }
    private static class MemberState {
        private final String memberId;
        private final long sessionTimeoutMs;
        private long lastHeartbeatMs;

        private MemberState(String memberId, long sessionTimeoutMs, long lastHeartbeatMs) {
            this.memberId = memberId;
            this.sessionTimeoutMs = sessionTimeoutMs;
            this.lastHeartbeatMs = lastHeartbeatMs;
        }
    }

    public record AssignmentSnapshot(
            String topic,
            long generation,
            Map<String, List<Integer>> assignments,
            Map<Integer, String> ownershipRegistry,
            Map<Integer, Long> offsetRegistry
    ) {
        public static AssignmentSnapshot empty() {
            return new AssignmentSnapshot(null, 0, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        }
    }

    private final Map<String, GroupState> groups = new HashMap<>();

    public synchronized AssignmentSnapshot joinConsumer(
            String groupId,
            String memberId,
            String topic,
            int partitionCount,
            long sessionTimeoutMs,
            long nowMs
    ) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be > 0");
        }
        if (sessionTimeoutMs <= 0) {
            throw new IllegalArgumentException("sessionTimeoutMs must be > 0");
        }

        GroupState state = groups.computeIfAbsent(groupId, ignored -> new GroupState());
        if (state.topic == null) {
            state.topic = topic;
        } else if (!state.topic.equals(topic)) {
            throw new IllegalArgumentException("Group already bound to topic: " + state.topic);
        }

        state.partitionCount = partitionCount;
        state.members.put(memberId, new MemberState(memberId, sessionTimeoutMs, nowMs));
        rebalance(state);
        return snapshot(state);
    }

    public synchronized AssignmentSnapshot leaveConsumer(String groupId, String memberId) {
        GroupState state = groups.get(groupId);
        if (state == null) {
            return AssignmentSnapshot.empty();
        }

        state.members.remove(memberId);
        rebalance(state);
        return snapshot(state);
    }

    public synchronized AssignmentSnapshot heartbeat(String groupId, String memberId, long nowMs) {
        GroupState state = groups.get(groupId);
        if (state == null) {
            throw new IllegalStateException("Unknown group: " + groupId);
        }

        MemberState member = state.members.get(memberId);
        if (member == null) {
            throw new IllegalStateException("Unknown member: " + memberId);
        }

        member.lastHeartbeatMs = nowMs;
        return snapshot(state);
    }

    public synchronized Map<String, AssignmentSnapshot> expireTimedOutMembers(long nowMs) {
        Map<String, AssignmentSnapshot> changed = new HashMap<>();

        for (Map.Entry<String, GroupState> entry : groups.entrySet()) {
            GroupState state = entry.getValue();
            List<String> expiredMembers = new ArrayList<>();
            for (MemberState member : state.members.values()) {
                if (nowMs - member.lastHeartbeatMs > member.sessionTimeoutMs) {
                    expiredMembers.add(member.memberId);
                }
            }

            if (!expiredMembers.isEmpty()) {
                for (String memberId : expiredMembers) {
                    state.members.remove(memberId);
                }
                rebalance(state);
                changed.put(entry.getKey(), snapshot(state));
            }
        }

        return changed;
    }

    public synchronized List<Integer> assignmentForMember(String groupId, String memberId) {
        GroupState state = groups.get(groupId);
        if (state == null) {
            return List.of();
        }
        return state.assignments.getOrDefault(memberId, List.of());
    }

    public synchronized boolean claimPartition(String groupId, String memberId, int partition, long generation) {
        GroupState state = groups.get(groupId);
        if (state == null || state.generation != generation) {
            return false;
        }

        List<Integer> assigned = state.assignments.getOrDefault(memberId, List.of());
        if (!assigned.contains(partition)) {
            return false;
        }

        String owner = state.ownershipRegistry.get(partition);
        if (owner != null && !owner.equals(memberId)) {
            return false;
        }

        state.ownershipRegistry.put(partition, memberId);
        return true;
    }

    public synchronized void releasePartition(String groupId, String memberId, int partition) {
        GroupState state = groups.get(groupId);
        if (state == null) {
            return;
        }

        String owner = state.ownershipRegistry.get(partition);
        if (memberId.equals(owner)) {
            state.ownershipRegistry.remove(partition);
        }
    }

    public synchronized Optional<String> ownerOf(String groupId, int partition) {
        GroupState state = groups.get(groupId);
        if (state == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(state.ownershipRegistry.get(partition));
    }

    public synchronized void commitOffset(
            String groupId,
            String memberId,
            long generation,
            int partition,
            long offset
    ) {
        GroupState state = groups.get(groupId);
        if (state == null) {
            throw new IllegalStateException("Unknown group: " + groupId);
        }
        if (state.generation != generation) {
            throw new IllegalStateException("Stale generation: " + generation + " current=" + state.generation);
        }

        String owner = state.ownershipRegistry.get(partition);
        if (!memberId.equals(owner)) {
            throw new IllegalStateException("Partition " + partition + " is not owned by member " + memberId);
        }

        state.offsetRegistry.put(partition, offset);
    }

    public synchronized long readCommittedOffset(String groupId, int partition, long defaultOffset) {
        GroupState state = groups.get(groupId);
        if (state == null) {
            return defaultOffset;
        }
        return state.offsetRegistry.getOrDefault(partition, defaultOffset);
    }

    public synchronized AssignmentSnapshot snapshot(String groupId) {
        GroupState state = groups.get(groupId);
        if (state == null) {
            return AssignmentSnapshot.empty();
        }
        return snapshot(state);
    }

    private AssignmentSnapshot snapshot(GroupState state) {
        Map<String, List<Integer>> assignmentCopy = new TreeMap<>();
        for (Map.Entry<String, List<Integer>> entry : state.assignments.entrySet()) {
            assignmentCopy.put(entry.getKey(), List.copyOf(entry.getValue()));
        }

        return new AssignmentSnapshot(
                state.topic,
                state.generation,
                assignmentCopy,
                Map.copyOf(state.ownershipRegistry),
                Map.copyOf(state.offsetRegistry)
        );
    }

    private void rebalance(GroupState state) {
        state.generation += 1;
        state.assignments.clear();

        List<String> members = new ArrayList<>(state.members.keySet());
        members.sort(Comparator.naturalOrder());

        if (members.isEmpty()) {
            state.ownershipRegistry.clear();
            return;
        }

        for (String memberId : members) {
            state.assignments.put(memberId, new ArrayList<>());
        }

        int n = members.size();
        int p = state.partitionCount;
        for (int memberIndex = 0; memberIndex < n; memberIndex++) {
            int start = (memberIndex * p) / n;
            int endExclusive = ((memberIndex + 1) * p) / n;
            List<Integer> ranges = state.assignments.get(members.get(memberIndex));
            for (int partition = start; partition < endExclusive; partition++) {
                ranges.add(partition);
            }
        }

        // Keep ownership only if partition is still assigned to that owner.
        Map<Integer, String> filteredOwnership = new HashMap<>();
        for (Map.Entry<Integer, String> ownership : state.ownershipRegistry.entrySet()) {
            List<Integer> assigned = state.assignments.getOrDefault(ownership.getValue(), List.of());
            if (assigned.contains(ownership.getKey())) {
                filteredOwnership.put(ownership.getKey(), ownership.getValue());
            }
        }
        state.ownershipRegistry.clear();
        state.ownershipRegistry.putAll(filteredOwnership);
    }

}
