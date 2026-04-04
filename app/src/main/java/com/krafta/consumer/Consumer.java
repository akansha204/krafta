package com.krafta.consumer;

import com.krafta.api.FetchRequest;
import com.krafta.api.FetchResponse;
import com.krafta.api.ListOffsetsRequest;
import com.krafta.api.ListOffsetsResponse;
import com.krafta.broker.Broker;
import com.krafta.coord.CoordService;
import com.krafta.storage.Record;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Consumer {
    private final String memberId;
    private final Broker broker;
    private final String topic;
    private final List<Integer> partitions;
    private final Map<Integer, Long> partitionOffsets = new HashMap<>();
    private CoordService coordService;
    private String groupId;
    private long generation;

    public Consumer(Broker broker, String topic, List<Integer> partitionList) throws Exception {
        this(UUID.randomUUID().toString(), broker, topic, partitionList);
    }

    public Consumer(String memberId, Broker broker, String topic, List<Integer> partitionList) throws Exception {
        this.memberId = memberId;
        this.broker = broker;
        this.topic = topic;
        this.partitions = new ArrayList<>(partitionList);

        for (int partition : partitionList) {
            ListOffsetsResponse earliest = broker.listOffsets(
                    new ListOffsetsRequest(topic, partition, ListOffsetsRequest.OffsetSpec.EARLIEST)
            );
            long startOffset = earliest.offset() == 0 ? 1L : earliest.offset();
            partitionOffsets.put(partition, startOffset);
        }
    }

    public String getMemberId() {
        return memberId;
    }

    public void poll(int maxMessages) throws Exception {
        for (int partition : partitions) {
            if (coordService != null) {
                boolean claimed = coordService.claimPartition(groupId, memberId, partition, generation);
                if (!claimed) {
                    continue;
                }
            }

            long currentOffset = partitionOffsets.getOrDefault(partition, 1L);
            FetchResponse response = broker.fetch(new FetchRequest(topic, partition, currentOffset, maxMessages, 0));

            for (Record msg : response.records()) {
                System.out.println(
                        "Consumed: topic=" + topic +
                                ", partition=" + partition +
                                ", offset=" + msg.offset +
                                ", payload=" + new String(msg.payload, StandardCharsets.UTF_8)
                );
                currentOffset = msg.offset + 1;
            }

            partitionOffsets.put(partition, currentOffset);
            if (coordService != null) {
                coordService.commitOffset(groupId, memberId, generation, partition, currentOffset);
            }
        }
    }

    public List<Record> pollPartition(int partition, int maxMessages, long maxWaitMs) throws Exception {
        long currentOffset = partitionOffsets.getOrDefault(partition, 1L);
        FetchResponse response = broker.fetch(new FetchRequest(topic, partition, currentOffset, maxMessages, maxWaitMs));
        if (!response.records().isEmpty()) {
            long nextOffset = response.records().get(response.records().size() - 1).offset + 1;
            partitionOffsets.put(partition, nextOffset);
        }
        return response.records();
    }

    public void addPartition(int partition) throws Exception {
        if (!partitions.contains(partition)) {
            partitions.add(partition);
            ListOffsetsResponse earliest = broker.listOffsets(
                    new ListOffsetsRequest(topic, partition, ListOffsetsRequest.OffsetSpec.EARLIEST)
            );
            long startOffset = earliest.offset() == 0 ? 1L : earliest.offset();
            partitionOffsets.put(partition, startOffset);
        }
    }

    public void syncFromCoordinator(
            CoordService coordService,
            String groupId,
            long generation,
            List<Integer> assignedPartitions
    ) throws Exception {
        this.coordService = coordService;
        this.groupId = groupId;
        this.generation = generation;

        partitions.clear();
        partitionOffsets.clear();
        for (int partition : assignedPartitions) {
            partitions.add(partition);
            ListOffsetsResponse earliest = broker.listOffsets(
                    new ListOffsetsRequest(topic, partition, ListOffsetsRequest.OffsetSpec.EARLIEST)
            );
            long defaultOffset = earliest.offset() == 0 ? 1L : earliest.offset();
            long committed = coordService.readCommittedOffset(groupId, partition, defaultOffset);
            partitionOffsets.put(partition, committed);
        }
    }

    public void heartbeat(long nowMs) {
        if (coordService != null) {
            coordService.heartbeat(groupId, memberId, nowMs);
        }
    }

    public void clearAssignments() {
        partitions.clear();
        partitionOffsets.clear();
    }
}
