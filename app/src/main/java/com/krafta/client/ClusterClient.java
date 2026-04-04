package com.krafta.client;

import com.krafta.api.FetchRequest;
import com.krafta.api.FetchResponse;
import com.krafta.api.ListOffsetsRequest;
import com.krafta.api.ListOffsetsResponse;
import com.krafta.api.ProduceRequest;
import com.krafta.api.ProduceResponse;
import com.krafta.broker.Broker;
import com.krafta.coord.ClusterCoordService;

import java.util.HashMap;
import java.util.Map;

public class ClusterClient {
    private final ClusterCoordService coordService;
    private final Map<Integer, Broker> brokersById;
    private ClusterCoordService.ClusterMetadata metadataCache;

    public ClusterClient(ClusterCoordService coordService, Map<Integer, Broker> brokersById) {
        this.coordService = coordService;
        this.brokersById = new HashMap<>(brokersById);
        refreshMetadata();
    }

    public void refreshMetadata() {
        this.metadataCache = coordService.metadataSnapshot();
    }

    public ProduceResponse produce(String topic, int partition, byte[] payload) throws Exception {
        Broker owner = ownerBroker(topic, partition);
        return owner.produce(new ProduceRequest(topic, partition, java.util.List.of(payload)));
    }

    public FetchResponse fetch(String topic, int partition, long offset, int maxRecords, long maxWaitMs) throws Exception {
        Broker owner = ownerBroker(topic, partition);
        return owner.fetch(new FetchRequest(topic, partition, offset, maxRecords, maxWaitMs));
    }

    public ListOffsetsResponse listOffsets(String topic, int partition, ListOffsetsRequest.OffsetSpec spec) throws Exception {
        Broker owner = ownerBroker(topic, partition);
        return owner.listOffsets(new ListOffsetsRequest(topic, partition, spec));
    }

    public int ownerBrokerId(String topic, int partition) {
        ClusterCoordService.TopicPlacement placement = metadataCache.topics().get(topic);
        if (placement == null) {
            throw new IllegalStateException("Unknown topic in metadata cache: " + topic);
        }
        Integer owner = placement.ownerByPartition().get(partition);
        if (owner == null) {
            throw new IllegalArgumentException("Unknown partition " + partition + " for topic " + topic);
        }
        return owner;
    }

    private Broker ownerBroker(String topic, int partition) {
        int ownerId = ownerBrokerId(topic, partition);
        Broker broker = brokersById.get(ownerId);
        if (broker == null) {
            throw new IllegalStateException("Owner broker not available in client map: " + ownerId);
        }
        return broker;
    }
}
