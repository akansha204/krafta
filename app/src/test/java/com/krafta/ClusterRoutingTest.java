package com.krafta;

import com.krafta.api.FetchResponse;
import com.krafta.api.ListOffsetsRequest;
import com.krafta.api.ListOffsetsResponse;
import com.krafta.broker.Broker;
import com.krafta.client.ClusterClient;
import com.krafta.coord.ClusterCoordService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClusterRoutingTest {

    @Test
    void topicPlacementAndOwnerRoutingWorkAcrossBrokers(@TempDir Path tempDir) throws Exception {
        long now = System.currentTimeMillis();
        ClusterCoordService clusterCoord = new ClusterCoordService();

        Broker broker1 = new Broker(
                tempDir.resolve("broker-1").toString(),
                1024 * 1024,
                24L * 60 * 60 * 1000,
                7L * 24 * 60 * 60 * 1000,
                clusterCoord,
                1,
                "broker-1",
                5_000
        );
        Broker broker2 = new Broker(
                tempDir.resolve("broker-2").toString(),
                1024 * 1024,
                24L * 60 * 60 * 1000,
                7L * 24 * 60 * 60 * 1000,
                clusterCoord,
                2,
                "broker-2",
                5_000
        );
        Broker broker3 = new Broker(
                tempDir.resolve("broker-3").toString(),
                1024 * 1024,
                24L * 60 * 60 * 1000,
                7L * 24 * 60 * 60 * 1000,
                clusterCoord,
                3,
                "broker-3",
                5_000
        );

        broker1.heartbeatCluster(now + 1);
        broker2.heartbeatCluster(now + 1);
        broker3.heartbeatCluster(now + 1);

        broker1.createTopicInCluster("orders", 6);
        broker2.syncTopicFromCluster("orders");
        broker3.syncTopicFromCluster("orders");

        Map<Integer, Broker> brokers = new HashMap<>();
        brokers.put(1, broker1);
        brokers.put(2, broker2);
        brokers.put(3, broker3);
        ClusterClient client = new ClusterClient(clusterCoord, brokers);

        ClusterCoordService.TopicPlacement placement = clusterCoord.topicPlacement("orders").orElseThrow();
        assertEquals(Map.of(0, 1, 1, 2, 2, 3, 3, 1, 4, 2, 5, 3), placement.ownerByPartition());

        client.produce("orders", 4, "p4-msg1".getBytes(StandardCharsets.UTF_8));
        ListOffsetsResponse latest = client.listOffsets("orders", 4, ListOffsetsRequest.OffsetSpec.LATEST);
        FetchResponse fetch = client.fetch("orders", 4, 1, 10, 0);

        assertEquals(2, client.ownerBrokerId("orders", 4));
        assertEquals(1L, latest.offset());
        assertEquals("p4-msg1", new String(fetch.records().get(0).payload, StandardCharsets.UTF_8));
    }
}
