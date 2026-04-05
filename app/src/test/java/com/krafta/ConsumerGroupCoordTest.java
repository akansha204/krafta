package com.krafta;

import com.krafta.broker.Broker;
import com.krafta.consumer.Consumer;
import com.krafta.consumer.ConsumerGroup;
import com.krafta.coord.CoordService;
import com.krafta.producer.Producer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerGroupCoordTest {

    @Test
    void twoConsumersSplitPartitionsDeterministically(@TempDir Path tempDir) throws Exception {
        Broker broker = new Broker(tempDir.toString());
        broker.createTopic("orders", 4);

        CoordService coordService = new CoordService();
        Consumer c1 = new Consumer("member-a", broker, "orders", List.of());
        Consumer c2 = new Consumer("member-b", broker, "orders", List.of());
        ConsumerGroup group = new ConsumerGroup(
                "group-1",
                "orders",
                broker,
                coordService,
                2_000,
                new ArrayList<>(),
                new HashMap<>()
        );

        long now = System.currentTimeMillis();
        group.registerConsumer(c1, now);
        CoordService.AssignmentSnapshot snapshot = group.registerConsumer(c2, now + 1);

        assertEquals(List.of(0, 1), snapshot.assignments().get("member-a"));
        assertEquals(List.of(2, 3), snapshot.assignments().get("member-b"));
    }

    @Test
    void timeoutReleaseAndCommittedOffsetResumeWork(@TempDir Path tempDir) throws Exception {
        Broker broker = new Broker(tempDir.toString());
        broker.createTopic("orders", 2);

        Producer producer = new Producer(broker);
        producer.send("orders", 0, "p0-m1");
        producer.send("orders", 0, "p0-m2");
        producer.send("orders", 1, "p1-m1");
        producer.send("orders", 1, "p1-m2");

        CoordService coordService = new CoordService();
        Consumer c1 = new Consumer("member-a", broker, "orders", List.of());
        Consumer c2 = new Consumer("member-b", broker, "orders", List.of());
        ConsumerGroup group = new ConsumerGroup(
                "group-1",
                "orders",
                broker,
                coordService,
                2_000,
                new ArrayList<>(),
                new HashMap<>()
        );

        long now = System.currentTimeMillis();
        group.registerConsumer(c1, now);
        group.registerConsumer(c2, now + 1);

        c1.poll(20);
        c2.poll(20);
        assertEquals(3L, coordService.readCommittedOffset("group-1", 0, -1));
        assertEquals(3L, coordService.readCommittedOffset("group-1", 1, -1));
        assertTrue(coordService.ownerOf("group-1", 0).isPresent());
        assertTrue(coordService.ownerOf("group-1", 1).isPresent());

        group.heartbeat("member-a", now + 500);
        CoordService.AssignmentSnapshot afterExpiry = group.expireTimedOutMembers(now + 2_300);
        assertEquals(List.of(0, 1), afterExpiry.assignments().get("member-a"));
        assertFalse(afterExpiry.assignments().containsKey("member-b"));
        assertTrue(coordService.ownerOf("group-1", 1).isEmpty());

        group.unregisterConsumer("member-a");
        Consumer restarted = new Consumer("member-a", broker, "orders", List.of());
        group.registerConsumer(restarted, now + 2_400);

        producer.send("orders", 0, "p0-m3");
        restarted.poll(20);

        assertEquals(4L, coordService.readCommittedOffset("group-1", 0, -1));
    }
}
