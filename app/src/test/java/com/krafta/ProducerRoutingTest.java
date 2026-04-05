package com.krafta;

import com.krafta.api.FetchRequest;
import com.krafta.api.FetchResponse;
import com.krafta.broker.Broker;
import com.krafta.producer.Producer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerRoutingTest {

    @Test
    void producerRoutingRespectsExplicitRoundRobinAndKeyBasedRules(@TempDir Path tempDir) throws Exception {
        Broker broker = new Broker(tempDir.toString());
        broker.createTopic("orders", 3);

        Producer producer = new Producer(broker);
        producer.send("orders", 2, "explicit-msg");
        producer.send("orders", "rr-msg-1");
        producer.send("orders", "rr-msg-2");
        producer.send("orders", "user-42", "key-msg-1");
        producer.send("orders", "user-42", "key-msg-2");

        FetchResponse partition0 = broker.fetch(new FetchRequest("orders", 0, 1, 10, 0));
        FetchResponse partition1 = broker.fetch(new FetchRequest("orders", 1, 1, 10, 0));
        FetchResponse partition2 = broker.fetch(new FetchRequest("orders", 2, 1, 10, 0));

        List<String> p0Values = toPayloads(partition0);
        List<String> p1Values = toPayloads(partition1);
        List<String> p2Values = toPayloads(partition2);

        assertEquals(List.of("rr-msg-1"), p0Values);
        assertEquals(List.of("rr-msg-2"), p1Values);
        assertTrue(p2Values.contains("explicit-msg"));
        assertTrue(p2Values.contains("key-msg-1"));
        assertTrue(p2Values.contains("key-msg-2"));
        assertEquals(3, p2Values.size());
    }

    private List<String> toPayloads(FetchResponse response) {
        return response.records().stream()
                .map(record -> new String(record.payload, StandardCharsets.UTF_8))
                .toList();
    }
}
