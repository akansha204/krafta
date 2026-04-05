package com.krafta;

import com.krafta.api.FetchRequest;
import com.krafta.api.FetchResponse;
import com.krafta.api.ListOffsetsRequest;
import com.krafta.api.ListOffsetsResponse;
import com.krafta.broker.Broker;
import com.krafta.producer.Producer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RecoveryTest {

    @Test
    void brokerRecoversDataAfterRestart(@TempDir Path tempDir) throws Exception {
        Broker firstBroker = new Broker(tempDir.toString());
        firstBroker.createTopic("orders", 1);

        Producer producer = new Producer(firstBroker);
        producer.send("orders", 0, "msg-1");
        producer.send("orders", 0, "msg-2");
        producer.send("orders", 0, "msg-3");

        Broker recoveredBroker = new Broker(tempDir.toString());
        FetchResponse fetch = recoveredBroker.fetch(new FetchRequest("orders", 0, 1, 10, 0));
        ListOffsetsResponse latest = recoveredBroker.listOffsets(
                new ListOffsetsRequest("orders", 0, ListOffsetsRequest.OffsetSpec.LATEST)
        );

        List<String> values = fetch.records().stream()
                .map(record -> new String(record.payload, StandardCharsets.UTF_8))
                .toList();

        assertEquals(List.of("msg-1", "msg-2", "msg-3"), values);
        assertEquals(3L, latest.offset());
    }
}
