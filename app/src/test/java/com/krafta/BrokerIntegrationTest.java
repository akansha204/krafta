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

import static org.junit.jupiter.api.Assertions.assertEquals;

class BrokerIntegrationTest {

    @Test
    void produceAndFetchSingleRecord(@TempDir Path tempDir) throws Exception {
        Broker broker = new Broker(tempDir.toString());
        broker.createTopic("orders", 1);

        Producer producer = new Producer(broker);
        producer.send("orders", 0, "hello");

        FetchResponse fetch = broker.fetch(new FetchRequest("orders", 0, 1, 10, 0));
        ListOffsetsResponse latest = broker.listOffsets(
                new ListOffsetsRequest("orders", 0, ListOffsetsRequest.OffsetSpec.LATEST)
        );

        assertEquals(1, fetch.records().size());
        assertEquals("hello", new String(fetch.records().get(0).payload, StandardCharsets.UTF_8));
        assertEquals(1L, fetch.records().get(0).offset);
        assertEquals(1L, latest.offset());
    }
}
