package com.krafta.producer;

import com.krafta.api.ProduceRequest;
import com.krafta.api.ProduceResponse;
import com.krafta.broker.Broker;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class Producer {
    private final Broker broker;

    public Producer(Broker broker) {
        this.broker = broker;
    }

    public ProduceResponse send(String topic, int partition, String message) throws Exception {
        return broker.produce(new ProduceRequest(
                topic,
                partition,
                List.of(message.getBytes(StandardCharsets.UTF_8))
        ));
    }

    public ProduceResponse sendBatch(String topic, int partition, List<String> messages) throws Exception {
        List<byte[]> payloads = messages.stream()
                .map(message -> message.getBytes(StandardCharsets.UTF_8))
                .toList();

        return broker.produce(new ProduceRequest(topic, partition, payloads));
    }
}
