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

    //round-robin
    public ProduceResponse send(String topic, String message) throws Exception {
        int partition = broker.selectNextPartition(topic);
        return send(topic, partition, message);
    }

    //explicit send caller picks up partition
    public ProduceResponse send(String topic, int partition, String message) throws Exception {
        return broker.produce(new ProduceRequest(
                topic,
                partition,
                List.of(message.getBytes(StandardCharsets.UTF_8))
        ));
    }

    //key based send
    public ProduceResponse send(String topic, String key, String message) throws Exception {
        int partition = broker.selectPartitionForKey(topic, key);
        return send(topic, partition, message);
    }
    // explicit batch
    public ProduceResponse sendBatch(String topic, List<String> messages) throws Exception {
        int partition = broker.selectNextPartition(topic);
        return sendBatch(topic, partition, messages);
    }
    // round robin batch
    public ProduceResponse sendBatch(String topic, int partition, List<String> messages) throws Exception {
        List<byte[]> payloads = messages.stream()
                .map(message -> message.getBytes(StandardCharsets.UTF_8))
                .toList();

        return broker.produce(new ProduceRequest(topic, partition, payloads));
    }
}
