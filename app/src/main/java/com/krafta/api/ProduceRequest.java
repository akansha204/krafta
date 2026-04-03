package com.krafta.api;

import java.util.List;

public record ProduceRequest(String topic, int partition, List<byte[]> payloads) {
}
