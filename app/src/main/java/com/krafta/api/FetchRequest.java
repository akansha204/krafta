package com.krafta.api;

public record FetchRequest(String topic, int partition, long offset, int maxRecords, long maxWaitMs) {
}
