package com.krafta.api;

public record ListOffsetsRequest(String topic, int partition, OffsetSpec spec) {
    public enum OffsetSpec {
        EARLIEST,
        LATEST
    }
}
