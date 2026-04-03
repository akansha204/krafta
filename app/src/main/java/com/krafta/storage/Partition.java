package com.krafta.storage;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Partition {

    private final LogSegment segment;
    private final Object monitor = new Object();

    public Partition(String path) throws IOException {
        File dir = new File(path);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Failed to create partition directory: " + path);
        }
        this.segment = new LogSegment(path + "/segment.log");
    }

    public long append(RecordBatch batch) throws IOException {
        long lastOffset = segment.appendBatch(batch);
        synchronized (monitor) {
            monitor.notifyAll();
        }
        return lastOffset;
    }

    //appends single record
    public long append(String message) throws IOException {
        Record record = new Record(-1, System.currentTimeMillis(), message.getBytes(StandardCharsets.UTF_8));
        RecordBatch batch = new RecordBatch(-1, List.of(record));
        return append(batch);
    }

    public Record read(long offset) throws IOException {
        return segment.readByOffset(offset);
    }

    public List<Record> readFrom(long offset, int maxRecords) throws IOException {
        return segment.readFromOffset(offset, maxRecords);
    }

    public long latestOffset() {
        return segment.getLatestOffset();
    }

    public long earliestOffset() throws IOException {
        return segment.getEarliestOffset();
    }

    public List<Record> fetch(long offset, int maxRecords, long maxWaitMs) throws IOException, InterruptedException {
        List<Record> records = readFrom(offset, maxRecords);
        if (!records.isEmpty() || maxWaitMs <= 0) {
            return records;
        }

        long deadline = System.currentTimeMillis() + maxWaitMs;
        synchronized (monitor) {
            while (latestOffset() < offset) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    break;
                }
                monitor.wait(remaining);
            }
        }

        return readFrom(offset, maxRecords);
    }
}
