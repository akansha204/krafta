package com.krafta.storage;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Partition {

    private final LogSegment segment;

    public Partition(String path) throws IOException {
        File dir = new File(path);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        this.segment = new LogSegment(path + "/segment.log");
    }

    public long append(RecordBatch batch) throws IOException {
        return segment.appendBatch(batch);
    }

    //appends single record
    public long append(String message) throws IOException {
        Record record = new Record(-1, System.currentTimeMillis(), message.getBytes(StandardCharsets.UTF_8));
        RecordBatch batch = new RecordBatch(-1, List.of(record));
        return append(batch);
    }

    public Record read(long offset) throws Exception {
        return segment.readByOffset(offset);
    }
}
