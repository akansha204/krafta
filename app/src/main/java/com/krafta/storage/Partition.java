package com.krafta.storage;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Partition {

    private static final long DEFAULT_MAX_SEGMENT_BYTES = 1024 * 1024;
    private static final long DEFAULT_MAX_SEGMENT_AGE_MS = 24L * 60 * 60 * 1000;
    private static final long DEFAULT_RETENTION_MS = 7L * 24 * 60 * 60 * 1000;

    private final File partitionDir;
    private final long maxSegmentBytes;
    private final long maxSegmentAgeMs;
    private final long retentionMs;
    private final List<LogSegment> segments = new ArrayList<>();
    private LogSegment activeSegment;
    private final Object monitor = new Object();

    public Partition(String path) throws IOException {
        this(path, DEFAULT_MAX_SEGMENT_BYTES, DEFAULT_MAX_SEGMENT_AGE_MS, DEFAULT_RETENTION_MS);
    }

    public Partition(String path, long maxSegmentBytes, long maxSegmentAgeMs, long retentionMs) throws IOException {
        File dir = new File(path);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Failed to create partition directory: " + path);
        }
        this.partitionDir = dir;
        this.maxSegmentBytes = maxSegmentBytes;
        this.maxSegmentAgeMs = maxSegmentAgeMs;
        this.retentionMs = retentionMs;
        loadSegments();
    }

    public long append(RecordBatch batch) throws IOException {
        maybeRollSegment();
        long lastOffset = activeSegment.appendBatch(batch);
        applyRetention(System.currentTimeMillis());
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
        if (offset <= 0) {
            throw new IOException("Invalid offset: " + offset);
        }

        for (LogSegment segment : segments) {
            if (segment.getLatestOffset() == 0 || segment.getLatestOffset() < offset) {
                continue;
            }
            try {
                return segment.readByOffset(offset);
            } catch (RuntimeException ignored) {
            }
        }
        throw new IOException("Offset not found: " + offset);
    }

    public List<Record> readFrom(long offset, int maxRecords) throws IOException {
        List<Record> result = new ArrayList<>();
        if (offset <= 0 || maxRecords <= 0) {
            return result;
        }

        long nextOffset = offset;
        for (LogSegment segment : segments) {
            if (result.size() >= maxRecords) {
                break;
            }

            long latest = segment.getLatestOffset();
            if (latest == 0 || latest < nextOffset) {
                continue;
            }

            long startOffset = Math.max(nextOffset, segment.getEarliestOffset());
            List<Record> fromSegment = segment.readFromOffset(startOffset, maxRecords - result.size());
            result.addAll(fromSegment);

            if (!fromSegment.isEmpty()) {
                nextOffset = fromSegment.get(fromSegment.size() - 1).offset + 1;
            }
        }
        return result;
    }

    public long latestOffset() {
        for (int i = segments.size() - 1; i >= 0; i--) {
            long latest = segments.get(i).getLatestOffset();
            long earliestFloor = segments.get(i).getBaseOffset() > 0 ? segments.get(i).getBaseOffset() - 1 : 0;
            if (latest > earliestFloor) {
                return latest;
            }
        }
        return 0;
    }

    public long earliestOffset() throws IOException {
        for (LogSegment segment : segments) {
            long earliest = segment.getEarliestOffset();
            if (earliest > 0) {
                return earliest;
            }
        }
        return 0;
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

    public int segmentCount() {
        return segments.size();
    }

    private void loadSegments() throws IOException {
        File[] logFiles = partitionDir.listFiles(file -> file.isFile() && file.getName().endsWith(".log"));
        if (logFiles == null || logFiles.length == 0) {
            LogSegment initial = new LogSegment(LogSegment.logFile(partitionDir, 0));
            segments.add(initial);
            activeSegment = initial;
            return;
        }

        List<File> files = new ArrayList<>(List.of(logFiles));
        files.sort(
                Comparator
                        .comparingLong((File file) -> baseOffsetFromFileName(file.getName()))
                        .thenComparing(File::getName)
        );
        for (File file : files) {
            segments.add(new LogSegment(file));
        }

        activeSegment = segments.get(segments.size() - 1);
        applyRetention(System.currentTimeMillis());
    }

    private void maybeRollSegment() throws IOException {
        if (activeSegment.isEmpty()) {
            return;
        }

        long nowMs = System.currentTimeMillis();
        boolean rollBySize = activeSegment.shouldRollBySize(maxSegmentBytes);
        boolean rollByAge = activeSegment.shouldRollByAge(maxSegmentAgeMs, nowMs);
        if (!rollBySize && !rollByAge) {
            return;
        }

        long nextBaseOffset = latestOffset() + 1;
        LogSegment nextSegment = new LogSegment(LogSegment.logFile(partitionDir, nextBaseOffset));
        segments.add(nextSegment);
        activeSegment = nextSegment;
    }

    private void applyRetention(long nowMs) throws IOException {
        if (retentionMs <= 0) {
            return;
        }

        while (segments.size() > 1) {
            LogSegment oldest = segments.get(0);
            if (!oldest.isExpiredForRetention(retentionMs, nowMs)) {
                break;
            }
            oldest.delete();
            segments.remove(0);
        }
    }

    private long baseOffsetFromFileName(String fileName) {
        if ("segment.log".equals(fileName)) {
            return 0;
        }

        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex <= 0) {
            return Long.MAX_VALUE;
        }

        try {
            return Long.parseLong(fileName.substring(0, dotIndex));
        } catch (NumberFormatException ignored) {
            return Long.MAX_VALUE;
        }
    }
}
