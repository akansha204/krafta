package com.krafta.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.binarySearch;

public class LogSegment {
    private static final int INDEX_INTERVAL = 100;
    private int messageSinceLastIdx = 0;
    private final long baseOffset;
    private final File logFile;
    private final File indexFile;
    private final long createdAtMs;
    private final RandomAccessFile idxfile;
    private final List<Long> indexedOffsets = new ArrayList<>();
    private final List<Long> indexedFilePositions = new ArrayList<>();

    private final RandomAccessFile file;
    private long currOffset;

    public LogSegment(String path) throws IOException {
        this(new File(path));
    }

    public LogSegment(File logFile) throws IOException {
        this.logFile = logFile;
        this.indexFile = new File(toIndexPath(logFile));
        this.baseOffset = parseBaseOffset(logFile.getName());
        this.file = new RandomAccessFile(logFile, "rw");
        this.idxfile = new RandomAccessFile(indexFile, "rw");
        this.currOffset = 0;
        this.createdAtMs = resolveCreatedAtMs();
        recover();
    }

    public synchronized long appendBatch(RecordBatch incomingBatch) throws IOException {
        List<Record> records = new ArrayList<>(incomingBatch.getRecords().size());
        long nextOffset = currOffset + 1;
        for (Record record : incomingBatch.getRecords()) {
            record.offset = nextOffset++;
            if (record.timestamp <= 0) {
                record.timestamp = System.currentTimeMillis();
            }
            records.add(record);
        }

        RecordBatch batch = new RecordBatch(currOffset + 1, records);
        long newOffset = currOffset + batch.getRecords().size();

        byte[] data = serializeBatch(batch);
        long fileOffset = file.length();
        file.seek(fileOffset);
        file.write(data);

        currOffset = newOffset;
        messageSinceLastIdx += batch.getRecords().size();

        if (currOffset == batch.getRecords().size() || messageSinceLastIdx >= INDEX_INTERVAL) {
            writeIdxEntry(currOffset, fileOffset);
            messageSinceLastIdx = 0;
        }

        return newOffset;
    }

    private void writeIdxEntry(long offset, long filepos) throws IOException {
        idxfile.seek(idxfile.length());
        idxfile.writeLong(offset);
        idxfile.writeLong(filepos);
        indexedOffsets.add(offset);
        indexedFilePositions.add(filepos);
    }

    public synchronized Record read(long fileOffset) throws IOException {
        file.seek(fileOffset);

        int length = file.readInt();

        byte[] buffer = new byte[4 + length];
        file.seek(fileOffset);
        file.readFully(buffer);

        return deserialize(buffer);
    }

    public synchronized Record readByOffset(long offset) throws IOException {
        int idx = binarySearch(indexedOffsets, offset);
        if (idx < 0) {
            idx = -idx - 2;
        }

        long startPos = idx < 0 ? 0 : indexedFilePositions.get(idx);
        file.seek(startPos);

        while (file.getFilePointer() < file.length()) {
            long currPos = file.getFilePointer();
            RecordBatch batch = deserializeBatch(currPos);
            for (Record record : batch.getRecords()) {
                if (record.offset == offset) {
                    return record;
                }
                if (record.offset > offset) {
                    break;
                }
            }
            file.seek(currPos + calculateBatchSize(batch));
        }

        throw new RuntimeException("Offset not found");
    }

    public synchronized List<Record> readFromOffset(long offset, int maxRecords) throws IOException {
        List<Record> result = new ArrayList<>();
        if (offset <= 0 || maxRecords <= 0) {
            return result;
        }

        int idx = binarySearch(indexedOffsets, offset);
        if (idx < 0) {
            idx = -idx - 2;
        }

        long startPos = idx < 0 ? 0 : indexedFilePositions.get(idx);
        file.seek(startPos);

        while (file.getFilePointer() < file.length() && result.size() < maxRecords) {
            long currPos = file.getFilePointer();
            RecordBatch batch = deserializeBatch(currPos);

            for (Record record : batch.getRecords()) {
                if (record.offset >= offset) {
                    result.add(record);
                    if (result.size() == maxRecords) {
                        break;
                    }
                }
            }

            file.seek(currPos + calculateBatchSize(batch));
        }

        return result;
    }

    private void recover() throws IOException {
        indexedOffsets.clear();
        indexedFilePositions.clear();
        idxfile.setLength(0);

        long scanPos = 0;
        long recoveredOffset = 0;
        int recoveredMessagesSinceLastIndex = 0;

        while (scanPos < file.length()) {
            try {
                RecordBatch batch = deserializeBatch(scanPos);
                long batchSize = calculateBatchSize(batch);
                long batchEndOffset = batch.getBaseOffset() + batch.getRecords().size() - 1;

                recoveredOffset = Math.max(recoveredOffset, batchEndOffset);
                recoveredMessagesSinceLastIndex += batch.getRecords().size();

                if (batchEndOffset == batch.getRecords().size() || recoveredMessagesSinceLastIndex >= INDEX_INTERVAL) {
                    writeIdxEntry(batchEndOffset, scanPos);
                    recoveredMessagesSinceLastIndex = 0;
                }

                scanPos += batchSize;
            } catch (IOException e) {
                file.setLength(scanPos);
                break;
            }
        }

        currOffset = recoveredOffset;
        messageSinceLastIdx = recoveredMessagesSinceLastIndex;
    }

    public synchronized long getLatestOffset() {
        return currOffset;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public synchronized long getEarliestOffset() throws IOException {
        if (currOffset == 0 || file.length() == 0) {
            return 0;
        }

        RecordBatch firstBatch = deserializeBatch(0);
        return firstBatch.getBaseOffset();
    }

    public synchronized long sizeBytes() throws IOException {
        return file.length();
    }

    public long createdAtMs() {
        return createdAtMs;
    }

    public long lastModifiedMs() {
        return logFile.lastModified();
    }

    public synchronized boolean shouldRollBySize(long maxSegmentBytes) throws IOException {
        return maxSegmentBytes > 0 && file.length() >= maxSegmentBytes;
    }

    public boolean shouldRollByAge(long maxSegmentAgeMs, long nowMs) {
        return maxSegmentAgeMs > 0 && nowMs - createdAtMs >= maxSegmentAgeMs;
    }

    public boolean isExpiredForRetention(long retentionMs, long nowMs) {
        return retentionMs > 0 && nowMs - lastModifiedMs() >= retentionMs;
    }

    public synchronized void close() throws IOException {
        idxfile.close();
        file.close();
    }

    public synchronized void delete() throws IOException {
        close();

        if (logFile.exists() && !logFile.delete()) {
            throw new IOException("Failed to delete log file: " + logFile.getPath());
        }
        if (indexFile.exists() && !indexFile.delete()) {
            throw new IOException("Failed to delete index file: " + indexFile.getPath());
        }
    }

    public static String segmentFileName(long baseOffset, String extension) {
        return String.format("%020d.%s", baseOffset, extension);
    }

    public static File logFile(File directory, long baseOffset) {
        return new File(directory, segmentFileName(baseOffset, "log"));
    }

    public static File indexFile(File directory, long baseOffset) {
        return new File(directory, segmentFileName(baseOffset, "idx"));
    }

    private long calculateBatchSize(RecordBatch batch) {
        long size = 8 + 4 + 8;
        for (Record record : batch.getRecords()) {
            size += 4 + 8 + 8 + record.payload.length;
        }
        return size;
    }

    private byte[] serialize(Record msg) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        int totalLength = 8 + 8 + msg.payload.length;

        dos.writeInt(totalLength);
        dos.writeLong(msg.offset);
        dos.writeLong(msg.timestamp);
        dos.write(msg.payload);

        return baos.toByteArray();
    }

    private Record deserialize(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);

        int length = dis.readInt();
        long offset = dis.readLong();
        long timestamp = dis.readLong();

        byte[] payload = new byte[length - 16];
        dis.readFully(payload);

        return new Record(offset, timestamp, payload);
    }

    public byte[] serializeBatch(RecordBatch batch) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        List<Record> records = batch.getRecords();
        try {
            dos.writeLong(batch.getBaseOffset());
            dos.writeInt(records.size());

            for (Record r : records) {
                byte[] recorData = serialize(r);
                dos.write(recorData);
            }
            dos.writeLong(batch.getCrc());
            dos.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }

    public RecordBatch deserializeBatch(long fileOffset) throws IOException {
        try {
            file.seek(fileOffset);

            long baseOffset = file.readLong();
            int recordCount = file.readInt();
            if (recordCount <= 0) {
                throw new IOException("Invalid record count: " + recordCount);
            }

            List<Record> records = new ArrayList<>();
            for (int i = 0; i < recordCount; i++) {
                int length = file.readInt();
                if (length < 16) {
                    throw new IOException("Invalid record length: " + length);
                }

                byte[] buffer = new byte[4 + length];
                file.seek(file.getFilePointer() - 4);
                file.readFully(buffer);
                records.add(deserialize(buffer));
            }

            long storedCrc = file.readLong();
            RecordBatch batch = new RecordBatch(baseOffset, records);
            if (batch.getCrc() != storedCrc) {
                throw new IOException("Batch corrupted: CRC mismatch at offset " + baseOffset);
            }
            return batch;
        } catch (EOFException e) {
            throw new IOException("Partial batch at fileOffset " + fileOffset, e);
        }
    }

    private long resolveCreatedAtMs() {
        long lastModified = logFile.lastModified();
        return lastModified > 0 ? lastModified : System.currentTimeMillis();
    }

    private static long parseBaseOffset(String fileName) {
        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex <= 0) {
            return 0;
        }

        String prefix = fileName.substring(0, dotIndex);
        try {
            return Long.parseLong(prefix);
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    private static String toIndexPath(File logFile) {
        String logPath = logFile.getPath();
        if (logPath.endsWith(".log")) {
            return logPath.substring(0, logPath.length() - 4) + ".idx";
        }
        return logPath + ".idx";
    }
}
