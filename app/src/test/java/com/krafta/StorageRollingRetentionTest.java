package com.krafta;

import com.krafta.storage.Partition;
import com.krafta.storage.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StorageRollingRetentionTest {

    @Test
    void fetchReadsAcrossRolledSegments(@TempDir Path tempDir) throws Exception {
        Partition partition = new Partition(
                tempDir.resolve("partition-0").toString(),
                1,
                24L * 60 * 60 * 1000,
                7L * 24 * 60 * 60 * 1000
        );

        partition.append("a");
        partition.append("b");
        partition.append("c");

        List<String> values = partition.readFrom(1, 10).stream()
                .map(record -> new String(record.payload, StandardCharsets.UTF_8))
                .toList();

        assertEquals(3, partition.segmentCount());
        assertEquals(List.of("a", "b", "c"), values);
    }

    @Test
    void retentionDeletesOnlyOldClosedSegments(@TempDir Path tempDir) throws Exception {
        Partition partition = new Partition(
                tempDir.resolve("partition-0").toString(),
                1024 * 1024,
                1,
                5
        );

        partition.append("old");
        Thread.sleep(20);
        partition.append("new");

        List<Record> records = partition.readFrom(1, 10);
        List<String> values = records.stream()
                .map(record -> new String(record.payload, StandardCharsets.UTF_8))
                .toList();

        assertEquals(1, partition.segmentCount());
        assertEquals(2L, partition.earliestOffset());
        assertEquals(List.of("new"), values);
    }
}
