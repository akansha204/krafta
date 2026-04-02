package com.krafta.storage;

import java.util.List;
import java.util.zip.CRC32;

public class RecordBatch {
    private long baseOffset;
    private List<Record> recordList;
    private long crc;

    public RecordBatch(long baseOffset, List<Record> recordList){
        this.baseOffset = baseOffset;
        this.recordList = recordList;
        this.crc = calculateCrc();
    }

    private long calculateCrc(){
        CRC32 crc32 = new CRC32();
        for(Record record : recordList){
            crc32.update(record.toBytes());
        }
        return crc32.getValue();
    }
    public boolean isValid(){
        return calculateCrc() == this.crc;
    }
    public List<Record> getRecords(){
        return recordList;
    }
    public long getBaseOffset() {
        return baseOffset;
    }
    public long getCrc() {
        return crc;
    }
}
