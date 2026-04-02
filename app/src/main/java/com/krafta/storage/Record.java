package com.krafta.storage;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Record {
    public byte[] payload;
    public long offset;
    public long timestamp;

    public Record(long offset, long timestamp, byte[] payload){
        this.offset = offset;
        this.timestamp = timestamp;
        this.payload = payload;
    }
    public byte[] toBytes(){
        ByteArrayOutputStream boas = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(boas);
        try{
            dos.writeLong(offset);
            dos.writeLong(timestamp);
            dos.writeLong(payload.length);
            dos.write(payload);
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
        return boas.toByteArray();
    }

}
