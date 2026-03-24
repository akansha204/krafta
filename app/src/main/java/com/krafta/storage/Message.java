package com.krafta.storage;

public class Message {
    public byte[] payload;
    public long offset;
    public long timestamp;

    public Message(long offset, long timestamp, byte[] payload){
        this.offset = offset;
        this.timestamp = timestamp;
        this.payload = payload;
    }

}
