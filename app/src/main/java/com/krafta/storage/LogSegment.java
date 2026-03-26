package com.krafta.storage;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class LogSegment {
    private RandomAccessFile file;
    private long currOffset;
    public LogSegment(String path) throws IOException {
        this.file = new RandomAccessFile(path, "rw");
        this.currOffset = 0;
    }
    public Map<Long,Long> offsetToFilePos = new HashMap<>();
    public synchronized long append(String message) throws IOException {
        long newOffset = currOffset + 1;
        currOffset = newOffset;

        Message msg = new Message(
                newOffset,
                System.currentTimeMillis(),
                message.getBytes()
        );

        byte[] data = serialize(msg);

        long fileOffset = file.length();
        offsetToFilePos.put(newOffset,fileOffset);
        file.seek(fileOffset);
        file.write(data);

        return newOffset;
    }

    public Message read(long fileOffset) throws IOException {
        file.seek(fileOffset);

        int length = file.readInt();

        byte[] buffer = new byte[4 + length];
        file.seek(fileOffset);
        file.readFully(buffer);

        return deserialize(buffer);
    }
    public Message readByOffset(long offset) throws IOException{
        Long filePos = offsetToFilePos.get(offset);
        if(filePos==null){
            throw new RuntimeException("Offset not found");
        }
        return read(filePos);
    }

    private byte[] serialize(Message msg) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(); //writes to in-memory byte[] buffer
        DataOutputStream dos = new DataOutputStream(baos);

        int totalLength = 8 + 8 + msg.payload.length;

        dos.writeInt(totalLength);
        dos.writeLong(msg.offset);
        dos.writeLong(msg.timestamp);
        dos.write(msg.payload);

        return baos.toByteArray();
    }

    private Message deserialize(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data); //reads from in-memory byte[] buffer
        DataInputStream dis = new DataInputStream(bais);

        int length = dis.readInt();
        long offset = dis.readLong();
        long timestamp = dis.readLong();

        byte[] payload = new byte[length - 16];
        dis.readFully(payload);

        return new Message(offset, timestamp, payload);
    }

}
