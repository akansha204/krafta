package com.krafta.storage;

import java.io.*;

public class LogSegment {
    private RandomAccessFile file;
    private long currOffset;
    public LogSegment(String path) throws IOException {
        this.file = new RandomAccessFile(path, "rw");
        this.currOffset = 0;
    }
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
