package com.krafta.storage;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.binarySearch;

public class LogSegment {
    private static final int INDEX_INTERVAL = 100;
    private int messageSinceLastIdx = 0;
    private RandomAccessFile idxfile;
    private List<Long> indexedOffsets = new ArrayList<>();
    private List<Long> indexedFilePositions = new ArrayList<>();

    private RandomAccessFile file;
    private long currOffset;
    public LogSegment(String path) throws IOException {
        this.file = new RandomAccessFile(path, "rw");
        this.idxfile = new RandomAccessFile(path.replace(".log", ".idx"), "rw");
        this.currOffset = 0;
        try {
            loadIndex(); //will load the sparse index file to memory after a restart
            if(!indexedOffsets.isEmpty()){
                long lastidxoffset = indexedOffsets.get(indexedOffsets.size()-1);
                messageSinceLastIdx = (int) ((currOffset-lastidxoffset) % INDEX_INTERVAL);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
//    public Map<Long,Long> offsetToFilePos = new HashMap<>();
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
        messageSinceLastIdx++;
        if(newOffset == 1 || messageSinceLastIdx >= INDEX_INTERVAL){
            writeIdxEntry(newOffset, fileOffset);
            messageSinceLastIdx = 0;
        }
//        offsetToFilePos.put(newOffset,fileOffset);

        return newOffset;
    }

    private void writeIdxEntry(long offset, long filepos) throws IOException{
        idxfile.seek((idxfile.length()));
        idxfile.writeLong(offset);
        idxfile.writeLong(filepos);
    }
    private void loadIndex() throws Exception{
        idxfile.seek(0);
        while(idxfile.getFilePointer() < idxfile.length()) {
            long offset = idxfile.readLong();
            long filepos = idxfile.readLong();
            indexedOffsets.add(offset);
            indexedFilePositions.add(filepos);
        }
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
        int idx = binarySearch(indexedOffsets, offset);
        if(idx<0){
            idx = -idx - 2; //find the largest offset smaller than the requested offset
        }
        long startPos;
        if(idx < 0){
            startPos = 0;
        } else{
            startPos = indexedFilePositions.get(idx);
        }
        file.seek(startPos);

       while(file.getFilePointer() < file.length()){
           long currPos = file.getFilePointer();
           Message msg = read(currPos);
           if(msg.offset == offset) return msg;
           if(msg.offset > offset) break;
           file.seek(currPos + 4 + 8 + 8 + msg.payload.length);
       }
       throw new RuntimeException("Offset not found");
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
