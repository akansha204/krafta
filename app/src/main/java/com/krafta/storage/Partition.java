package com.krafta.storage;

import java.io.File;
import java.io.IOException;

public class Partition {

    private LogSegment segment;

    public Partition(String path) throws IOException {
        File dir = new File(path);
        if(!dir.exists()){
            dir.mkdirs();
        }
        this.segment = new LogSegment(path + "/segment.log");
    }

    public long append(String message) throws IOException {
        return segment.append(message);
    }
    public Message read(long offset) throws Exception{
        return segment.readByOffset(offset);
    }
}