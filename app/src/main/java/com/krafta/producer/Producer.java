package com.krafta.producer;

import com.krafta.storage.Partition;

import java.io.IOException;

public class Producer {
    private Partition partition;
    public Producer(Partition partition){
        this.partition = partition;
    }
    public long send(String message) throws IOException {
        return partition.append(message);
    }
}
