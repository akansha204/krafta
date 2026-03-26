package com.krafta.consumer;

import com.krafta.storage.Message;
import com.krafta.storage.Partition;

public class Consumer {
    private Partition partition;
    private long curroffset = 1;

    public Consumer(Partition partition){
        this.partition = partition;
    }

    public void poll(int maxMessages) throws Exception{
        for(int i=0;i<maxMessages;i++){
            try {
                Message msg = partition.read(curroffset);
                if (msg == null) break;

                System.out.println("Consumed: " + new String(msg.payload));
                curroffset++;
            } catch (RuntimeException e) {
                if (e.getMessage().equals("Offset not found")) {
                    System.out.println("No more messages to consume.");
                    break;
                } else {
                    throw e;
                }
            }
        }
    }
}
