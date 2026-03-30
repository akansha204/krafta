package com.krafta.consumer;

import com.krafta.storage.Message;
import com.krafta.storage.Partition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Consumer {
    private String topic;
    private List<Partition> partitionList;
    private Map<Partition,Long> partitionoffset = new HashMap<>();

    public Consumer(String topic, List<Partition> partitionList){
        this.topic = topic;
        this.partitionList = partitionList;

        for(Partition p:partitionList){
            partitionoffset.put(p,1L);
        }
    }

    public void poll(int maxMessages) throws Exception{
        for(Partition p :partitionList){
            long curroffset = partitionoffset.get(p);
            for(int i=0;i<maxMessages;i++){
                try {
                    Message msg = p.read(curroffset);
                    if (msg == null) break;

                    System.out.println("Consumed: " + "Partition" + p + "Offset" + curroffset + "->" + new String(msg.payload));
                    curroffset++;
                } catch (Exception e) {
                    break;
                }
            }
            partitionoffset.put(p,curroffset);
        }
    }
    public void addPartition(Partition p) {
        if (!partitionList.contains(p)) {
            partitionList.add(p);
            partitionoffset.put(p, 1L);
        }
    }

    public void clearAssignments() {
        partitionList.clear();
        partitionoffset.clear();
    }

}
