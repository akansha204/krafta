package com.krafta.consumer;

import com.krafta.storage.Partition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumerGroup {
    private String groupId;
    private List<Consumer> consumersList;
    private Map<Partition, Consumer> partitionAssignment;

    public ConsumerGroup(String groupId, List<Consumer> consumersList, Map<Partition, Consumer> partitionAssignment){
        this.groupId = groupId;
        this.consumersList = consumersList;
        this.partitionAssignment = (partitionAssignment != null )? new HashMap<>(partitionAssignment) : new HashMap<>();
    }


    public void setPartitionAssignment(List<Partition> partitionList){
        if(consumersList == null || consumersList.isEmpty()){
            throw new IllegalStateException("No consumers in the group");
        }
        if(partitionList == null){
            partitionList = new ArrayList<>();
        }
        for(Consumer c : consumersList){
            c.clearAssignments();
        }

        partitionAssignment.clear();
        int i = 0;

        for(Partition p:partitionList){
            Consumer c = consumersList.get(i % consumersList.size());
            partitionAssignment.put(p,c);
            c.addPartition(p);
            i++;
        }
        for (Map.Entry<Partition, Consumer> entry : partitionAssignment.entrySet()) {
            System.out.println("Assigned: " + entry.getKey() + " -> Consumer " + consumersList.indexOf(entry.getValue()));
        }

    }
}
