package com.krafta.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumerGroup {
    private String groupId;
    private List<Consumer> consumersList;
    private Map<Integer, Consumer> partitionAssignment;

    public ConsumerGroup(String groupId, List<Consumer> consumersList, Map<Integer, Consumer> partitionAssignment){
        this.groupId = groupId;
        this.consumersList = consumersList;
        this.partitionAssignment = (partitionAssignment != null )? new HashMap<>(partitionAssignment) : new HashMap<>();
    }


    public void setPartitionAssignment(List<Integer> partitionList) throws Exception {
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

        for(Integer partitionId : partitionList){
            Consumer c = consumersList.get(i % consumersList.size());
            partitionAssignment.put(partitionId, c);
            c.addPartition(partitionId);
            i++;
        }
        for (Map.Entry<Integer, Consumer> entry : partitionAssignment.entrySet()) {
            System.out.println("Assigned: " + entry.getKey() + " -> Consumer " + consumersList.indexOf(entry.getValue()));
        }

    }
}
