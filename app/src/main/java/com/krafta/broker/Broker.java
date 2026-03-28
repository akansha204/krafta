package com.krafta.broker;

import com.krafta.exceptions.TopicAlreadyExistsException;
import com.krafta.exceptions.TopicNotFoundException;
import com.krafta.storage.Message;
import com.krafta.storage.Partition;

import java.io.IOException;
import java.util.*;

public class Broker {
    private Map<String, List<Partition>> topics = new HashMap<>();
    private Map<String, Integer> nextPartitionIndex = new HashMap<>();

    public void createTopic(String topicName, int totalPartition) throws TopicAlreadyExistsException, IOException {
        if (topics.containsKey(topicName)) {
            throw new TopicAlreadyExistsException("Topic already exists");
        }
        List<Partition> partitionList = new ArrayList<>();
        for(int i=0;i<totalPartition;i++){
            String path = "../data/" + topicName + "/partitions" + i;
            Partition currpartition = new Partition(path);
            partitionList.add(currpartition);
        }
        topics.put(topicName, partitionList);
        nextPartitionIndex.put(topicName, 0);

    }
    public void send(String topicName, String message) throws TopicNotFoundException, IOException {
        if (!topics.containsKey(topicName)) {
            throw new TopicNotFoundException("Topic not found");
        }
        List<Partition> partitionsList = topics.get(topicName);
        int idx = nextPartitionIndex.get(topicName);

        Partition partition = partitionsList.get(idx);
        partition.append(message);

        int nextidx = (idx+1) % partitionsList.size();
        nextPartitionIndex.put(topicName, nextidx);

    }
    public List<Partition> getPartitions(String topic) throws TopicNotFoundException {
        if (!topics.containsKey(topic)) {
            throw new TopicNotFoundException("Topic not found");
        }
        return topics.get(topic);
    }

}
