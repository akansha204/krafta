package com.krafta.broker;

import com.krafta.exceptions.TopicAlreadyExistsException;
import com.krafta.exceptions.TopicNotFoundException;
import com.krafta.storage.Partition;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Broker {
    private final Map<String, List<Partition>> topics = new HashMap<>();
    private final Map<String, Integer> nextPartitionIndex = new HashMap<>();
    private final String dataRoot;

    public Broker() {
        this("../data");
    }

    public Broker(String dataRoot) {
        this.dataRoot = dataRoot;
    }

    public void createTopic(String topicName, int totalPartition) throws TopicAlreadyExistsException, IOException {
        if(totalPartition<=0){
            throw new IllegalArgumentException("Total partition must be greater than 0");
        }
        if (topics.containsKey(topicName)) {
            throw new TopicAlreadyExistsException("Topic already exists");
        }
        File topicDir = new File(dataRoot ,topicName);
        if(topicDir.exists()){
            throw new TopicAlreadyExistsException("Topic already exists in file system");
        }
        if(!topicDir.mkdirs()){
            throw new IOException("Failed to create topic directory: " + topicDir.getPath());
        }
        List<Partition> partitionList = new ArrayList<>();
        for (int i = 0; i < totalPartition; i++) {
            String path = dataRoot + "/" + topicName + "/partitions" + i;
            Partition currpartition = new Partition(path);
            partitionList.add(currpartition);
        }
        topics.put(topicName, partitionList);
        nextPartitionIndex.put(topicName, 0);
    }

    public long send(String topicName, String message) throws TopicNotFoundException, IOException {
        if (!topics.containsKey(topicName)) {
            throw new TopicNotFoundException("Topic not found");
        }
        List<Partition> partitionsList = topics.get(topicName);
        int idx = nextPartitionIndex.get(topicName);

        Partition partition = partitionsList.get(idx);
        long offset = partition.append(message);

        int nextidx = (idx + 1) % partitionsList.size();
        nextPartitionIndex.put(topicName, nextidx);

        return offset;
    }

    public List<Partition> getPartitions(String topic) throws TopicNotFoundException {
        if (!topics.containsKey(topic)) {
            throw new TopicNotFoundException("Topic not found");
        }
        return topics.get(topic);
    }
}
