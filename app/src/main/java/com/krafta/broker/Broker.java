package com.krafta.broker;

import com.krafta.api.FetchRequest;
import com.krafta.api.FetchResponse;
import com.krafta.api.ListOffsetsRequest;
import com.krafta.api.ListOffsetsResponse;
import com.krafta.api.ProduceRequest;
import com.krafta.api.ProduceResponse;
import com.krafta.exceptions.TopicAlreadyExistsException;
import com.krafta.exceptions.TopicNotFoundException;
import com.krafta.storage.Partition;
import com.krafta.storage.Record;
import com.krafta.storage.RecordBatch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
        loadExistingTopics();
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

    public ProduceResponse produce(ProduceRequest request) throws IOException, TopicNotFoundException {
        Partition partition = getPartition(request.topic(), request.partition());

        List<Record> records = new ArrayList<>();
        for (byte[] payload : request.payloads()) {
            records.add(new Record(-1, System.currentTimeMillis(), payload));
        }

        long baseOffset = partition.latestOffset() + 1;
        long lastOffset = partition.append(new RecordBatch(-1, records));
        return new ProduceResponse(baseOffset, lastOffset);
    }

    public FetchResponse fetch(FetchRequest request) throws IOException, InterruptedException, TopicNotFoundException {
        Partition partition = getPartition(request.topic(), request.partition());
        List<Record> records = partition.fetch(request.offset(), request.maxRecords(), request.maxWaitMs());
        return new FetchResponse(records, partition.latestOffset());
    }

    public ListOffsetsResponse listOffsets(ListOffsetsRequest request) throws IOException, TopicNotFoundException {
        Partition partition = getPartition(request.topic(), request.partition());
        long offset = switch (request.spec()) {
            case EARLIEST -> partition.earliestOffset();
            case LATEST -> partition.latestOffset();
        };
        return new ListOffsetsResponse(offset);
    }

    public List<Partition> getPartitions(String topic) throws TopicNotFoundException {
        if (!topics.containsKey(topic)) {
            throw new TopicNotFoundException("Topic not found");
        }
        return topics.get(topic);
    }

    private Partition getPartition(String topic, int partition) throws TopicNotFoundException {
        List<Partition> partitions = getPartitions(topic);
        if (partition < 0 || partition >= partitions.size()) {
            throw new IllegalArgumentException("Invalid partition: " + partition);
        }
        return partitions.get(partition);
    }

    private void loadExistingTopics() {
        File root = new File(dataRoot);
        if (!root.exists() || !root.isDirectory()) {
            return;
        }

        File[] topicDirs = root.listFiles(File::isDirectory);
        if (topicDirs == null) {
            return;
        }

        Arrays.sort(topicDirs, Comparator.comparing(File::getName));
        for (File topicDir : topicDirs) {
            File[] partitionDirs = topicDir.listFiles(File::isDirectory);
            if (partitionDirs == null) {
                continue;
            }

            Arrays.sort(partitionDirs, Comparator.comparing(File::getName));
            List<Partition> partitions = new ArrayList<>();
            for (File partitionDir : partitionDirs) {
                try {
                    partitions.add(new Partition(partitionDir.getPath()));
                } catch (IOException e) {
                    throw new RuntimeException("Failed to load partition: " + partitionDir.getPath(), e);
                }
            }

            if (!partitions.isEmpty()) {
                topics.put(topicDir.getName(), partitions);
                nextPartitionIndex.put(topicDir.getName(), 0);
            }
        }
    }
}
