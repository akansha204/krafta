package com.krafta.broker;

import com.krafta.api.FetchRequest;
import com.krafta.api.FetchResponse;
import com.krafta.api.ListOffsetsRequest;
import com.krafta.api.ListOffsetsResponse;
import com.krafta.api.ProduceRequest;
import com.krafta.api.ProduceResponse;
import com.krafta.coord.ClusterCoordService;
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
import java.util.Optional;

public class Broker {
    private static final long DEFAULT_MAX_SEGMENT_BYTES = 1024 * 1024;
    private static final long DEFAULT_MAX_SEGMENT_AGE_MS = 24L * 60 * 60 * 1000;
    private static final long DEFAULT_RETENTION_MS = 7L * 24 * 60 * 60 * 1000;
    private static final long DEFAULT_BROKER_SESSION_TIMEOUT_MS = 5_000;

    private final Map<String, List<Partition>> topics = new HashMap<>();
    private final Map<String, Integer> nextPartitionIndex = new HashMap<>();
    private final String dataRoot;
    private final long maxSegmentBytes;
    private final long maxSegmentAgeMs;
    private final long retentionMs;
    private final ClusterCoordService clusterCoordService;
    private final Integer brokerId;
    private final String endpoint;
    private final long brokerSessionTimeoutMs;

    public Broker() {
        this("../data");
    }

    public Broker(String dataRoot) {
        this(dataRoot, DEFAULT_MAX_SEGMENT_BYTES, DEFAULT_MAX_SEGMENT_AGE_MS, DEFAULT_RETENTION_MS);
    }

    public Broker(String dataRoot, long maxSegmentBytes, long maxSegmentAgeMs, long retentionMs) {
        this(dataRoot, maxSegmentBytes, maxSegmentAgeMs, retentionMs, null, null, null, DEFAULT_BROKER_SESSION_TIMEOUT_MS);
    }

    public Broker(
            String dataRoot,
            long maxSegmentBytes,
            long maxSegmentAgeMs,
            long retentionMs,
            ClusterCoordService clusterCoordService,
            Integer brokerId,
            String endpoint,
            long brokerSessionTimeoutMs
    ) {
        this.dataRoot = dataRoot;
        this.maxSegmentBytes = maxSegmentBytes;
        this.maxSegmentAgeMs = maxSegmentAgeMs;
        this.retentionMs = retentionMs;
        this.clusterCoordService = clusterCoordService;
        this.brokerId = brokerId;
        this.endpoint = endpoint;
        this.brokerSessionTimeoutMs = brokerSessionTimeoutMs;
        if (isClusterMode()) {
            registerToCluster(System.currentTimeMillis());
        } else {
            loadExistingTopics();
        }
    }

    public void createTopic(String topicName, int totalPartition) throws TopicAlreadyExistsException, IOException {
        if (isClusterMode()) {
            throw new IllegalStateException("Use createTopicInCluster() in cluster mode");
        }
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
            Partition currpartition = new Partition(path, maxSegmentBytes, maxSegmentAgeMs, retentionMs);
            partitionList.add(currpartition);
        }
        topics.put(topicName, partitionList);
        nextPartitionIndex.put(topicName, 0);
    }

    public void createTopicInCluster(String topicName, int totalPartition) throws IOException {
        if (!isClusterMode()) {
            throw new IllegalStateException("Cluster coordinator is not configured");
        }
        if (totalPartition <= 0) {
            throw new IllegalArgumentException("Total partition must be greater than 0");
        }

        try {
            clusterCoordService.createTopicStaticPlacement(topicName, totalPartition);
        } catch (IllegalArgumentException alreadyExists) {
            // Topic metadata may already exist if another broker created it first.
        }
        syncTopicFromCluster(topicName);
    }

    public void syncTopicFromCluster(String topicName) throws IOException {
        if (!isClusterMode()) {
            throw new IllegalStateException("Cluster coordinator is not configured");
        }
        Optional<ClusterCoordService.TopicPlacement> placementOpt = clusterCoordService.topicPlacement(topicName);
        if (placementOpt.isEmpty()) {
            throw new IllegalStateException("Unknown topic in cluster metadata: " + topicName);
        }

        ClusterCoordService.TopicPlacement placement = placementOpt.get();
        List<Partition> partitionList = new ArrayList<>(placement.partitionCount());
        for (int i = 0; i < placement.partitionCount(); i++) {
            partitionList.add(null);
        }

        File topicDir = new File(dataRoot, topicName);
        if (!topicDir.exists() && !topicDir.mkdirs()) {
            throw new IOException("Failed to create topic directory: " + topicDir.getPath());
        }

        for (Map.Entry<Integer, Integer> entry : placement.ownerByPartition().entrySet()) {
            int partitionId = entry.getKey();
            int ownerBrokerId = entry.getValue();
            if (ownerBrokerId == brokerId) {
                String path = dataRoot + "/" + topicName + "/partitions" + partitionId;
                partitionList.set(partitionId, new Partition(path, maxSegmentBytes, maxSegmentAgeMs, retentionMs));
            }
        }

        topics.put(topicName, partitionList);
        nextPartitionIndex.put(topicName, 0);
    }

    public long send(String topicName, String message) throws TopicNotFoundException, IOException {
        int idx = selectNextPartition(topicName);
        Partition partition = getPartition(topicName, idx);
        long offset = partition.append(message);
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

    public int getPartitionCount(String topic) throws TopicNotFoundException {
        return getPartitions(topic).size();
    }

    public int selectNextPartition(String topic) throws TopicNotFoundException {
        List<Partition> partitionsList = getPartitions(topic);
        int idx = nextPartitionIndex.getOrDefault(topic, 0);
        int nextIdx = (idx + 1) % partitionsList.size();
        nextPartitionIndex.put(topic, nextIdx);
        return idx;
    }

    public int selectPartitionForKey(String topic, String key) throws TopicNotFoundException {
        int partitionCount = getPartitionCount(topic);
        return Math.floorMod(key.hashCode(), partitionCount);
    }

    public int getSegmentCount(String topic, int partition) throws TopicNotFoundException {
        return getPartition(topic, partition).segmentCount();
    }

    public void registerToCluster(long nowMs) {
        if (!isClusterMode()) {
            throw new IllegalStateException("Cluster coordinator is not configured");
        }
        clusterCoordService.registerBroker(brokerId, endpoint, brokerSessionTimeoutMs, nowMs);
    }

    public void heartbeatCluster(long nowMs) {
        if (!isClusterMode()) {
            throw new IllegalStateException("Cluster coordinator is not configured");
        }
        clusterCoordService.brokerHeartbeat(brokerId, nowMs);
    }

    public ClusterCoordService.ClusterMetadata clusterMetadata() {
        if (!isClusterMode()) {
            throw new IllegalStateException("Cluster coordinator is not configured");
        }
        return clusterCoordService.metadataSnapshot();
    }

    private Partition getPartition(String topic, int partition) throws TopicNotFoundException {
        List<Partition> partitions = getPartitions(topic);
        if (partition < 0 || partition >= partitions.size()) {
            throw new IllegalArgumentException("Invalid partition: " + partition);
        }

        Partition local = partitions.get(partition);
        if (local == null) {
            if (isClusterMode()) {
                int owner = clusterCoordService.ownerBroker(topic, partition);
                throw new IllegalStateException(
                        "Partition " + partition + " of topic " + topic + " is owned by broker " + owner
                );
            }
            throw new IllegalStateException("Partition " + partition + " is not available locally");
        }
        return local;
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
                    partitions.add(new Partition(partitionDir.getPath(), maxSegmentBytes, maxSegmentAgeMs, retentionMs));
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

    private boolean isClusterMode() {
        return clusterCoordService != null && brokerId != null && endpoint != null;
    }
}
