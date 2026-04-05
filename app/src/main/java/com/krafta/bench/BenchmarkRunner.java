package com.krafta.bench;

import com.krafta.api.FetchRequest;
import com.krafta.api.FetchResponse;
import com.krafta.api.ListOffsetsRequest;
import com.krafta.api.ListOffsetsResponse;
import com.krafta.broker.Broker;
import com.krafta.client.ClusterClient;
import com.krafta.coord.ClusterCoordService;
import com.krafta.producer.Producer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BenchmarkRunner {
    private static final long SEVEN_DAYS_MS = 7L * 24 * 60 * 60 * 1000;
    private static final long ONE_DAY_MS = 24L * 60 * 60 * 1000;
    private static final int RUNS = 5;
    private static final int WARMUP_MESSAGES = 5_000;
    private static final int FETCH_BATCH_SIZE = 1_000;
    private static final Scenario SINGLE_BROKER_PRODUCE = new Scenario("Single Broker Produce", 50_000, payloadOfSize(128), 1, 1);
    private static final Scenario SINGLE_BROKER_FETCH = new Scenario("Single Broker Fetch", 50_000, payloadOfSize(128), 1, 1);
    private static final Scenario MULTI_BROKER_PRODUCE = new Scenario("Multi Broker Produce", 50_000, payloadOfSize(128), 3, 6);

    public static void main(String[] args) throws Exception {
        String benchmarkRoot = "../data/benchmarks";
        String suiteId = "run-" + System.currentTimeMillis();

        System.out.println("# Benchmark Report");
        System.out.println();
        printEnvironment(benchmarkRoot + "/" + suiteId);
        System.out.println();

        List<ScenarioResult> results = new ArrayList<>();
        results.add(runSingleBrokerProduceBenchmark(benchmarkRoot + "/" + suiteId + "/single-produce"));
        results.add(runSingleBrokerFetchBenchmark(benchmarkRoot + "/" + suiteId + "/single-fetch"));
        results.add(runMultiBrokerProduceBenchmark(benchmarkRoot + "/" + suiteId + "/cluster-produce"));

        printMedianSummary(results);
    }

    private static ScenarioResult runSingleBrokerProduceBenchmark(String dataRoot) throws Exception {
        Broker warmupBroker = new Broker(dataRoot + "/warmup");
        warmupBroker.createTopic("orders", 1);
        Producer warmupProducer = new Producer(warmupBroker);
        runProduceLoop(warmupProducer, "orders", 0, WARMUP_MESSAGES, SINGLE_BROKER_PRODUCE.payload);

        List<RunResult> runs = new ArrayList<>();
        for (int run = 1; run <= RUNS; run++) {
            Broker broker = new Broker(dataRoot + "/run-" + run);
            broker.createTopic("orders", 1);
            Producer producer = new Producer(broker);

            long start = System.nanoTime();
            runProduceLoop(producer, "orders", 0, SINGLE_BROKER_PRODUCE.messages, SINGLE_BROKER_PRODUCE.payload);
            long elapsedNanos = System.nanoTime() - start;

            runs.add(toRunResult(SINGLE_BROKER_PRODUCE, elapsedNanos));
        }

        ScenarioResult result = new ScenarioResult(SINGLE_BROKER_PRODUCE, runs, dataRoot);
        printScenario(result);
        return result;
    }

    private static ScenarioResult runSingleBrokerFetchBenchmark(String dataRoot) throws Exception {
        List<RunResult> runs = new ArrayList<>();
        for (int run = 1; run <= RUNS; run++) {
            Broker broker = new Broker(dataRoot + "/run-" + run);
            broker.createTopic("orders", 1);
            Producer producer = new Producer(broker);
            runProduceLoop(producer, "orders", 0, SINGLE_BROKER_FETCH.messages, SINGLE_BROKER_FETCH.payload);

            long totalFetched = 0;
            long nextOffset = 1;
            long start = System.nanoTime();
            while (true) {
                FetchResponse response = broker.fetch(new FetchRequest("orders", 0, nextOffset, FETCH_BATCH_SIZE, 0));
                if (response.records().isEmpty()) {
                    break;
                }
                totalFetched += response.records().size();
                nextOffset = response.records().get(response.records().size() - 1).offset + 1;
            }
            long elapsedNanos = System.nanoTime() - start;

            runs.add(toRunResult(new Scenario(SINGLE_BROKER_FETCH.name, (int) totalFetched, SINGLE_BROKER_FETCH.payload, 1, 1), elapsedNanos));
        }

        ScenarioResult result = new ScenarioResult(SINGLE_BROKER_FETCH, runs, dataRoot);
        printScenario(result);
        return result;
    }

    private static ScenarioResult runMultiBrokerProduceBenchmark(String dataRoot) throws Exception {
        List<RunResult> runs = new ArrayList<>();
        Map<Integer, Integer> ownerMap = null;

        for (int run = 1; run <= RUNS; run++) {
            long now = System.currentTimeMillis();
            ClusterCoordService clusterCoord = new ClusterCoordService();

            Broker broker1 = new Broker(
                    dataRoot + "/run-" + run + "/broker-1", 1024 * 1024, ONE_DAY_MS, SEVEN_DAYS_MS,
                    clusterCoord, 1, "broker-1", 5_000
            );
            Broker broker2 = new Broker(
                    dataRoot + "/run-" + run + "/broker-2", 1024 * 1024, ONE_DAY_MS, SEVEN_DAYS_MS,
                    clusterCoord, 2, "broker-2", 5_000
            );
            Broker broker3 = new Broker(
                    dataRoot + "/run-" + run + "/broker-3", 1024 * 1024, ONE_DAY_MS, SEVEN_DAYS_MS,
                    clusterCoord, 3, "broker-3", 5_000
            );

            broker1.heartbeatCluster(now);
            broker2.heartbeatCluster(now);
            broker3.heartbeatCluster(now);

            broker1.createTopicInCluster("orders", MULTI_BROKER_PRODUCE.partitions);
            broker2.syncTopicFromCluster("orders");
            broker3.syncTopicFromCluster("orders");

            Map<Integer, Broker> brokerMap = new HashMap<>();
            brokerMap.put(1, broker1);
            brokerMap.put(2, broker2);
            brokerMap.put(3, broker3);
            ClusterClient client = new ClusterClient(clusterCoord, brokerMap);

            long start = System.nanoTime();
            for (int i = 0; i < MULTI_BROKER_PRODUCE.messages; i++) {
                int partition = i % MULTI_BROKER_PRODUCE.partitions;
                client.produce("orders", partition, MULTI_BROKER_PRODUCE.payload);
            }
            long elapsedNanos = System.nanoTime() - start;
            runs.add(toRunResult(MULTI_BROKER_PRODUCE, elapsedNanos));

            if (ownerMap == null) {
                ownerMap = new HashMap<>();
                for (int partition = 0; partition < MULTI_BROKER_PRODUCE.partitions; partition++) {
                    ownerMap.put(partition, client.ownerBrokerId("orders", partition));
                }
            }
        }

        ScenarioResult result = new ScenarioResult(MULTI_BROKER_PRODUCE, runs, dataRoot);
        printScenario(result);
        System.out.println("owner-metadata=" + ownerMap);
        return result;
    }

    private static void runProduceLoop(Producer producer, String topic, int partition, int count, byte[] payloadBytes) throws Exception {
        String payload = new String(payloadBytes, StandardCharsets.UTF_8);
        for (int i = 0; i < count; i++) {
            producer.send(topic, partition, payload);
        }
    }

    private static void printEnvironment(String benchmarkRoot) {
        File root = new File(benchmarkRoot);
        root.mkdirs();
        System.out.println("## Environment");
        System.out.println("- OS: " + System.getProperty("os.name"));
        System.out.println("- CPUs: " + Runtime.getRuntime().availableProcessors());
        System.out.println("- Max Heap MB: " + (Runtime.getRuntime().maxMemory() / (1024 * 1024)));
        System.out.println("- Runtime: in-process harness");
        System.out.println("- Runs per scenario: " + RUNS);
        System.out.println("- Benchmark data root: " + root.getPath());
    }

    private static void printScenario(ScenarioResult result) {
        System.out.println();
        System.out.println("## " + result.scenario.name);
        System.out.println("| Run | Messages | Payload Bytes | Brokers | Partitions | Elapsed ms | Msgs/sec | MB/sec | Avg latency us |");
        System.out.println("|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
        for (int i = 0; i < result.runs.size(); i++) {
            RunResult run = result.runs.get(i);
            System.out.printf(
                    "| %d | %d | %d | %d | %d | %.2f | %.2f | %.2f | %.2f |%n",
                    i + 1,
                    result.scenario.messages,
                    result.scenario.payload.length,
                    result.scenario.brokers,
                    result.scenario.partitions,
                    run.elapsedMs,
                    run.msgsPerSec,
                    run.mbPerSec,
                    run.avgLatencyMicros
            );
        }
        System.out.println("- data path: " + result.dataPath);
    }

    private static void printMedianSummary(List<ScenarioResult> results) {
        System.out.println();
        System.out.println("## Median Summary");
        System.out.println("| Scenario | Brokers | Partitions | Messages | Payload Bytes | Median Elapsed ms | Median Msgs/sec | Median MB/sec | Median Avg latency us |");
        System.out.println("|---|---:|---:|---:|---:|---:|---:|---:|---:|");
        for (ScenarioResult result : results) {
            RunResult median = medianRun(result.runs);
            System.out.printf(
                    "| %s | %d | %d | %d | %d | %.2f | %.2f | %.2f | %.2f |%n",
                    result.scenario.name,
                    result.scenario.brokers,
                    result.scenario.partitions,
                    result.scenario.messages,
                    result.scenario.payload.length,
                    median.elapsedMs,
                    median.msgsPerSec,
                    median.mbPerSec,
                    median.avgLatencyMicros
            );
        }
    }

    private static RunResult toRunResult(Scenario scenario, long elapsedNanos) {
        double seconds = elapsedNanos / 1_000_000_000.0;
        double totalBytes = (double) scenario.messages * scenario.payload.length;
        double msgsPerSec = scenario.messages / seconds;
        double mbPerSec = (totalBytes / (1024.0 * 1024.0)) / seconds;
        double avgLatencyMicros = (elapsedNanos / 1_000.0) / scenario.messages;
        return new RunResult(elapsedNanos / 1_000_000.0, msgsPerSec, mbPerSec, avgLatencyMicros);
    }

    private static RunResult medianRun(List<RunResult> runs) {
        List<RunResult> sorted = new ArrayList<>(runs);
        sorted.sort(Comparator.comparingDouble(run -> run.msgsPerSec));
        return sorted.get(sorted.size() / 2);
    }

    private static byte[] payloadOfSize(int size) {
        byte[] payload = new byte[size];
        byte[] seed = "benchmark".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < size; i++) {
            payload[i] = seed[i % seed.length];
        }
        return payload;
    }

    private record Scenario(String name, int messages, byte[] payload, int brokers, int partitions) {
    }

    private record RunResult(double elapsedMs, double msgsPerSec, double mbPerSec, double avgLatencyMicros) {
    }

    private record ScenarioResult(Scenario scenario, List<RunResult> runs, String dataPath) {
    }
}
