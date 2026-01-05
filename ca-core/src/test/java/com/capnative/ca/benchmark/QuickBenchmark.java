package com.capnative.ca.benchmark;

import com.capnative.ca.service.BatchTransactionProcessor;
import com.capnative.ca.service.TransactionProcessor;
import com.capnative.ca.storage.AgentLedgerStorage;
import com.capnative.ca.storage.ConsolidatedLedgerStorage;
import com.capnative.common.storage.LmdbEnv;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Quick benchmark comparing legacy vs batch processor.
 * Run with: java -cp ... com.capnative.ca.benchmark.QuickBenchmark
 */
public class QuickBenchmark {

    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("QUICK BENCHMARK: Legacy vs Batch Processor");
        System.out.println("=".repeat(80));
        System.out.println();

        int numTransactions = 10000;

        // Test 1: Legacy Processor
        System.out.println("TEST 1: Legacy Single-Threaded Processor");
        System.out.println("-".repeat(80));
        Result legacyResult = benchmarkLegacy(numTransactions);
        printResult(legacyResult);

        // Test 2: Batch Processor (default config)
        System.out.println("\nTEST 2: Batch Processor (batch=100, threads=8)");
        System.out.println("-".repeat(80));
        Result batchResult = benchmarkBatch(numTransactions, 100, 8);
        printResult(batchResult);

        // Comparison
        System.out.println("\n" + "=".repeat(80));
        System.out.println("COMPARISON");
        System.out.println("=".repeat(80));
        double speedup = (double) legacyResult.durationMs / batchResult.durationMs;
        double throughputImprovement = (batchResult.throughput / legacyResult.throughput - 1) * 100;

        System.out.println("Speedup:                  " + String.format("%.2fx faster", speedup));
        System.out.println("Throughput Improvement:   " + String.format("+%.1f%%", throughputImprovement));
        System.out.println("Latency Reduction (p50):  " + String.format("%.1f%%",
                (1.0 - (double) batchResult.p50LatencyUs / legacyResult.p50LatencyUs) * 100));
        System.out.println("Memory Overhead:          " + String.format("%.2f MB",
                (batchResult.memoryMB - legacyResult.memoryMB)));
        System.out.println("=".repeat(80));
    }

    static Result benchmarkLegacy(int numTx) throws Exception {
        File dbDir = createTempDir("legacy");
        try {
            LmdbEnv env = new LmdbEnv(dbDir, 1024 * 1024 * 1024, 10);
            AgentLedgerStorage agentStorage = new AgentLedgerStorage(env);
            ConsolidatedLedgerStorage consolidatedStorage = new ConsolidatedLedgerStorage(env);

            SimpleValidator validator = new SimpleValidator();
            TransactionProcessor processor = new TransactionProcessor(
                    agentStorage, consolidatedStorage, validator);

            // Warmup
            for (int i = 0; i < 100; i++) {
                processor.processTransaction("agent", i, "tx-w-" + i, generatePayload());
            }

            // Benchmark
            long startMem = getUsedMemoryMB();
            long startTime = System.currentTimeMillis();
            List<Long> latencies = new ArrayList<>();

            for (int i = 0; i < numTx; i++) {
                long txStart = System.nanoTime();
                processor.processTransaction("agent", i, "tx-" + i, generatePayload());
                latencies.add((System.nanoTime() - txStart) / 1000); // microseconds
            }

            long durationMs = System.currentTimeMillis() - startTime;
            long endMem = getUsedMemoryMB();

            env.close();
            cleanup(dbDir);

            latencies.sort(Long::compareTo);
            return new Result(numTx, durationMs, endMem - startMem, latencies);

        } catch (Exception e) {
            cleanup(dbDir);
            throw e;
        }
    }

    static Result benchmarkBatch(int numTx, int batchSize, int threads) throws Exception {
        File dbDir = createTempDir("batch");
        try {
            LmdbEnv env = new LmdbEnv(dbDir, 1024 * 1024 * 1024, 10);
            AgentLedgerStorage agentStorage = new AgentLedgerStorage(env);
            ConsolidatedLedgerStorage consolidatedStorage = new ConsolidatedLedgerStorage(env);

            SimpleValidator validator = new SimpleValidator();
            BatchTransactionProcessor processor = new BatchTransactionProcessor(
                    agentStorage, consolidatedStorage, validator,
                    batchSize, threads, 10000, "test-ca");

            processor.start();

            // Warmup
            List<CompletableFuture<?>> warmupFutures = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                warmupFutures.add(processor.submitTransaction("agent", i, "tx-w-" + i, generatePayload()));
            }
            CompletableFuture.allOf(warmupFutures.toArray(new CompletableFuture[0])).get();

            // Benchmark
            long startMem = getUsedMemoryMB();
            long startTime = System.currentTimeMillis();
            List<Long> latencies = new ArrayList<>();
            List<CompletableFuture<?>> futures = new ArrayList<>();

            for (int i = 0; i < numTx; i++) {
                long txStart = System.nanoTime();
                CompletableFuture<TransactionProcessor.TransactionResult> future =
                        processor.submitTransaction("agent", i, "tx-" + i, generatePayload());

                future.thenAccept(result -> {
                    synchronized (latencies) {
                        latencies.add((System.nanoTime() - txStart) / 1000); // microseconds
                    }
                });

                futures.add(future);
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

            long durationMs = System.currentTimeMillis() - startTime;
            long endMem = getUsedMemoryMB();

            processor.stop();
            env.close();
            cleanup(dbDir);

            latencies.sort(Long::compareTo);
            return new Result(numTx, durationMs, endMem - startMem, latencies);

        } catch (Exception e) {
            cleanup(dbDir);
            throw e;
        }
    }

    static byte[] generatePayload() {
        return "test-payload-data-12345".getBytes(StandardCharsets.UTF_8);
    }

    static File createTempDir(String prefix) {
        File dir = new File("/tmp/ca-bench-" + prefix + "-" + System.nanoTime());
        dir.mkdirs();
        return dir;
    }

    static void cleanup(File dir) {
        if (dir.exists()) {
            deleteRecursively(dir);
        }
    }

    static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File child : files) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }

    static long getUsedMemoryMB() {
        Runtime runtime = Runtime.getRuntime();
        System.gc();
        try { Thread.sleep(100); } catch (InterruptedException e) {}
        return (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024;
    }

    static void printResult(Result r) {
        System.out.println("Transactions:      " + r.numTx);
        System.out.println("Duration:          " + r.durationMs + " ms");
        System.out.println("Throughput:        " + String.format("%.2f tx/sec", r.throughput));
        System.out.println("Memory:            " + r.memoryMB + " MB");
        System.out.println("Latency (min):     " + r.minLatencyUs + " μs");
        System.out.println("Latency (p50):     " + r.p50LatencyUs + " μs");
        System.out.println("Latency (p95):     " + r.p95LatencyUs + " μs");
        System.out.println("Latency (p99):     " + r.p99LatencyUs + " μs");
        System.out.println("Latency (max):     " + r.maxLatencyUs + " μs");
    }

    static class Result {
        final int numTx;
        final long durationMs;
        final long memoryMB;
        final double throughput;
        final long minLatencyUs;
        final long p50LatencyUs;
        final long p95LatencyUs;
        final long p99LatencyUs;
        final long maxLatencyUs;

        Result(int numTx, long durationMs, long memoryMB, List<Long> latencies) {
            this.numTx = numTx;
            this.durationMs = durationMs;
            this.memoryMB = memoryMB;
            this.throughput = (double) numTx / (durationMs / 1000.0);
            this.minLatencyUs = latencies.get(0);
            this.p50LatencyUs = latencies.get((int) (latencies.size() * 0.50));
            this.p95LatencyUs = latencies.get((int) (latencies.size() * 0.95));
            this.p99LatencyUs = latencies.get((int) (latencies.size() * 0.99));
            this.maxLatencyUs = latencies.get(latencies.size() - 1);
        }
    }

    static class SimpleValidator implements TransactionProcessor.BusinessLogicValidator {
        @Override
        public TransactionProcessor.ValidationResult validate(String agentId, String txId, byte[] payload) {
            return TransactionProcessor.ValidationResult.success(
                    ("diff-" + txId).getBytes(StandardCharsets.UTF_8));
        }
    }
}
