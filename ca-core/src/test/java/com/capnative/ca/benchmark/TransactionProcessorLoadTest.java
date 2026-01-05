package com.capnative.ca.benchmark;

import com.capnative.ca.service.BatchTransactionProcessor;
import com.capnative.ca.service.TransactionProcessor;
import com.capnative.ca.storage.AgentLedgerStorage;
import com.capnative.ca.storage.ConsolidatedLedgerStorage;
import com.capnative.common.storage.LmdbEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Load testing tool for comparing TransactionProcessor vs BatchTransactionProcessor.
 * Generates actual performance measurements with real workloads.
 */
public class TransactionProcessorLoadTest {
    private static final Logger logger = LoggerFactory.getLogger(TransactionProcessorLoadTest.class);

    private static final int WARMUP_TRANSACTIONS = 1000;
    private static final int[] TEST_TRANSACTION_COUNTS = {1000, 5000, 10000, 50000};
    private static final int[] BATCH_SIZES = {10, 50, 100, 200};
    private static final int[] THREAD_COUNTS = {2, 4, 8};

    public static void main(String[] args) throws Exception {
        logger.info("=".repeat(80));
        logger.info("TRANSACTION PROCESSOR PERFORMANCE BENCHMARK");
        logger.info("=".repeat(80));

        // Test legacy processor
        logger.info("\n### LEGACY SINGLE-THREADED PROCESSOR ###\n");
        runLegacyProcessorTests();

        // Test batch processor with different configurations
        logger.info("\n### BATCH TRANSACTION PROCESSOR ###\n");
        for (int batchSize : BATCH_SIZES) {
            for (int threads : THREAD_COUNTS) {
                logger.info("\n--- Batch Size: {}, Validation Threads: {} ---", batchSize, threads);
                runBatchProcessorTests(batchSize, threads);
            }
        }

        logger.info("\n" + "=".repeat(80));
        logger.info("BENCHMARK COMPLETE");
        logger.info("=".repeat(80));
    }

    private static void runLegacyProcessorTests() throws Exception {
        for (int txCount : TEST_TRANSACTION_COUNTS) {
            File dbDir = createTempDir("legacy-" + txCount);
            try {
                BenchmarkResult result = benchmarkLegacyProcessor(dbDir, txCount);
                printResult("Legacy Processor", txCount, result);
            } finally {
                cleanup(dbDir);
            }
        }
    }

    private static void runBatchProcessorTests(int batchSize, int threads) throws Exception {
        for (int txCount : TEST_TRANSACTION_COUNTS) {
            File dbDir = createTempDir("batch-" + batchSize + "-" + threads + "-" + txCount);
            try {
                BenchmarkResult result = benchmarkBatchProcessor(dbDir, txCount, batchSize, threads);
                printResult(String.format("Batch Processor (batch=%d, threads=%d)", batchSize, threads),
                           txCount, result);
            } finally {
                cleanup(dbDir);
            }
        }
    }

    private static BenchmarkResult benchmarkLegacyProcessor(File dbDir, int transactionCount) throws Exception {
        LmdbEnv env = new LmdbEnv(dbDir, 1024 * 1024 * 1024, 10);
        AgentLedgerStorage agentLedgerStorage = new AgentLedgerStorage(env);
        ConsolidatedLedgerStorage consolidatedLedgerStorage = new ConsolidatedLedgerStorage(env);

        SimpleTestValidator validator = new SimpleTestValidator();
        TransactionProcessor processor = new TransactionProcessor(
                agentLedgerStorage, consolidatedLedgerStorage, validator);

        // Warmup
        logger.debug("Warming up...");
        for (int i = 0; i < WARMUP_TRANSACTIONS; i++) {
            byte[] payload = generatePayload(100);
            processor.processTransaction("agent-warmup", i, "tx-warmup-" + i, payload);
        }

        // Actual benchmark
        logger.debug("Running benchmark with {} transactions...", transactionCount);
        long startTime = System.nanoTime();
        long startMemory = getUsedMemory();

        int committed = 0;
        int rejected = 0;
        List<Long> latencies = new ArrayList<>();

        for (int i = 0; i < transactionCount; i++) {
            byte[] payload = generatePayload(100);
            long txStartTime = System.nanoTime();

            TransactionProcessor.TransactionResult result = processor.processTransaction(
                    "agent-test", i, "tx-test-" + i, payload);

            long txEndTime = System.nanoTime();
            latencies.add((txEndTime - txStartTime) / 1_000); // microseconds

            if (result.isCommitted()) {
                committed++;
            } else {
                rejected++;
            }
        }

        long endTime = System.nanoTime();
        long endMemory = getUsedMemory();

        env.close();

        return new BenchmarkResult(
                transactionCount,
                committed,
                rejected,
                (endTime - startTime) / 1_000_000, // milliseconds
                endMemory - startMemory,
                latencies
        );
    }

    private static BenchmarkResult benchmarkBatchProcessor(
            File dbDir, int transactionCount, int batchSize, int threads) throws Exception {

        LmdbEnv env = new LmdbEnv(dbDir, 1024 * 1024 * 1024, 10);
        AgentLedgerStorage agentLedgerStorage = new AgentLedgerStorage(env);
        ConsolidatedLedgerStorage consolidatedLedgerStorage = new ConsolidatedLedgerStorage(env);

        SimpleTestValidator validator = new SimpleTestValidator();
        BatchTransactionProcessor processor = new BatchTransactionProcessor(
                agentLedgerStorage, consolidatedLedgerStorage, validator,
                batchSize, threads, 10000, "test-ca");

        processor.start();

        // Warmup
        logger.debug("Warming up...");
        List<CompletableFuture<TransactionProcessor.TransactionResult>> warmupFutures = new ArrayList<>();
        for (int i = 0; i < WARMUP_TRANSACTIONS; i++) {
            byte[] payload = generatePayload(100);
            warmupFutures.add(processor.submitTransaction("agent-warmup", i, "tx-warmup-" + i, payload));
        }
        for (CompletableFuture<?> future : warmupFutures) {
            future.get();
        }

        // Actual benchmark
        logger.debug("Running benchmark with {} transactions...", transactionCount);
        long startTime = System.nanoTime();
        long startMemory = getUsedMemory();

        AtomicLong committed = new AtomicLong(0);
        AtomicLong rejected = new AtomicLong(0);
        List<Long> latencies = new ArrayList<>();
        List<CompletableFuture<TransactionProcessor.TransactionResult>> futures = new ArrayList<>();

        for (int i = 0; i < transactionCount; i++) {
            byte[] payload = generatePayload(100);
            long txStartTime = System.nanoTime();

            CompletableFuture<TransactionProcessor.TransactionResult> future = processor.submitTransaction(
                    "agent-test", i, "tx-test-" + i, payload);

            future.thenAccept(result -> {
                long txEndTime = System.nanoTime();
                synchronized (latencies) {
                    latencies.add((txEndTime - txStartTime) / 1_000); // microseconds
                }

                if (result.isCommitted()) {
                    committed.incrementAndGet();
                } else {
                    rejected.incrementAndGet();
                }
            });

            futures.add(future);
        }

        // Wait for all transactions to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

        long endTime = System.nanoTime();
        long endMemory = getUsedMemory();

        processor.stop();
        env.close();

        return new BenchmarkResult(
                transactionCount,
                (int) committed.get(),
                (int) rejected.get(),
                (endTime - startTime) / 1_000_000, // milliseconds
                endMemory - startMemory,
                latencies
        );
    }

    private static byte[] generatePayload(int size) {
        String data = UUID.randomUUID().toString();
        while (data.length() < size) {
            data += UUID.randomUUID().toString();
        }
        return data.substring(0, size).getBytes(StandardCharsets.UTF_8);
    }

    private static File createTempDir(String prefix) {
        File dir = new File("/tmp/ca-benchmark-" + prefix + "-" + System.nanoTime());
        dir.mkdirs();
        return dir;
    }

    private static void cleanup(File dir) {
        if (dir.exists()) {
            deleteRecursively(dir);
        }
    }

    private static void deleteRecursively(File file) {
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

    private static long getUsedMemory() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static void printResult(String processorName, int txCount, BenchmarkResult result) {
        result.latencies.sort(Long::compareTo);

        long p50 = result.latencies.get((int) (result.latencies.size() * 0.50));
        long p95 = result.latencies.get((int) (result.latencies.size() * 0.95));
        long p99 = result.latencies.get((int) (result.latencies.size() * 0.99));
        long min = result.latencies.get(0);
        long max = result.latencies.get(result.latencies.size() - 1);

        double throughput = (double) result.totalTransactions / (result.durationMs / 1000.0);

        System.out.println();
        System.out.println("Processor: " + processorName);
        System.out.println("  Transactions:     " + result.totalTransactions);
        System.out.println("  Committed:        " + result.committed);
        System.out.println("  Rejected:         " + result.rejected);
        System.out.println("  Duration:         " + result.durationMs + " ms");
        System.out.println("  Throughput:       " + String.format("%.2f", throughput) + " tx/sec");
        System.out.println("  Memory Used:      " + String.format("%.2f", result.memoryUsedBytes / 1024.0 / 1024.0) + " MB");
        System.out.println("  Latency (min):    " + min + " μs");
        System.out.println("  Latency (p50):    " + p50 + " μs");
        System.out.println("  Latency (p95):    " + p95 + " μs");
        System.out.println("  Latency (p99):    " + p99 + " μs");
        System.out.println("  Latency (max):    " + max + " μs");
        System.out.println();
    }

    static class BenchmarkResult {
        final int totalTransactions;
        final int committed;
        final int rejected;
        final long durationMs;
        final long memoryUsedBytes;
        final List<Long> latencies;

        BenchmarkResult(int totalTransactions, int committed, int rejected,
                       long durationMs, long memoryUsedBytes, List<Long> latencies) {
            this.totalTransactions = totalTransactions;
            this.committed = committed;
            this.rejected = rejected;
            this.durationMs = durationMs;
            this.memoryUsedBytes = memoryUsedBytes;
            this.latencies = latencies;
        }
    }

    /**
     * Simple validator that always accepts transactions with minimal processing.
     */
    static class SimpleTestValidator implements TransactionProcessor.BusinessLogicValidator {
        @Override
        public TransactionProcessor.ValidationResult validate(String agentId, String txId, byte[] payload) {
            // Simulate minimal validation work
            byte[] ledgerDiffs = ("diff-" + txId).getBytes(StandardCharsets.UTF_8);
            return TransactionProcessor.ValidationResult.success(ledgerDiffs);
        }
    }
}
