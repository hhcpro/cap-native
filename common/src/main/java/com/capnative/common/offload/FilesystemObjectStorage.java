package com.capnative.common.offload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Filesystem-based object storage implementation.
 * Stores binary protobuf files in local directories.
 */
public class FilesystemObjectStorage implements ObjectStorage {
    private static final Logger logger = LoggerFactory.getLogger(FilesystemObjectStorage.class);

    private final Path rootPath;
    private final Path segmentsPath;
    private final Path snapshotsPath;

    public FilesystemObjectStorage(String rootDir) {
        this.rootPath = Path.of(rootDir);
        this.segmentsPath = rootPath.resolve("segments");
        this.snapshotsPath = rootPath.resolve("snapshots");

        try {
            Files.createDirectories(segmentsPath);
            Files.createDirectories(snapshotsPath);
            logger.info("Initialized filesystem storage at {}", rootPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create storage directories", e);
        }
    }

    @Override
    public void putSegment(String key, byte[] data) throws IOException {
        Path path = segmentsPath.resolve(key);
        Files.write(path, data);
        logger.debug("Wrote segment: {} ({} bytes)", key, data.length);
    }

    @Override
    public byte[] getSegment(String key) throws IOException {
        Path path = segmentsPath.resolve(key);
        if (!Files.exists(path)) {
            throw new IOException("Segment not found: " + key);
        }
        return Files.readAllBytes(path);
    }

    @Override
    public void putSnapshot(String key, byte[] data) throws IOException {
        Path path = snapshotsPath.resolve(key);
        Files.write(path, data);
        logger.debug("Wrote snapshot: {} ({} bytes)", key, data.length);
    }

    @Override
    public byte[] getSnapshot(String key) throws IOException {
        Path path = snapshotsPath.resolve(key);
        if (!Files.exists(path)) {
            throw new IOException("Snapshot not found: " + key);
        }
        return Files.readAllBytes(path);
    }

    @Override
    public List<String> listSegments() throws IOException {
        List<String> segments = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(segmentsPath, 1)) {
            paths.filter(Files::isRegularFile)
                    .forEach(p -> segments.add(p.getFileName().toString()));
        }
        return segments;
    }

    @Override
    public List<String> listSnapshots() throws IOException {
        List<String> snapshots = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(snapshotsPath, 1)) {
            paths.filter(Files::isRegularFile)
                    .forEach(p -> snapshots.add(p.getFileName().toString()));
        }
        return snapshots;
    }

    @Override
    public void deleteSegment(String key) throws IOException {
        Path path = segmentsPath.resolve(key);
        Files.deleteIfExists(path);
        logger.debug("Deleted segment: {}", key);
    }

    @Override
    public void deleteSnapshot(String key) throws IOException {
        Path path = snapshotsPath.resolve(key);
        Files.deleteIfExists(path);
        logger.debug("Deleted snapshot: {}", key);
    }

    @Override
    public boolean isHealthy() {
        return Files.isDirectory(rootPath) &&
               Files.isWritable(rootPath) &&
               Files.isReadable(rootPath);
    }
}
