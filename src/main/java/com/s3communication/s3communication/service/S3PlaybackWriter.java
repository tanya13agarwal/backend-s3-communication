package com.s3communication.s3communication.service;

//package ai.replay.io.writer;
//
//import ai.replay.io.protobuf.DataChunk;
//import ai.replay.io.protobuf.RecordingMetadata;
//import ai.replay.io.session.RecordingSession;
//import ai.replay.io.util.ProtobufUtils;
//import software.amazon.awssdk.services.s3.S3Client;
//import software.amazon.awssdk.services.s3.model.*;

import com.s3communication.s3communication.interfaces.PlaybackStreamWriter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class S3PlaybackWriter implements PlaybackStreamWriter {

    private final RecordingSession session;
    private final S3Client s3Client;
    private final String bucketName;
    private final Duration flushInterval;
    private final Duration maxDuration;

    private final Map<Instant, String> uploadedFiles = new ConcurrentHashMap<>();

    private OutputStream tempOutputStream;
    private File tempFile;

    private final List<DataChunk> buffer = new ArrayList<>();
    private Instant lastFlushTime;
    private Instant startTime;

    public S3PlaybackWriter(RecordingSession session, S3Client s3Client, String bucketName,
                            Duration flushInterval, Duration maxDuration) {
        this.session = session;
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.flushInterval = flushInterval;
        this.maxDuration = maxDuration;
    }

    @Override
    public void startRecordingSession() throws IOException {
        this.startTime = Instant.now();
        this.lastFlushTime = Instant.now();

        this.tempFile = File.createTempFile("recording_", ".rec");
        this.tempOutputStream = new BufferedOutputStream(new FileOutputStream(tempFile));
    }

    @Override
    public void write(DataChunk chunk) throws IOException {
        buffer.add(chunk);
        if (Duration.between(lastFlushTime, Instant.now()).compareTo(flushInterval) >= 0) {
            flush();
        }

        if (Duration.between(startTime, Instant.now()).compareTo(maxDuration) >= 0) {
            finishRecordingAndStartNewOne();
        }
    }

    private void flush() throws IOException {
        if (buffer.isEmpty()) return;

        for (DataChunk chunk : buffer) {
            chunk.writeDelimitedTo(tempOutputStream);
        }

        tempOutputStream.flush();
        buffer.clear();
        lastFlushTime = Instant.now();
    }

    private void finishRecordingAndStartNewOne() throws IOException {
        flush();

        tempOutputStream.close();

        String objectKey = "recordings/" + session.getSessionId() + "_" + System.currentTimeMillis() + ".rec";
        putFileToS3(bucketName, objectKey, tempFile);
        uploadedFiles.put(startTime, objectKey);

        // Start a new temp file
        this.tempFile = File.createTempFile("recording_", ".rec");
        this.tempOutputStream = new BufferedOutputStream(new FileOutputStream(tempFile));
        this.startTime = Instant.now();
        this.lastFlushTime = Instant.now();
    }

    private void putFileToS3(String bucketName, String objectKey, File file) throws IOException {
        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .contentType("application/octet-stream")
                .build();

        s3Client.putObject(putRequest, file.toPath());
    }

    @Override
    public void finishAndCloseStreams() throws IOException {
        flush();

        tempOutputStream.close();

        String objectKey = "recordings/" + session.getSessionId() + "_" + System.currentTimeMillis() + ".rec";
        putFileToS3(bucketName, objectKey, tempFile);
        uploadedFiles.put(startTime, objectKey);

        // Write metadata (optional)
        writeMetadataToS3();

        Files.deleteIfExists(tempFile.toPath());
    }

    private void writeMetadataToS3() throws IOException {
        RecordingMetadata metadata = RecordingMetadata.newBuilder()
                .setSessionId(session.getSessionId())
                .setStartTime(session.getStartTime().toEpochMilli())
                .build();

        File metadataFile = File.createTempFile("recording_", ".meta");
        try (OutputStream metaOut = new BufferedOutputStream(new FileOutputStream(metadataFile))) {
            ProtobufUtils.writeToStream(metadata, metaOut);
        }

        String metadataKey = "recordings/" + session.getSessionId() + ".meta";
        putFileToS3(bucketName, metadataKey, metadataFile);

        Files.deleteIfExists(metadataFile.toPath());
    }

    @Override
    public Map<Instant, String> getRecordingFileMap() {
        return Collections.unmodifiableMap(uploadedFiles);
    }
}
