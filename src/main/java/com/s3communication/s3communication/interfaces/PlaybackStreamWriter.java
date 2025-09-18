package com.s3communication.s3communication.interfaces;

import ai.replay.io.protobuf.DataChunk;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

public interface PlaybackStreamWriter {

    void startRecordingSession() throws IOException;

    void write(DataChunk chunk) throws IOException;

    void finishAndCloseStreams() throws IOException;

    Map<Instant, String> getRecordingFileMap();
}
