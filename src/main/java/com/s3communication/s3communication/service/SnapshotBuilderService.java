package com.s3communication.s3communication.service;

import com.google.protobuf.ByteString;
import com.s3communication.protobuf.FileProto;
import com.s3communication.s3communication.config.AwsProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class SnapshotBuilderService {

    private final S3Client s3Client;
    private final AwsProperties awsProperties;
    private final ProtobufService protobufService;

     // Runs hourly to build a new snapshot from deltas and previous snapshot
    @Scheduled(cron = "0 0 * * * *") // Runs at top of every hour
    public void runHourlySnapshotBuilder() {
//        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC).minusHours(1); // Process previous hour
        ZoneId systemZone = ZoneId.systemDefault();
        LocalDateTime now = LocalDateTime.now(systemZone).minusHours(1);

        LocalDate date = now.toLocalDate();
        int hour = now.getHour();
        System.out.println("time(hour) for snapshot:"+hour);

        try {
            buildSnapshot(date, hour);
            log.info("Snapshot built for {} hour {}", date, hour);
        } catch (Exception e) {
            log.error("Failed to build snapshot for {} hour {}", date, hour, e);
        }
    }

    public void buildSnapshot(LocalDate date, int hour) throws IOException {

        if (loadDeltas(date, hour).isEmpty()) {
            log.info("⏭ No deltas found for {} hour {}, skipping snapshot build", date, hour);
            return;
        }

        String datePrefix = date.format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
        String hourStr = String.format("%02d", hour);

        // 1. Load previous snapshot
        Map<String, FileProto.FileData> snapshotFiles = new HashMap<>();
        if (hour != 0) {
            snapshotFiles.putAll(loadSnapshot(date, hour - 1));
        } else {
            // If midnight, try previous day’s 23rd hour
            snapshotFiles.putAll(loadSnapshot(date.minusDays(1), 23));
        }

        // 2. Load deltas for this hour
        Map<String, FileProto.FileData> deltas = loadDeltas(date, hour);

        // 3. Apply deltas to snapshot
        // Updates the snapshot with new or changed files from the deltas
        // Used map to overwrite by file name
        for (Map.Entry<String, FileProto.FileData> entry : deltas.entrySet()) {
            snapshotFiles.put(entry.getKey(), entry.getValue()); // Overwrite or add
        }

        // 4. Upload new snapshot to /snapshots/{hour}/filename
        for (Map.Entry<String, FileProto.FileData> entry : snapshotFiles.entrySet()) {
            String fileName = entry.getKey();
            FileProto.FileData fileData = entry.getValue();

            String snapshotKey = String.format("%s/snapshots/%s/%s", datePrefix, hourStr, fileName);

            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(awsProperties.getS3().getBucketName())
                    .key(snapshotKey)
                    .contentType("application/x-protobuf")
                    .build();

            s3Client.putObject(putRequest, RequestBody.fromBytes(fileData.toByteArray()));
        }
    }

    private Map<String, FileProto.FileData> loadSnapshot(LocalDate date, int hour) {
        String prefix = String.format("%s/snapshots/%02d/",
                date.format(DateTimeFormatter.ofPattern("yyyy/MM/dd")),
                hour);

        return listAndDownloadFilesByPrefix(prefix);
    }

    private Map<String, FileProto.FileData> loadDeltas(LocalDate date, int hour) {
        String basePrefix = String.format("%s/delta/%02d/",
                date.format(DateTimeFormatter.ofPattern("yyyy/MM/dd")),
                hour);

        ListObjectsV2Response response = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(awsProperties.getS3().getBucketName())
                .prefix(basePrefix)
                .build());

        // Collect all delta files from all 15-min folders
        List<String> keys = response.contents().stream()
                .filter(o -> !o.key().endsWith("/"))
                .map(S3Object::key)
                .collect(Collectors.toList());

        return listAndDownloadFiles(keys);
    }

    private Map<String, FileProto.FileData> listAndDownloadFilesByPrefix(String prefix) {
        ListObjectsV2Response response = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(awsProperties.getS3().getBucketName())
                .prefix(prefix)
                .build());

        List<String> keys = response.contents().stream()
                .filter(o -> !o.key().endsWith("/"))
                .map(S3Object::key)
                .collect(Collectors.toList());

        return listAndDownloadFiles(keys);
    }

    private Map<String, FileProto.FileData> listAndDownloadFiles(List<String> keys) {
        Map<String, FileProto.FileData> result = new HashMap<>();

        for (String key : keys) {
            try (ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(GetObjectRequest.builder()
                    .bucket(awsProperties.getS3().getBucketName())
                    .key(key)
                    .build())) {

                FileProto.FileData fileData = protobufService.deserializeFile(s3Object.readAllBytes());
                result.put(fileData.getFileName(), fileData);
            } catch (IOException e) {
                log.warn("Skipping corrupted or unreadable delta/snapshot file: {}", key, e);
            }
        }

        return result;
    }
}
