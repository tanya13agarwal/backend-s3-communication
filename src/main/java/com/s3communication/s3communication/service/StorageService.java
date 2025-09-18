package com.s3communication.s3communication.service;

import com.s3communication.s3communication.config.AwsProperties;
import com.s3communication.s3communication.DTO.ReplayDataResponse;
import com.s3communication.s3communication.enums.FileType;
import com.s3communication.protobuf.FileProto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Service
@RequiredArgsConstructor
@Slf4j
public class StorageService {

    private final S3Client s3Client;
    private final S3Presigner s3Presigner;
    private final AwsProperties awsProperties;
    private final ProtobufService protobufService;

    public List<String> uploadFiles(List<MultipartFile> files, FileType type) {
        List<String> uploadedKeys = new ArrayList<>();

        for (MultipartFile file : files) {
            try {
                String filename = Paths.get(Objects.requireNonNull(file.getOriginalFilename())).getFileName().toString();
                LocalDateTime now = LocalDateTime.now();

                String datePath = now.format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
                String hour = now.format(DateTimeFormatter.ofPattern("HH"));
                String minute = roundToNearest15Minutes(now.toLocalTime()).format(DateTimeFormatter.ofPattern("mm"));

                // Build path for snapshot or delta
                String fullKey = switch (type) {
                    case SNAPSHOT -> String.format("%s/snapshots/%s/%s", datePath, hour, filename);
                    case DELTA -> String.format("%s/delta/%s/%s/%s", datePath, hour, minute, filename);
                };

                // Serialize using protobuf
                byte[] serialized = protobufService.serializeFile(file);

                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(awsProperties.getS3().getBucketName())
                        .key(fullKey)
                        .contentType("application/x-protobuf")
                        .build();

                s3Client.putObject(request, RequestBody.fromBytes(serialized));
                uploadedKeys.add(fullKey);

            } catch (IOException e) {
                throw new RuntimeException("Failed to upload one of the files", e);
            }
        }

        return uploadedKeys;
    }

    public byte[] downloadFile(String key) {
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(awsProperties.getS3().getBucketName())
                    .key(key)
                    .build();

            try (ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(request)) {
                FileProto.FileData protoData = protobufService.deserializeFile(s3Object.readAllBytes());
                return protoData.getContent().toByteArray();
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to download file", e);
        }
    }

    public String deleteFile(String key) {
        try {
            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(awsProperties.getS3().getBucketName())
                    .key(key)
                    .build());
            return "Deleted: " + key;
        } catch (S3Exception e) {
            return "Error deleting: " + e.awsErrorDetails().errorMessage();
        }
    }

    public byte[] downloadAndZipFiles(List<String> keys) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ZipOutputStream zos = new ZipOutputStream(baos)) {
            // iterate over all keys and downloads and deserializes each file
            for (String key : keys) {
                byte[] fileBytes = downloadFile(key);
                zos.putNextEntry(new ZipEntry(Paths.get(key).getFileName().toString()));
                zos.write(fileBytes);
                zos.closeEntry();
            }

            zos.finish();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to zip files", e);
        }
    }

    public byte[] downloadFilesByPrefixAsZip(String prefix) {
        String normalized = prefix.endsWith("/") ? prefix : prefix + "/";

        ListObjectsV2Response response = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(awsProperties.getS3().getBucketName())
                .prefix(normalized)
                .build());

        List<String> keys = response.contents().stream()
                .map(S3Object::key)
                .filter(k -> !k.endsWith("/")) // Ignore folder placeholders
                .toList();

        return downloadAndZipFilesWithPaths(keys, normalized);
    }

    private byte[] downloadAndZipFilesWithPaths(List<String> keys, String basePrefix) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ZipOutputStream zos = new ZipOutputStream(baos)) {

            for (String key : keys) {
                byte[] fileBytes = downloadFile(key);

                // Preserve folder structure by making ZIP entry relative to base prefix
                String zipEntryPath = key.substring(basePrefix.length());

                zos.putNextEntry(new ZipEntry(zipEntryPath));
                zos.write(fileBytes);
                zos.closeEntry();
            }

            zos.finish();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to zip files with structure", e);
        }
    }

    public List<Map<String, Object>> listFileMetadataByPrefix(String prefix) {
        String normalized = prefix.endsWith("/") ? prefix : prefix + "/";

        return s3Client.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(awsProperties.getS3().getBucketName())
                        .prefix(normalized)
                        .build())
                .contents().stream()
                .filter(obj -> !obj.key().endsWith("/"))
                .map(obj -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("fileName", Paths.get(obj.key()).getFileName().toString());
                    map.put("s3Key", obj.key());
                    map.put("size", obj.size());
                    map.put("lastModified", obj.lastModified().atZone(ZoneId.systemDefault()).toString());
                    return map;
                })
                .toList();
    }

    // Returns a DTO for replay data segmented by time
    // without snapshot and delta logic
    public ReplayDataResponse getReplayData(LocalDate date, int startHour, int endHour) {
        List<String> staticData = getStaticPresignedUrls(date);
        Map<LocalDateTime, List<String>> dynamicData = new TreeMap<>();

        String datePrefix = date.format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));

        LocalDateTime startTime = date.atTime(startHour, 0);
        LocalDateTime endTime = date.atTime(endHour, 59);

        for (LocalDateTime current = startTime; !current.isAfter(endTime); current = current.plusMinutes(15)) {
            String hour = String.format("%02d", current.getHour());
            String minute = String.format("%02d", (current.getMinute() / 15) * 15);  // rounded

            String bucketPrefix = String.format("%s/delta/%s/%s/", datePrefix, hour, minute);

            ListObjectsV2Response response = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(awsProperties.getS3().getBucketName())
                    .prefix(bucketPrefix)
                    .build());

            for (S3Object obj : response.contents()) {
                Matcher matcher = Pattern.compile("(\\d{4})-").matcher(Paths.get(obj.key()).getFileName().toString());
                if (matcher.find()) {
                    String timeStr = matcher.group(1);
                    LocalDateTime timestamp = date.atTime(
                            Integer.parseInt(timeStr.substring(0, 2)),
                            Integer.parseInt(timeStr.substring(2, 4))
                    );

                    dynamicData.computeIfAbsent(timestamp, t -> new ArrayList<>())
                            .add(generatePresignedUrl(obj.key()));
                }
            }
        }

        return new ReplayDataResponse(staticData, dynamicData);
    }

    public Map<String, FileProto.FileData> getReplay(LocalDate date, int startHour, int startMinute, int endHour, int endMinute) {
        Map<String, FileProto.FileData> reconstructedData = new HashMap<>();

        // Load base snapshot at start hour
        reconstructedData.putAll(loadSnapshot(date, startHour));

        // Iterate from start to end in 15-minute intervals
        LocalDateTime start = date.atTime(startHour, (startMinute / 15) * 15);
        LocalDateTime end = date.atTime(endHour, (endMinute / 15) * 15);

        for (LocalDateTime current = start; !current.isAfter(end); current = current.plusMinutes(15)) {
            reconstructedData.putAll(loadDeltaFiles(current)); // Apply deltas
        }

        return reconstructedData;
    }

    private Map<String, FileProto.FileData> loadSnapshot(LocalDate date, int hour) {
        String prefix = String.format("%s/snapshots/%02d/",
                date.format(DateTimeFormatter.ofPattern("yyyy/MM/dd")), hour);

        return listAndDownloadFiles(prefix);
    }

    private Map<String, FileProto.FileData> loadDeltaFiles(LocalDateTime timestamp) {
        String prefix = String.format("%s/delta/%02d/%02d/",
                timestamp.toLocalDate().format(DateTimeFormatter.ofPattern("yyyy/MM/dd")),
                timestamp.getHour(),
                (timestamp.getMinute() / 15) * 15);

        return listAndDownloadFiles(prefix);
    }

    private Map<String, FileProto.FileData> listAndDownloadFiles(String prefix) {
        ListObjectsV2Response listObjectsResponse = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(awsProperties.getS3().getBucketName())
                .prefix(prefix)
                .build());

        List<String> keys = listObjectsResponse.contents().stream()
                .filter(o -> !o.key().endsWith("/"))
                .map(S3Object::key)
                .toList();

        Map<String, FileProto.FileData> files = new HashMap<>();
        for (String key : keys) {
            try (ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(GetObjectRequest.builder()
                    .bucket(awsProperties.getS3().getBucketName())
                    .key(key)
                    .build())) {
                FileProto.FileData fileData = protobufService.deserializeFile(s3Object.readAllBytes());
                files.put(fileData.getFileName(), fileData);
            } catch (IOException e) {
                log.warn("Failed to load file: {}", key, e);
            }
        }
        return files;
    }

    public String generatePresignedUrl(String key) {
        GetObjectRequest getRequest = GetObjectRequest.builder()
                .bucket(awsProperties.getS3().getBucketName())
                .key(key)
                .build();

        GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(60))
                .getObjectRequest(getRequest)
                .build();

        return s3Presigner.presignGetObject(presignRequest).url().toString();
    }

    public List<String> getStaticPresignedUrls(LocalDate date) {
        String prefix = date.format(DateTimeFormatter.ofPattern("yyyy/MM/dd")) + "/snapshots/";

        return s3Client.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(awsProperties.getS3().getBucketName())
                        .prefix(prefix)
                        .build())
                .contents().stream()
                .filter(o -> !o.key().endsWith("/"))
                .map(o -> generatePresignedUrl(o.key()))
                .toList();
    }

    // round to nearest 15 minutes
    private LocalTime roundToNearest15Minutes(LocalTime time) {
        int minute = (time.getMinute() / 15) * 15;
        return LocalTime.of(time.getHour(), minute);
    }

}
