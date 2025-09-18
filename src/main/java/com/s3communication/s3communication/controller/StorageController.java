package com.s3communication.s3communication.controller;

import com.s3communication.protobuf.FileProto;
import com.s3communication.s3communication.DTO.ReplayDataResponse;
import com.s3communication.s3communication.enums.FileType;
import com.s3communication.s3communication.service.StorageService;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import jakarta.servlet.http.HttpServletRequest;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/files")
@RequiredArgsConstructor
public class StorageController {

    private final StorageService storageService;

//    @PostMapping("/upload")
//    public ResponseEntity<String> upload(@RequestParam MultipartFile file, @RequestParam String type) {
//        FileType fileType = FileType.from(type);
//        String key = storageService.uploadFile(file, fileType);
//        return ResponseEntity.ok("Uploaded: " + key);
//    }

    @PostMapping("/upload")
    public ResponseEntity<List<String>> upload(@RequestParam("file") List<MultipartFile> files,
                                               @RequestParam String type) {
        FileType fileType = FileType.from(type);
        List<String> uploadedKeys = storageService.uploadFiles(files, fileType);
        return ResponseEntity.ok(uploadedKeys);
    }

    @GetMapping("/download/**")
    public ResponseEntity<ByteArrayResource> download(HttpServletRequest request) {
        String key = decodeKey(request, "/files/download/");
        byte[] data = storageService.downloadFile(key);
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + key + "\"")
                .contentLength(data.length)
                .body(new ByteArrayResource(data));
    }

    @DeleteMapping("/delete/**")
    public ResponseEntity<String> delete(HttpServletRequest request) {
        String key = decodeKey(request, "/files/delete/");
        return ResponseEntity.ok(storageService.deleteFile(key));
    }

    @PostMapping("/download-zip")
    public ResponseEntity<ByteArrayResource> downloadZip(@RequestBody List<String> keys) {
        byte[] zip = storageService.downloadAndZipFiles(keys);
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"files.zip\"")
                .body(new ByteArrayResource(zip));
    }

    @GetMapping("/download-zip-by-prefix")
    public ResponseEntity<ByteArrayResource> downloadByPrefix(@RequestParam String prefix) {
        byte[] zip = storageService.downloadFilesByPrefixAsZip(prefix);
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"download.zip\"")
                .body(new ByteArrayResource(zip));
    }

    @GetMapping("/list-metadata-by-prefix")
    public ResponseEntity<List<Map<String, Object>>> listMetadata(@RequestParam String prefix) {
        return ResponseEntity.ok(storageService.listFileMetadataByPrefix(prefix));
    }

    @GetMapping("/timestamp-map")
    public ResponseEntity<ReplayDataResponse> getReplayData(
            @RequestParam("date") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
            @RequestParam("startHour") int startHour,
            @RequestParam("endHour") int endHour
    ) {
        return ResponseEntity.ok(storageService.getReplayData(date, startHour, endHour));
    }

    @GetMapping("/replay-range")
    public ResponseEntity<List<Map<String, Object>>> getReplayForRange(
            @RequestParam("date") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
            @RequestParam("startHour") int startHour,
            @RequestParam("startMinute") int startMinute,
            @RequestParam("endHour") int endHour,
            @RequestParam("endMinute") int endMinute
    ) {
        Map<String, FileProto.FileData> files = storageService.getReplay(date, startHour, startMinute, endHour, endMinute);
        List<Map<String, Object>> response = files.values().stream()
                .map(f -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("fileName", f.getFileName());
                    map.put("size", f.getContent().size());
                    return map;
                })
                .toList();
        return ResponseEntity.ok(response);
    }


    private String decodeKey(HttpServletRequest request, String prefix) {
        return java.net.URLDecoder.decode(request.getRequestURI().substring(prefix.length()), StandardCharsets.UTF_8);
    }
}
