package com.s3communication.s3communication.service;

import com.google.protobuf.ByteString;
import com.s3communication.protobuf.FileProto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@Service
@RequiredArgsConstructor
public class ProtobufService {

    public byte[] serializeFile(MultipartFile file) throws IOException {
        FileProto.FileData protoData = FileProto.FileData.newBuilder()
                .setFileName(file.getOriginalFilename())
                .setContent(ByteString.readFrom(file.getInputStream()))
                .build();
        return protoData.toByteArray();
    }

    public FileProto.FileData deserializeFile(byte[] data) throws IOException {
        return FileProto.FileData.parseFrom(data);
    }
}
