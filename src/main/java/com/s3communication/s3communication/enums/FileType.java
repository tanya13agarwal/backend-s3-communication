package com.s3communication.s3communication.enums;

public enum FileType {
    SNAPSHOT,
    DELTA;

    public static FileType from(String value) {
        return FileType.valueOf(value.toUpperCase());
    }
}
