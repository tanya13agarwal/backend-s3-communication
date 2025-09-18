package com.s3communication.s3communication.DTO;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class ReplayDataResponse {
    private List<String> staticData;
    private Map<LocalDateTime, List<String>> dynamicData;
}
