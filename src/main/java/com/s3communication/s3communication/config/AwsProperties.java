package com.s3communication.s3communication.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

//Aws properties from application.yml
@Configuration
@ConfigurationProperties(prefix = "aws")
@Data
public class AwsProperties {

    private String region;
    private S3 s3; // create instance of s3

    @Data
    public static class S3{
        private String bucketName;
    }
}
