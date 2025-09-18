package com.s3communication.s3communication;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class S3communicationApplication {

	public static void main(String[] args) {
		SpringApplication.run(S3communicationApplication.class, args);
	}

}
