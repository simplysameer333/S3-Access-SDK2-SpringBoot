package com.example.s3.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
@ComponentScan({ "com.example.s3" })
public class AwsS3Example {

    public static void main(String[] args) {
        SpringApplication.run(AwsS3Example.class, args);
    }
}
