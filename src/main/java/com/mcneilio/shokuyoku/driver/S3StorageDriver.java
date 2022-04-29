package com.mcneilio.shokuyoku.driver;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.file.Path;
import java.nio.file.Paths;

public class S3StorageDriver implements StorageDriver{


    public S3StorageDriver(String s3Bucket, String prefix) {
        this.s3Bucket=s3Bucket;
        this.prefix = prefix.endsWith("/") ? prefix : prefix+"/";
    }

    @Override
    public void addFile(String date, String eventName, String fileName, Path path) {
        PutObjectRequest putOb = PutObjectRequest.builder()
            .bucket(System.getenv("S3_BUCKET"))
            .key(prefix + eventName + "/date=" + date + "/" + fileName)
            .build();

        s3.putObject(putOb, Paths.get(fileName));
    }
    String s3Bucket;
    String prefix;
    final S3Client s3 = S3Client.builder()
        .region(Region.of(System.getenv("AWS_DEFAULT_REGION") !=null ? System.getenv("AWS_DEFAULT_REGION") : "us-west-2"))
        .build();
}
