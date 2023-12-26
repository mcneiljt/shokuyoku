package com.mcneilio.shokuyoku.driver;

import com.mcneilio.shokuyoku.util.Statsd;
import com.timgroup.statsd.StatsDClient;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * This StorageDriver implementation uploads files to s3.
 */
public class S3StorageDriver implements StorageDriver{

    public S3StorageDriver(String s3Bucket, String prefix) {
        this.s3Bucket = s3Bucket;
        this.prefix = prefix.endsWith("/") ? prefix : prefix + "/";
        this.statsd = Statsd.getInstance();
    }

    @Override
    public void addFile(String date, String eventName, String fileName, Path path) {
        PutObjectRequest putOb = PutObjectRequest.builder()
            .bucket(System.getenv("S3_BUCKET"))
            .key(prefix + eventName + "/date=" + date + "/" + fileName)
            .build();

        if (System.getenv("DRY_RUN") == null) {
            try {
                s3.putObject(putOb, Paths.get(fileName));
            } catch (SdkClientException e) {
                throw new RuntimeException("Error putting object in S3: ", e);
            }
        }
        statsd.increment("s3StorageDriver.files.uploaded", 1, new String[]{"env:" + System.getenv("STATSD_ENV")});
    }
    String s3Bucket;
    String prefix;
    StatsDClient statsd;
    final S3Client s3 = S3Client.builder()
        .region(Region.of(System.getenv("AWS_DEFAULT_REGION") !=null ? System.getenv("AWS_DEFAULT_REGION") : "us-west-2"))
        .build();
}
