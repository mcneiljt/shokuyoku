package com.mcneilio.shokuyoku.funnel;

import com.mcneilio.shokuyoku.common.Firehose;
import com.mcneilio.shokuyoku.common.JSONColumnFormat;
import com.mcneilio.shokuyoku.common.Statsd;
import com.timgroup.statsd.StatsDClient;
import org.apache.kafka.clients.consumer.*;
import org.json.JSONObject;
import software.amazon.ion.Timestamp;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

public class Funnel {

    public static void main(String[] args) {
        verifyEnvironment();
        KafkaConsumer<String,byte[]> consumer = configureKafkaConsumer();
        HashMap<String, EventDriver> drivers = new HashMap<>();
        long iterationTime = 0;
        System.out.println("shokuyoku will start processing requests from topic: " + System.getenv("KAFKA_TOPIC"));
        StatsDClient statsd = Statsd.getInstance();

        long currentOffset = 0;
        while (true) {
            ConsumerRecords<String,byte[]> records = consumer.poll(Duration.ofMillis(
                    Integer.parseInt(System.getenv("KAFKA_POLL_DURATION_MS"))));
            for (ConsumerRecord<String,byte[]> record : records) {
                if(iterationTime == 0 && !records.isEmpty()) {
                    iterationTime = System.currentTimeMillis();
                }
                Firehose f = new Firehose(record.value());
                String eventName = f.getTopic();
                JSONObject msg = new JSONColumnFormat(new JSONObject(f.getMessage())).getFlattened();
                String date = msg.getString("timestamp").split("T")[0];
                if (!drivers.containsKey(eventName+date)) {
                    System.out.println("Creating driver for event: " + eventName + "with date: " + date);
                    drivers.put(eventName+date, new BasicEventDriver(eventName, date));
                }
                drivers.get(eventName+date).addMessage(msg);
                currentOffset = record.offset();
            }
            if((System.currentTimeMillis() - iterationTime) > (Integer.parseInt(System.getenv("FLUSH_MINUTES"))*1000*60)) {
                drivers.forEach((s, eventDriver) -> {
                    System.out.println("Flushing Event Driver for: "+s);
                    eventDriver.flush();
                });
                drivers.clear();
                if(currentOffset != 0) {
                    System.out.println("Committing offset: " + currentOffset + " at: " + Timestamp.nowZ());
                    currentOffset = 0;
                    consumer.commitSync();
                }
                iterationTime = System.currentTimeMillis();
            }
            statsd.histogram("kafka.poll.size", records.count(), new String[]{"env:"+System.getenv("STATSD_ENV")});
        }
    }

    private static KafkaConsumer<String,byte[]> configureKafkaConsumer() {
        KafkaConsumer<String,byte[]> consumer;
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_SERVERS"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, System.getenv("KAFKA_GROUP_ID"));
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(System.getenv("KAFKA_TOPIC")));

        return consumer;
    }

    private static void verifyEnvironment() {
        boolean missingEnv = false;
        if(System.getenv("KAFKA_SERVERS") == null) {
            System.out.println("KAFKA_SERVERS environment variable should contain a comma-separated list of kafka servers. e.g. localhost:9092,localhost:9093");
            missingEnv = true;
        }
        if(System.getenv("KAFKA_GROUP_ID") == null) {
            System.out.println("KAFKA_GROUP_ID environment variable should contain the name of the Kafka group. e.g. shokuyoku");
            missingEnv = true;
        }
        if(System.getenv("KAFKA_TOPIC") == null) {
            System.out.println("KAFKA_TOPIC environment variable should contain the topic to subscribe to. e.g. events");
            missingEnv = true;
        }
        if(System.getenv("KAFKA_POLL_DURATION_MS") == null) {
            System.out.println("KAFKA_POLL_DURATION_MS environment variable should contain the duration for the Kafka Consumer `poll` method in milliseconds. e.g. 500");
            missingEnv = true;
        }
        if(System.getenv("FLUSH_MINUTES") == null) {
            System.out.println("FLUSH_MINUTES environment variable should contain the interval between flushes in minutes. e.g. 15");
            missingEnv = true;
        }
        if(System.getenv("AWS_DEFAULT_REGION") == null) {
            System.out.println("AWS_DEFAULT_REGION environment variable should be set https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html");
            missingEnv = true;
        }
        if(System.getenv("S3_BUCKET") == null) {
            System.out.println("S3_BUCKET environment variable should contain the bucket to write events to. e.g. my-event-bucket");
            missingEnv = true;
        }
        if(System.getenv("S3_PREFIX") == null) {
            System.out.println("S3_PREFIX environment variable should contain the folder to prefix all events. e.g. data");
            missingEnv = true;
        }
        if(System.getenv("HIVE_DATABASE") == null) {
            System.out.println("HIVE_DATABASE environment variable should contain the name of the hive database for all events. e.g. events");
            missingEnv = true;
        }
        if(System.getenv("ORC_BATCH_SIZE") == null) {
            System.out.println("ORC_BATCH_SIZE environment variable should contain the number of records per orc batch. e.g. 1024");
            missingEnv = true;
        }
        if(missingEnv) {
            System.out.println("Missing required environment variable(s); exiting.");
            System.exit(1);
        }
    }
}
