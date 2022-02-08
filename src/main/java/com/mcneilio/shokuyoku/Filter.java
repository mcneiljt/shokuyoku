package com.mcneilio.shokuyoku;

import com.mcneilio.shokuyoku.format.Firehose;
import com.mcneilio.shokuyoku.format.JSONColumnFormat;
import com.mcneilio.shokuyoku.util.JSONSchemaDictionary;
import com.mcneilio.shokuyoku.util.OrcJSONSchemaDictionary;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.time.Duration;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class Filter {

    private static void verifyEnvironment() {
        boolean missingEnv = false;
        if(System.getenv("KAFKA_SERVERS") == null) {
            System.out.println("KAFKA_SERVERS environment variable should contain a comma-separated list of kafka servers. e.g. localhost:9092,localhost:9093");
            missingEnv = true;
        }
        if(System.getenv("KAFKA_GROUP_ID") == null) {
            System.out.println("KAFKA_GROUP_ID environment variable should contain a the kafka group id that the consume should use.");
            missingEnv = true;
        }


        if(System.getenv("KAFKA_INPUT_TOPIC") == null) {
            System.out.println("KAFKA_TOPIC environment variable should contain the topic to subscribe to. e.g. events");
            missingEnv = true;
        }
        if(System.getenv("KAFKA_ERROR_TOPIC") == null) {
            System.out.println("KAFKA_ERROR_TOPIC environment variable should contain the topic to send errors to to. e.g. events");
            missingEnv = true;
        }
        if(System.getenv("KAFKA_OUTPUT_TOPIC") == null) {
            System.out.println("KAFKA_OUTPUT_TOPIC environment variable should contain the topic to publish to. e.g. events");
            missingEnv = true;
        }

        if(missingEnv) {
            System.out.println("Missing required environment variable(s); exiting.");
            System.exit(1);
        }
    }

    public static void main(String[] args){
        verifyEnvironment();
        System.out.println("Shokuyoku filter will start processing requests from topic: " + System.getenv("KAFKA_INPUT_TOPIC") " and output to: " + System.getenv("KAFKA_OUTPUT_TOPIC"));
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_SERVERS"));
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, System.getenv("KAFKA_GROUP_ID"));
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        Properties producerProps = new Properties();
        producerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_SERVERS"));

        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");


        KafkaConsumer<String,byte[]> consumer = new KafkaConsumer<>(consumerProps);

        KafkaProducer<String,byte[]> producer = new KafkaProducer<>(producerProps);

        consumer.subscribe(Arrays.asList(System.getenv("KAFKA_INPUT_TOPIC")));

        OrcJSONSchemaDictionary orcJSONSchemaDictionary = new OrcJSONSchemaDictionary();

        long pollMS = System.getenv("KAFKA_POLL_DURATION_MS")!=null ? Integer.parseInt(System.getenv("KAFKA_POLL_DURATION_MS")) : 1000;

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(pollMS));

            System.out.println("Received batch of size: " + records.count());

            for (ConsumerRecord<String,byte[]> record : records) {
                Firehose f = new Firehose(record.value());
                String eventName = f.getTopic();
                int hadDot =eventName.lastIndexOf(".");
                if(hadDot>=0)
                    eventName =eventName.substring(hadDot+1);

                JSONSchemaDictionary.EventTypeJSONSchema eventTypeJSONSchema = orcJSONSchemaDictionary.getEventJSONSchema(eventName);
                if(eventTypeJSONSchema==null){
                    saveEventError(record);
                    producer.send(new ProducerRecord<>(System.getenv("KAFKA_ERROR_TOPIC"), record.value()));

                    continue;
                }

                JSONObject cleanedObject = new JSONColumnFormat(new JSONObject(f.getMessage())).getCopy(eventTypeJSONSchema.getJSONColumnFormatFilter(), false, Collections.singleton("properties"));
                Firehose firehoseMessage = new Firehose(f.getTopic(), cleanedObject.toString());

                producer.send(new ProducerRecord<>(System.getenv("KAFKA_OUTPUT_TOPIC"), firehoseMessage.getByteArray()));
            }

            consumer.commitSync();
        }
    }
}
