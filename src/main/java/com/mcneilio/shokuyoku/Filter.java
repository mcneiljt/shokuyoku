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

    public static void main(String[] args){
        System.out.println("shokuyoku will start processing requests from topic: " + System.getenv("KAFKA_TOPIC"));
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

        consumer.subscribe(Arrays.asList(System.getenv("KAFKA_TOPIC")));

        OrcJSONSchemaDictionary orcJSONSchemaDictionary = new OrcJSONSchemaDictionary();

        long currentOffset = 0;
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(
                Integer.parseInt(System.getenv("KAFKA_POLL_DURATION_MS"))));

            System.out.println("shokuyoku will start processing requests from topic: " + records.count());

            for (ConsumerRecord<String,byte[]> record : records) {
                Firehose f = new Firehose(record.value());
                String eventName = f.getTopic();
                int hadDot =eventName.lastIndexOf(".");
                if(hadDot>=0)
                    eventName =eventName.substring(hadDot+1);

                JSONSchemaDictionary.EventTypeJSONSchema eventTypeJSONSchema = orcJSONSchemaDictionary.getEventJSONSchema(eventName);
                if(eventTypeJSONSchema==null){
                    saveEventError(record);
                    continue;
                }

                JSONObject cleanedObject = new JSONColumnFormat(new JSONObject(f.getMessage())).getCopy(eventTypeJSONSchema.getJSONColumnFormatFilter(), false, Collections.singleton("properties"));
                Firehose firehoseMessage = new Firehose(f.getTopic(), cleanedObject.toString());

                producer.send(new ProducerRecord<>("blackhole_filter", firehoseMessage.getByteArray()));
                System.out.println("ASD");
            }

            consumer.commitSync();
        }
    }

    public static void saveEventError(ConsumerRecord<String,byte[]> record){
        // TODO do something here
    }
}
