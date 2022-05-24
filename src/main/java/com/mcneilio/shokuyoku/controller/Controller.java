package com.mcneilio.shokuyoku.controller;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Properties;

public class Controller {
    public void setOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        admin.listConsumerGroupOffsets(System.getenv("KAFKA_GROUP"));
        admin.alterConsumerGroupOffsets("KAFKA_GROUP", offsets);
    }

    public Map<TopicPartition,OffsetAndMetadata> getOffsets() {
        try {
            return admin.listConsumerGroupOffsets(System.getenv("KAFKA_GROUP")).partitionsToOffsetAndMetadata().get();
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Controller() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_SERVERS"));
        this.admin = KafkaAdminClient.create(props);
    }

    AdminClient admin = null;
}
