package com.mcneilio.shokuyoku;

import com.mcneilio.shokuyoku.filter.FilterEventByColumnValue;
import com.mcneilio.shokuyoku.format.Firehose;
import com.mcneilio.shokuyoku.format.JSONColumnFormat;
import com.mcneilio.shokuyoku.model.EventType;
import com.mcneilio.shokuyoku.model.EventTypeColumnModifier;
import com.mcneilio.shokuyoku.util.*;
import com.timgroup.statsd.StatsDClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.json.JSONObject;

import java.time.Duration;

import java.util.*;

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
        if(System.getenv("HIVE_URL") == null) {
            System.out.println("HIVE_URL environment variable should contain the hive url");
            missingEnv = true;
        }
        if(System.getenv("HIVE_DATABASE") == null) {
            System.out.println("HIVE_DATABASE environment variable should contain the hive database to pull schemas from.");
            missingEnv = true;
        }

        if(missingEnv) {
            System.out.println("Missing required environment variable(s); exiting.");
            System.exit(1);
        }
    }

    public  void start() {
        verifyEnvironment();
        System.out.println("Shokuyoku filter will start processing requests from topic: " + System.getenv("KAFKA_INPUT_TOPIC")+ " and output to: " + System.getenv("KAFKA_OUTPUT_TOPIC"));

        boolean checkSimilar = "true".equals(System.getenv("CHECK_SIMILAR"));
        boolean ignoreNulls = "true".equals(System.getenv("IGNORE_NULLS"));
        boolean allowInvalidCoercions = "true".equals(System.getenv("ALLOW_INVALID_COERCIONS"));

        statsd = Statsd.getInstance();

        KafkaConsumer<String,byte[]> consumer = new KafkaConsumer<>(createConsumerProps());
        KafkaProducer<String,byte[]> producer = new KafkaProducer<>(createProducerProps());

        consumer.subscribe(Arrays.asList(System.getenv("KAFKA_INPUT_TOPIC")));

        boolean littleEndian = System.getenv("ENDIAN") != null && System.getenv("ENDIAN").equals("little");

        Set<String> dropEvents = null;
        if (System.getenv("DROP_EVENT_TOPICS") !=null) {
            dropEvents = new HashSet<>();
            for(String eventTopic: System.getenv("DROP_EVENT_TOPICS").split(",") ){
                dropEvents.add(eventTopic.trim());
            }
        }

        SessionFactory sessionFactory = DBUtil.getSessionFactory();
        Session readSession = sessionFactory.openSession();

        Query q = readSession.createQuery("select et from EventTypeColumnModifier et", EventTypeColumnModifier.class);
        List<EventTypeColumnModifier> eventTypeColumnModifierList = q.list();

        readSession.close();

        HashMap<String, HashMap<String, Class>> schemaOverrides = new HashMap<>();
        // This is gross and I feel bad about it.
        if (System.getenv("SCHEMA_OVERRIDES") != null){
            JSONObject schemaOverridesEnv = new JSONObject(System.getenv("SCHEMA_OVERRIDES"));
            HashMap<String, Class> columns = new HashMap<>();
            for(String eventTypeName: schemaOverridesEnv.keySet()) {
                for(String columnName: ((JSONObject)schemaOverridesEnv.get(eventTypeName)).keySet()) {
                    String columnType = ((JSONObject)schemaOverridesEnv.get(eventTypeName)).getString(columnName);
                    Class c = ShokuyokuTypes.getOrcJsonType(columnType);
                    if(c!=null) {
                        columns.put(columnName, c);
                    }
                }
                schemaOverrides.put(eventTypeName, columns);
            }
        }

        OrcJSONSchemaDictionary orcJSONSchemaDictionary = new OrcJSONSchemaDictionary(System.getenv("HIVE_URL"), System.getenv("HIVE_DATABASE"), ignoreNulls, allowInvalidCoercions, schemaOverrides, eventTypeColumnModifierList);

        FilterEventByColumnValue filterEventByColumnValue = null;
        if (System.getenv().containsKey("EVENT_COLUMN_VALUE_FILTER")) {
            filterEventByColumnValue = new FilterEventByColumnValue();
        }

        long pollMS = System.getenv("KAFKA_POLL_DURATION_MS")!=null ? Integer.parseInt(System.getenv("KAFKA_POLL_DURATION_MS")) : 1000;

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(pollMS));

            statsd.histogram("filter.batch_size", records.count(), new String[]{"env:"+System.getenv("STATSD_ENV")});

            for (ConsumerRecord<String,byte[]> record : records) {
                if (record.value() == null) {
                    System.err.println("Found a NULL record. Skipping...");
                    continue;
                }
                Firehose f = new Firehose(record.value(), littleEndian);
                String eventName = f.getTopic();
                int hadDot = eventName.lastIndexOf(".");
                if (hadDot >= 0)
                    eventName =eventName.substring(hadDot + 1);

                if (dropEvents!=null && dropEvents.contains(eventName)) {
                    statsd.increment("filter.drop_override", 1, new String[]{"env:"+System.getenv("STATSD_ENV"),"topic:"+eventName});
                    continue;
                }

                JSONSchemaDictionary.EventTypeJSONSchema eventTypeJSONSchema = orcJSONSchemaDictionary.getEventJSONSchema(eventName);

                if (eventTypeJSONSchema == null) {
                    producer.send(new ProducerRecord<>(System.getenv("KAFKA_ERROR_TOPIC"), record.value()));
                    statsd.increment("filter.skipped", 1, new String[]{"env:"+System.getenv("STATSD_ENV"),"topic:"+eventName});
                    continue;
                }
                JSONColumnFormat.JSONColumnFormatFilter filter =  eventTypeJSONSchema.getJSONColumnFormatFilter();
                JSONObject cleanedObject = new JSONColumnFormat(new JSONObject(f.getMessage())).getCopy(filter, false, Collections.singleton("properties"));
                Firehose firehoseMessage = new Firehose(f.getTopic(), cleanedObject.toString());

                if (filter.getFilterCount() > 0) {
                    statsd.histogram("filter.error", filter.getFilterCount(), new String[]{"env:"+System.getenv("STATSD_ENV"),"topic:"+eventName});
                    producer.send(new ProducerRecord<>(System.getenv("KAFKA_ERROR_TOPIC"), record.value()));
                }

                if (checkSimilar) {
                    if (new JSONObject(f.getMessage()).similar(cleanedObject)) {
                        statsd.increment("filter.similar", 1, new String[]{"env:" + System.getenv("STATSD_ENV"), "similar:true", "topic:" + eventName});
                    } else {
                        statsd.increment("filter.similar", 1, new String[]{"env:" + System.getenv("STATSD_ENV"), "similar:false", "topic:" + eventName});
                    }
                }


                if (filterEventByColumnValue == null) {
                    producer.send(new ProducerRecord<>(System.getenv("KAFKA_OUTPUT_TOPIC"), firehoseMessage.getByteArray()));
                    statsd.increment("filter.forwarded", 1, new String[]{"env:"+System.getenv("STATSD_ENV"),"topic:"+eventName});
                }
                else if (filterEventByColumnValue.shouldForward(cleanedObject)) {
                    producer.send(new ProducerRecord<>(System.getenv("KAFKA_OUTPUT_TOPIC"), firehoseMessage.getByteArray()));
                    statsd.increment("filter.forwarded", 1, new String[]{"env:"+System.getenv("STATSD_ENV"),"topic:"+eventName});
                }
                else {
                    statsd.increment("filter.blocked_event", 1, new String[]{"env:"+System.getenv("STATSD_ENV"),"topic:"+eventName});
                }

            }

            consumer.commitSync();
        }
    }

    private Properties createProducerProps() {
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_SERVERS"));
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        if (System.getenv("KAFKA_LINGER_MS") != null) {
            producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, System.getenv("KAFKA_LINGER_MS"));
        }
        if (System.getenv("KAFKA_BATCH_SIZE") != null) {
            producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, System.getenv("KAFKA_BATCH_SIZE"));
        }
        if (System.getenv("KAFKA_ACKS") != null) {
            producerProps.setProperty(ProducerConfig.ACKS_CONFIG, System.getenv("KAFKA_ACKS"));
        }
        return producerProps;
    }

    private Properties createConsumerProps() {
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_SERVERS"));
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, System.getenv("KAFKA_GROUP_ID"));
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        if (System.getenv("KAFKA_CLIENT_RACK") != null) {
            consumerProps.setProperty(ConsumerConfig.CLIENT_RACK_CONFIG, System.getenv("KAFKA_CLIENT_RACK"));
        }
        if (System.getenv("KAFKA_FETCH_MIN_BYTES") != null) {
            consumerProps.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, System.getenv("KAFKA_FETCH_MIN_BYTES"));
        }
        return consumerProps;
    }

    static StatsDClient statsd;
}
