package com.mcneilio.shokuyoku;

import com.mcneilio.shokuyoku.format.Firehose;
import com.mcneilio.shokuyoku.format.JSONColumnFormat;
import com.mcneilio.shokuyoku.model.EventError;
import com.mcneilio.shokuyoku.model.EventType;
import com.mcneilio.shokuyoku.model.EventTypeColumn;
import com.mcneilio.shokuyoku.util.*;
import com.timgroup.statsd.StatsDClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ErrorWorker {

    private  StatsDClient statsd;
    KafkaConsumer<String,byte[]> consumer;
    private boolean littleEndian;
    private boolean running = true;
    private long iterationTime;

    public static void main(String[] args) {
        new ErrorWorker().start();
    }

    public ErrorWorker() {
        verifyEnvironment();
    }

    public void start(){


        SessionFactory sessionFactory = DBUtil.getSessionFactory();

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("KAFKA_SERVERS"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, System.getenv("KAFKA_GROUP_ID"));
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        String kafkaTopic = System.getenv("ERROR_WORKER_KAFKA_TOPIC")!=null ? System.getenv("ERROR_WORKER_KAFKA_TOPIC") : System.getenv("KAFKA_TOPIC");
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList(kafkaTopic));
        this.littleEndian = System.getenv("ENDIAN") != null && System.getenv("ENDIAN").equals("little");
        statsd = Statsd.getInstance();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Shutdown signal received...");
                running = false;
            }
        });

        OrcJSONSchemaDictionary orcJSONSchemaDictionary = new OrcJSONSchemaDictionary(System.getenv("HIVE_URL"), System.getenv("HIVE_DATABASE"), true, true, null);

        while (running) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(
                Integer.parseInt(System.getenv("KAFKA_POLL_DURATION_MS"))));
            for (ConsumerRecord<String, byte[]> record : records) {
                if (this.iterationTime == 0 && !records.isEmpty()) {
                    this.iterationTime = System.currentTimeMillis();
                }
                Firehose f = new Firehose(record.value(), littleEndian);
                System.out.println("Shutdown");

                String eventName = f.getTopic();
                int hadDot = eventName.lastIndexOf(".");
                if (hadDot >= 0)
                    eventName =eventName.substring(hadDot + 1);

                JSONSchemaDictionary.EventTypeJSONSchema eventTypeJSONSchema = orcJSONSchemaDictionary.getEventJSONSchema(eventName);
                if (eventTypeJSONSchema == null) {
                    JSONObject flat = new JSONColumnFormat(new JSONObject(f.getMessage())).getCopy(null, true, Collections.singleton("properties"));
                    Session session = sessionFactory.openSession();
                    Transaction trx = session.beginTransaction();
                    EventType eventType = session.get(EventType.class, eventName);
                    if(eventType==null){
                        eventType = new EventType(eventName, new Timestamp(System.currentTimeMillis()), false);
                        session.persist(eventType);
                    }else {
                        eventType.setLastError(new Timestamp(System.currentTimeMillis()));
                        session.persist(eventType);
                    }
                    for(String columnName: flat.keySet()){
                        EventTypeColumn eventTypeColumn = session.get(EventTypeColumn.class, new EventTypeColumn.EventTypeColumnKey(eventName, columnName));
                        if(eventTypeColumn==null){
                            eventTypeColumn = new EventTypeColumn(new EventTypeColumn.EventTypeColumnKey(eventName, columnName), ShokuyokuTypes.getOrcStringForClass(flat.get(columnName).getClass()));
                            session.persist(eventTypeColumn);
                        } else {
                            eventTypeColumn.setLastError(new Timestamp(System.currentTimeMillis()));
                            session.persist(eventTypeColumn);
                        }
                    }
                    trx.commit();
                    continue;
                }
                JSONColumnFormat.JSONColumnFormatFilter filter =  eventTypeJSONSchema.getJSONColumnFormatFilter();
                JSONObject cleanedObject = new JSONColumnFormat(new JSONObject(f.getMessage())).getCopy(new JSONColumnFormat.JSONColumnFormatFilter() {
                    @Override
                    public long getFilterCount() {
                        return 0;
                    }

                    @Override
                    public void resetFilterCount() {

                    }

                    @Override
                    public boolean filterPrefix(String prefix) {
                        // keep all prefixes so we get all of the flattened columns
                        return false;
                    }

                    @Override
                    public boolean filterColumn(String str, Object o) {
                        if (filter.filterColumn(str, o))
                        {
                            return true;
                        }
                        return false;
                    }
                }, false, Collections.singleton("properties"));

                if (filter.getFilterCount() > 0) {
                }
            }
            consumer.commitSync();
        }
    }


    private void verifyEnvironment() {
        boolean missingEnv = false;
        if(System.getenv("KAFKA_SERVERS") == null) {
            System.out.println("KAFKA_SERVERS environment variable should contain a comma-separated list of kafka servers. e.g. localhost:9092,localhost:9093");
            missingEnv = true;
        }
        if(System.getenv("KAFKA_GROUP_ID") == null) {
            System.out.println("KAFKA_GROUP_ID environment variable should contain the name of the Kafka group. e.g. shokuyoku");
            missingEnv = true;
        }
        if(System.getenv("KAFKA_TOPIC") == null && System.getenv("ERROR_WORKER_KAFKA_TOPIC") == null) {
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
        if(System.getenv("HIVE_DATABASE") == null) {
            System.out.println("HIVE_DATABASE environment variable should contain the name of the hive database for all events. e.g. events");
            missingEnv = true;
        }
        if(missingEnv) {
            System.out.println("Missing required environment variable(s); exiting.");
            System.exit(1);
        }
    }
}
