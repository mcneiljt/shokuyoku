package com.mcneilio.shokuyoku;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.mcneilio.shokuyoku.format.Firehose;
import com.mcneilio.shokuyoku.format.JSONColumnFormat;
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
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.mcneilio.shokuyoku.util.ShokuyokuTypes.getArrayType;

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

        Cache<Object, Object> seen = CacheBuilder.newBuilder()
            .maximumSize(50000)
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

        Session readSession = sessionFactory.openSession();

        while (running) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(
                Integer.parseInt(System.getenv("KAFKA_POLL_DURATION_MS"))));
            Session writeSession = sessionFactory.openSession();

            for (ConsumerRecord<String, byte[]> record : records) {
                if (this.iterationTime == 0 && !records.isEmpty()) {
                    this.iterationTime = System.currentTimeMillis();
                }
                Firehose f = new Firehose(record.value(), littleEndian);

                String eventName = f.getTopic();
                int hadDot = eventName.lastIndexOf(".");
                if (hadDot >= 0)
                    eventName =eventName.substring(hadDot + 1);

                JSONSchemaDictionary.EventTypeJSONSchema eventTypeJSONSchema = orcJSONSchemaDictionary.getEventJSONSchema(eventName);


                if (eventTypeJSONSchema == null) {
                    JSONObject flat = new JSONColumnFormat(new JSONObject(f.getMessage())).getCopy(null, true, Collections.singleton("properties"));
                    EventType eventType = readSession.get(EventType.class, eventName);

                    if(seen.getIfPresent(eventName)==null) {
                        seen.put(eventName, true);
                        if (eventType == null) {
                            eventType = new EventType(eventName, new Timestamp(System.currentTimeMillis()), false);
                            Transaction trx = writeSession.beginTransaction();
                            writeSession.persist(eventType);
                            trx.commit();
                        } else {
                            eventType.setLastError(new Timestamp(System.currentTimeMillis()));
                            Transaction trx = writeSession.beginTransaction();
                            writeSession.merge(eventType);
                            trx.commit();                        }
                    }
                    for(String columnName: flat.keySet()){
                        String cacheKey = eventName+":"+columnName;
                        if(seen.getIfPresent(cacheKey)==null) {
                            seen.put(cacheKey, true);
                            EventTypeColumn eventTypeColumn = readSession.get(EventTypeColumn.class, new EventTypeColumn.EventTypeColumnKey(eventName, columnName));
                            if (eventTypeColumn == null) {

                                Class cl = flat.get(columnName).getClass();
                                if (flat.get(columnName) instanceof JSONArray) {
                                    cl = getArrayType((JSONArray) flat.get(columnName));
                                    System.out.println("Unsupported Column Type2: " + cl);
                                }

	
				String inferredType =ShokuyokuTypes.getOrcStringForClass(cl);
				inferredType = inferredType !=null ? inferredType : ("Unknown Type: "+cl.toString());

                                eventTypeColumn = new EventTypeColumn(new EventTypeColumn.EventTypeColumnKey(eventName, columnName), inferredType, new Timestamp(System.currentTimeMillis()));

                                Transaction trx = writeSession.beginTransaction();
                                writeSession.persist(eventTypeColumn);
                                trx.commit();
                            } else {
                                eventTypeColumn.setLastError(new Timestamp(System.currentTimeMillis()));

                                Transaction trx = writeSession.beginTransaction();
                                writeSession.merge(eventTypeColumn);
                                trx.commit();
                            }
                        }
                    }
                //    session.flush();
                } else {
                    List<String[]> errorColumns = new ArrayList<>();
                    JSONColumnFormat.JSONColumnFormatFilter filter = eventTypeJSONSchema.getJSONColumnFormatFilter();
                    new JSONColumnFormat(new JSONObject(f.getMessage())).getCopy(new JSONColumnFormat.JSONColumnFormatFilter() {
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
                            if (filter.filterColumn(str, o)) {

                                Class cl = o.getClass();
                                if (o instanceof JSONArray) {
                                    cl = getArrayType((JSONArray) o);
                                    System.out.println("Unsupported Column Type: " + cl);

                                }

                                String inferredType = ShokuyokuTypes.getOrcStringForClass(cl);
                                errorColumns.add(new String[]{str, inferredType !=null ? inferredType : ("Unknown Type: "+o.getClass().toString())});
                                System.out.println("Flilter column: "+str);
                                return true;
                            }
                            return false;
                        }

                        @Override
                        public Object modifyColumn(String str, Object o) {
                            return o;
                        }
                    }, false, Collections.singleton("properties"));

                    if(errorColumns.size()>0)
                    {
                        Session session = sessionFactory.openSession();
                        for(String[] columnE: errorColumns){
                            String columnName = columnE[0];
                            String cacheKey = eventName+":"+columnName;
                            if(seen.getIfPresent(cacheKey)==null) {
                                seen.put(cacheKey, true);
                                EventTypeColumn eventTypeColumn = session.get(EventTypeColumn.class, new EventTypeColumn.EventTypeColumnKey(eventName, columnName));
                                if (eventTypeColumn == null) {
                                    eventTypeColumn = new EventTypeColumn(new EventTypeColumn.EventTypeColumnKey(eventName, columnName), columnE[1], new Timestamp(System.currentTimeMillis()));

                                    Transaction trx = writeSession.beginTransaction();
                                    writeSession.persist(eventTypeColumn);
                                    trx.commit();
                                } else {
                                    eventTypeColumn.setLastError(new Timestamp(System.currentTimeMillis()));

                                    Transaction trx = writeSession.beginTransaction();
                                    writeSession.merge(eventTypeColumn);
                                    trx.commit();
                                }
                            }
                        }
                        System.out.println("Committing");
                    }
                }
            }
            consumer.commitSync();
            writeSession.close();
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
