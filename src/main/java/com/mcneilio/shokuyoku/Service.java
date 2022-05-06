package com.mcneilio.shokuyoku;

import com.mcneilio.shokuyoku.format.Firehose;
import io.undertow.Undertow;
import io.undertow.server.handlers.proxy.LoadBalancingProxyClient;
import io.undertow.server.handlers.proxy.ProxyHandler;
import io.undertow.util.HttpString;
import org.apache.kafka.clients.producer.*;
import org.json.JSONObject;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

public class Service {
    public Service() {
        verifyEnvironment();
        System.out.println("Shokuyoku will start listening for requests on: " + System.getenv("LISTEN_ADDR") + ":" + System.getenv("LISTEN_PORT"));
        Properties props = new Properties();
        props.put("bootstrap.servers", System.getenv("KAFKA_SERVERS"));
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        this.producer = new KafkaProducer<>(props);
    }
    protected void start() throws URISyntaxException {
        LoadBalancingProxyClient loadBalancer = new LoadBalancingProxyClient()
            .addHost(new URI("http://localhost:3000"))
            .setConnectionsPerThread(20);
        ProxyHandler pr = ProxyHandler.builder().setProxyClient(loadBalancer).setMaxRequestTime( 30000).build();

        Undertow server = Undertow.builder()
                .addHttpListener(Integer.parseInt(System.getenv("LISTEN_PORT")), System.getenv("LISTEN_ADDR"))
                .setHandler(httpServerExchange -> {

                    if(httpServerExchange.getRequestMethod().equals(new HttpString("GET"))) {
                        pr.handleRequest(httpServerExchange);
                        return;
                    }

                    String kafkaTopic = System.getenv("SERVICE_KAFKA_TOPIC") != null ? System.getenv("SERVICE_KAFKA_TOPIC") : System.getenv("KAFKA_TOPIC");

                    httpServerExchange.getRequestReceiver().receiveFullString((httpServerExchange1, s) -> {
                        JSONObject rq = new JSONObject(s);
                        Firehose f = new Firehose(rq.getString("event"), s);
                        producer.send(new ProducerRecord<>(kafkaTopic, f.getByteArray()),
                                (recordMetadata, e) -> {
                                    if( e != null ) {
                                        System.out.println("Error producing kafka record.");
                                        e.printStackTrace();
                                    } else {
                                        System.out.println("Kafka record produced.");
                                    }
                                });
                    });
                    httpServerExchange.setStatusCode(202);
                }).build();
        server.start();
    }

    private void verifyEnvironment() {
        boolean missingEnv = false;
        if(System.getenv("KAFKA_SERVERS") == null) {
            System.out.println("KAFKA_SERVERS environment variable should contain a comma-separated list of kafka servers. e.g. localhost:9092,localhost:9093");
            missingEnv = true;
        }
        if(System.getenv("KAFKA_TOPIC") == null && System.getenv("SERVICE_KAFKA_TOPIC")==null) {
            System.out.println("KAFKA_TOPIC environment variable should contain the topic to subscribe to. e.g. events");
            missingEnv = true;
        }
        if(System.getenv("LISTEN_ADDR") == null) {
            System.out.println("LISTEN_ADDR environment variable should contain the address to listen on. e.g. localhost");
            missingEnv = true;
        }
        if(System.getenv("LISTEN_PORT") == null) {
            System.out.println("LISTEN_PORT environment variable should contain the port to listen on e.g. 8080");
            missingEnv = true;
        }
        if(missingEnv) {
            System.out.println("Missing required environment variable(s); exiting.");
            System.exit(1);
        }
    }
    private final Producer<String, byte[]> producer;
}
