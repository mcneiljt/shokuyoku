package com.mcneilio.shokuyoku.util;

import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;

public class Statsd {
    private static StatsDClient instance = null;

    public static StatsDClient getInstance() {
        if (instance == null)
            instance = new NonBlockingStatsDClientBuilder()
                .prefix(System.getenv("STATSD_PREFIX"))
                .hostname(System.getenv("STATSD_HOST")!=null ? System.getenv("STATSD_HOST") : "localhost")
                .port(System.getenv("STATSD_PORT") !=null ? Integer.parseInt(System.getenv("STATSD_PORT")) : 8125)
                .aggregationFlushInterval(System.getenv("STATSD_FLUSH_MS") !=null ? Integer.parseInt(System.getenv("STATSD_FLUSH_MS")) : 1000)
                .build();

        return instance;
    }
}
