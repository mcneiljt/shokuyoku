package com.mcneilio.shokuyoku.util;

import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;

public class Statsd {
    private static StatsDClient instance = null;

    public static StatsDClient getInstance() {
        if (instance == null)
            instance = new NonBlockingStatsDClientBuilder()
                .prefix(System.getenv("STATSD_PREFIX"))
                .hostname(System.getenv("STATSD_HOST"))
                .port(Integer.parseInt(System.getenv("STATSD_PORT")))
                .aggregationFlushInterval(Integer.parseInt(System.getenv("STATSD_FLUSH_MS")))
                .build();

        return instance;
    }
}
