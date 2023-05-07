package com.wepay.kafka.connect.bigquery.metrics.events;

public class BQLoaderRunStatusEvent implements MetricEvent {

    private final boolean isSuccessfulBQLoaderRun;

    public BQLoaderRunStatusEvent(boolean isSuccessfulBQLoaderRun) {
        this.isSuccessfulBQLoaderRun = isSuccessfulBQLoaderRun;
    }

    public boolean isSuccessfulBQLoaderRun() {
        return isSuccessfulBQLoaderRun;
    }
}
