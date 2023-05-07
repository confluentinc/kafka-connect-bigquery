package com.wepay.kafka.connect.bigquery.metrics.events;

public class StorageBlobDeleteOperationStatusEvent implements MetricEvent {

    private final int numberOfSuccessfullyDeletedBlobs;
    private final int numberOfFailedDeletedBlobs;

    public StorageBlobDeleteOperationStatusEvent(int numberOfSuccessfullyDeletedBlobs, int numberOfFailedDeletedBlobs) {
        this.numberOfSuccessfullyDeletedBlobs = numberOfSuccessfullyDeletedBlobs;
        this.numberOfFailedDeletedBlobs = numberOfFailedDeletedBlobs;
    }

    public int getNumberOfSuccessfullyDeletedBlobs() {
        return numberOfSuccessfullyDeletedBlobs;
    }

    public int getNumberOfFailedDeletedBlobs() {
        return numberOfFailedDeletedBlobs;
    }
}
