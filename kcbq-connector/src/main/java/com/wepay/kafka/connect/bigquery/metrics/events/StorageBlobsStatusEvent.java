package com.wepay.kafka.connect.bigquery.metrics.events;

public class StorageBlobsStatusEvent implements MetricEvent {

    private final long totalNumberOfBlobsInStorage;
    private final int claimedNumberOfBlobsInStorage;
    private final int deletableNumberOfBlobsInStorage;

    public StorageBlobsStatusEvent(long totalNumberOfBlobsInStorage, int claimedNumberOfBlobsInStorage, int deletableNumberOfBlobsInStorage) {
        this.totalNumberOfBlobsInStorage = totalNumberOfBlobsInStorage;
        this.claimedNumberOfBlobsInStorage = claimedNumberOfBlobsInStorage;
        this.deletableNumberOfBlobsInStorage = deletableNumberOfBlobsInStorage;
    }

    public long getTotalNumberOfBlobsInStorage() {
        return totalNumberOfBlobsInStorage;
    }

    public int getClaimedNumberOfBlobsInStorage() {
        return claimedNumberOfBlobsInStorage;
    }

    public int getDeletableNumberOfBlobsInStorage() {
        return deletableNumberOfBlobsInStorage;
    }
}
