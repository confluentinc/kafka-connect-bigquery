package com.wepay.kafka.connect.bigquery.metrics.jmx;

import com.wepay.kafka.connect.bigquery.metrics.Metrics;
import com.wepay.kafka.connect.bigquery.metrics.MetricsEventPublisher;
import com.wepay.kafka.connect.bigquery.metrics.events.*;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BqSinkConnectorMetricsImpl extends Metrics implements BqSinkConnectorMetricsMXBean {

    private final AtomicInteger numberOfActiveLoadJobs = new AtomicInteger(0);
    private final AtomicInteger numberOfSuccessfulLoadJobs = new AtomicInteger(0);
    private final AtomicInteger numberOfFailedLoadJobs = new AtomicInteger(0);

    // BQ-Loader Run status events
    private final AtomicInteger successfulBQLoaderRunCounter = new AtomicInteger(0);
    private final AtomicInteger failedBQLoaderRunCounter = new AtomicInteger(0);

    // storage blobs status events
    private final AtomicLong totalNumberOfBlobsInStorage = new AtomicLong(0);
    private final AtomicInteger claimedNumberOfBlobsInStorage = new AtomicInteger(0);
    private final AtomicInteger deletableNumberOfBlobsInStorage = new AtomicInteger(0);

    // storage blob delete operation status event
    private final AtomicInteger numberOfSuccessfullyDeletedStorageBlobs = new AtomicInteger(0);
    private final AtomicInteger numberOfFailedDeletedStorageBlobs = new AtomicInteger(0);

    // storage exception counter
    private final AtomicInteger storageExceptionCounter = new AtomicInteger(0);

    private final MetricsEventPublisher metricsEventPublisher;

    public BqSinkConnectorMetricsImpl(String taskId) {
        super(taskId);

        this.metricsEventPublisher = new MetricsEventPublisher();

        metricsEventPublisher.subscribe(LoadJobStatusEvent.class, event -> {
            numberOfActiveLoadJobs.set(event.getNumberOfActiveLoadJobs());
            numberOfSuccessfulLoadJobs.set(event.getNumberOfSuccessfulLoadJobs());
            numberOfFailedLoadJobs.set(event.getNumberOfFailedLoadJobs());
        });

        metricsEventPublisher.subscribe(BQLoaderRunStatusEvent.class, event -> {
            if (event.isSuccessfulBQLoaderRun()) {
                successfulBQLoaderRunCounter.incrementAndGet();
            } else {
                failedBQLoaderRunCounter.incrementAndGet();
            }
        });

        metricsEventPublisher.subscribe(StorageBlobsStatusEvent.class, event -> {
            totalNumberOfBlobsInStorage.set(event.getTotalNumberOfBlobsInStorage());
            claimedNumberOfBlobsInStorage.set(event.getClaimedNumberOfBlobsInStorage());
            deletableNumberOfBlobsInStorage.set(event.getDeletableNumberOfBlobsInStorage());
        });

        metricsEventPublisher.subscribe(StorageBlobDeleteOperationStatusEvent.class, event -> {
            numberOfSuccessfullyDeletedStorageBlobs.set(event.getNumberOfSuccessfullyDeletedBlobs());
            numberOfFailedDeletedStorageBlobs.set(event.getNumberOfFailedDeletedBlobs());
        });

        metricsEventPublisher.subscribe(StorageExceptionCountEvent.class, event -> {
            storageExceptionCounter.incrementAndGet();
        });
    }

    public MetricsEventPublisher getMetricsEventPublisher() {
        return this.metricsEventPublisher;
    }

    public int getNumberOfActiveLoadJobs() {
        return numberOfActiveLoadJobs.get();
    }

    public int getNumberOfSuccessfulLoadJobs() {
        return numberOfSuccessfulLoadJobs.get();
    }

    public int getNumberOfFailedLoadJobs() {
        return numberOfFailedLoadJobs.get();
    }

    public int getSuccessfulBQLoaderRunCounter() {
        return successfulBQLoaderRunCounter.get();
    }

    public int getFailedBQLoaderRunCounter() {
        return failedBQLoaderRunCounter.get();
    }

    public long getTotalNumberOfBlobsInStorage() {
        return totalNumberOfBlobsInStorage.get();
    }

    public int getClaimedNumberOfBlobsInStorage() {
        return claimedNumberOfBlobsInStorage.get();
    }

    public int getDeletableNumberOfBlobsInStorage() {
        return deletableNumberOfBlobsInStorage.get();
    }

    public int getNumberOfSuccessfullyDeletedStorageBlobs() {
        return numberOfSuccessfullyDeletedStorageBlobs.get();
    }

    public int getNumberOfFailedDeletedStorageBlobs() {
        return numberOfFailedDeletedStorageBlobs.get();
    }

    public int getStorageExceptionCounter() {
        return storageExceptionCounter.get();
    }
}
