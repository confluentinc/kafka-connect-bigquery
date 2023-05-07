package com.wepay.kafka.connect.bigquery.metrics.jmx;

import javax.management.MXBean;

@MXBean
public interface BqSinkConnectorMetricsMXBean {

    int getNumberOfActiveLoadJobs();

    int getNumberOfSuccessfulLoadJobs();

    int getNumberOfFailedLoadJobs();

    int getSuccessfulBQLoaderRunCounter();

    int getFailedBQLoaderRunCounter();

    long getTotalNumberOfBlobsInStorage();

    int getClaimedNumberOfBlobsInStorage();

    int getDeletableNumberOfBlobsInStorage();

    int getNumberOfSuccessfullyDeletedStorageBlobs();

    int getNumberOfFailedDeletedStorageBlobs();

    int getStorageExceptionCounter();
}
