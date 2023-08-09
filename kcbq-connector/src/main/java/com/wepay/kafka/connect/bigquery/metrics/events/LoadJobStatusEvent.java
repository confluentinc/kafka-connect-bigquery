package com.wepay.kafka.connect.bigquery.metrics.events;

public class LoadJobStatusEvent implements MetricEvent {

    private final int numberOfActiveLoadJobs;
    private final int numberOfSuccessfulLoadJobs;
    private final int numberOfFailedLoadJobs;

    public LoadJobStatusEvent(int numberOfActiveLoadJobs, int numberOfSuccessfulLoadJobs, int numberOfFailedLoadJobs) {
        this.numberOfActiveLoadJobs = numberOfActiveLoadJobs;
        this.numberOfSuccessfulLoadJobs = numberOfSuccessfulLoadJobs;
        this.numberOfFailedLoadJobs = numberOfFailedLoadJobs;
    }

    public int getNumberOfActiveLoadJobs() {
        return numberOfActiveLoadJobs;
    }

    public int getNumberOfSuccessfulLoadJobs() {
        return numberOfSuccessfulLoadJobs;
    }

    public int getNumberOfFailedLoadJobs() {
        return numberOfFailedLoadJobs;
    }
}
