package com.wepay.kafka.connect.bigquery.metrics;

import com.wepay.kafka.connect.bigquery.metrics.events.MetricEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class MetricsEventPublisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsEventPublisher.class);
    private final Map<Class<? extends MetricEvent>, Consumer<? extends MetricEvent>> subscribes = new ConcurrentHashMap<>();

    public <T extends MetricEvent> void publishMetricEvent(T metricEvent) {
        Consumer<T> consumer = (Consumer<T>) subscribes.get(metricEvent.getClass());
        if (consumer != null) {
            try {
                consumer.accept(metricEvent);
            } catch (Exception ex) {
                LOGGER.warn("Failed to process metric event: " + metricEvent, ex);
            }
        }
    }

    public <T extends MetricEvent> void subscribe(Class<T> clazz, Consumer<T> consumer) {
        if (subscribes.containsKey(clazz)) {
            throw new IllegalStateException();
        }
        subscribes.put(clazz, consumer);
    }
}
