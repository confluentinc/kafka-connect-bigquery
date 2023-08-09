package com.wepay.kafka.connect.bigquery.metrics;

import io.debezium.annotation.ThreadSafe;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.management.ManagementFactory;

@ThreadSafe
public abstract class Metrics {
    private static final Logger LOGGER = LoggerFactory.getLogger(Metrics.class);
    private final ObjectName name;
    private volatile boolean registered = false;

    protected Metrics(String taskId) {
        this.name = this.metricName(taskId);
    }

    public synchronized void register() {
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mBeanServer == null) {
                LOGGER.info("JMX not supported, bean '{}' not registered", this.name);
            } else {
                if (!mBeanServer.isRegistered(this.name)) {
                    // StandardMBean mbean = new StandardMBean(bqSinkConnectorMetrics, BqSinkConnectorMetricsMXBean.class);
                    try {
                        mBeanServer.registerMBean(this, this.name);
                    } catch (InstanceAlreadyExistsException ex1) {
                        LOGGER.error("Failed to register metrics MBean, as an old set with the same name exists, metrics will not be available", ex1);
                    } catch (MBeanRegistrationException ex2) {
                        LOGGER.error("Failed to register metrics MBean, metrics will not be available", ex2);
                    }
                }
                this.registered = true;
            }
        } catch (NotCompliantMBeanException ex) {
            LOGGER.error("Failed to create Standard MBean, metrics will not be available", ex);
        }
    }

    public synchronized void unregister() {
        if (this.name != null && this.registered) {
            try {
                MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                if (mBeanServer == null) {
                    LOGGER.debug("JMX not supported, bean '{}' not registered", this.name);
                    return;
                }

                try {
                    mBeanServer.unregisterMBean(this.name);
                } catch (InstanceNotFoundException var3) {
                    LOGGER.info("Unable to unregister metrics MBean '{}' as it was not found", this.name);
                }

                this.registered = false;
            } catch (JMException var4) {
                throw new RuntimeException("Unable to unregister the MBean '" + this.name + "'", var4);
            }
        }
    }

    protected ObjectName metricName(String taskId) {
        String metricName = "kafka.connect:type=bqsink-connector-metrics,taskid=" + taskId;
        try {
            return new ObjectName(metricName);
        } catch (MalformedObjectNameException var5) {
            throw new ConnectException("Invalid metric name '" + metricName + "'");
        }
    }
}
