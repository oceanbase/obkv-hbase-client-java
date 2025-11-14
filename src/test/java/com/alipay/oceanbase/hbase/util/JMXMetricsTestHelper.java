/*-
 * #%L
 * com.oceanbase:obkv-hbase-client
 * %%
 * Copyright (C) 2022 - 2025 OceanBase Group
 * %%
 * OBKV HBase Client Framework  is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

package com.alipay.oceanbase.hbase.util;

import com.alipay.oceanbase.hbase.metrics.OHMetrics;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.OHOperationType;
import org.junit.Assert;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.Set;

/**
 * JMX Metrics Test Helper
 */
public class JMXMetricsTestHelper {

    private static final String JMX_DOMAIN = "com.oceanbase.hbase.metrics";
    private final MBeanServer   mBeanServer;

    public JMXMetricsTestHelper() {
        this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    /**
     * acquire the ObjectName of a specific attribute in JMX
     * 
     * @param opType OHOperationType
     * @param metricsName metrics name
     * @param attributeName attribute name (latencyHistogram, failedOpCounter, totalSingleOpCount, totalRuntime)
     * @return ObjectName
     */
    public ObjectName getObjectName(OHOperationType opType, String metricsName, String attributeName) {
        try {
            // the format of JMX name: domain:name=metricName
            // example: com.alipay.oceanbase.hbase.util.OHMetrics.PUT.latencyHistogram.metricsName
            String name = String.format("%s.%s.%s.%s", OHMetrics.class.getName(), opType.name(),
                attributeName, metricsName);

            String objectNameStr = String.format("%s:name=%s", JMX_DOMAIN, name);
            return new ObjectName(objectNameStr);
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException("Failed to create ObjectName", e);
        }
    }

    /**
     * acquire latency attribute
     * Timer attributes: Count, MeanRate, OneMinuteRate, FiveMinuteRate, FifteenMinuteRate
     * Min, Max, Mean, StdDev, 50thPercentile, 75thPercentile, 95thPercentile, 98thPercentile, 99thPercentile, 999thPercentile
     */
    public Object getTimerAttribute(OHOperationType opType, String metricsName, String attributeName) {
        try {
            ObjectName objectName = getObjectName(opType, metricsName, "latencyHistogram");
            return mBeanServer.getAttribute(objectName, attributeName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Timer attribute: " + attributeName, e);
        }
    }

    /**
     * acquire failed operations attribute
     * Meter attributes: Count, MeanRate, OneMinuteRate, FiveMinuteRate, FifteenMinuteRate
     */
    public Object getMeterAttribute(OHOperationType opType, String metricsName, String attributeName) {
        try {
            ObjectName objectName = getObjectName(opType, metricsName, "failedOpCounter");
            return mBeanServer.getAttribute(objectName, attributeName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Meter attribute: " + attributeName, e);
        }
    }

    /**
     * acquire Counter attribute
     * Counter attributes: Count
     */
    public Object getCounterAttribute(OHOperationType opType, String metricsName,
                                      String counterName, String attributeName) {
        try {
            ObjectName objectName = getObjectName(opType, metricsName, counterName);
            return mBeanServer.getAttribute(objectName, attributeName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Counter attribute: " + attributeName, e);
        }
    }

    /**
     * verify latency attributes
     */
    public void assertTimerExists(OHOperationType opType, String metricsName) {
        try {
            ObjectName objectName = getObjectName(opType, metricsName, "latencyHistogram");
            Assert.assertTrue("Timer should be registered in JMX",
                mBeanServer.isRegistered(objectName));
        } catch (Exception e) {
            Assert.fail("Failed to check Timer existence: " + e.getMessage());
        }
    }

    /**
     * verify failed operation attributes
     */
    public void assertMeterExists(OHOperationType opType, String metricsName) {
        try {
            ObjectName objectName = getObjectName(opType, metricsName, "failedOpCounter");
            Assert.assertTrue("Meter should be registered in JMX",
                mBeanServer.isRegistered(objectName));
        } catch (Exception e) {
            Assert.fail("Failed to check Meter existence: " + e.getMessage());
        }
    }

    /**
     * verify Counter attributes
     */
    public void assertCounterExists(OHOperationType opType, String metricsName, String counterName) {
        try {
            ObjectName objectName = getObjectName(opType, metricsName, counterName);
            Assert.assertTrue("Counter should be registered in JMX",
                mBeanServer.isRegistered(objectName));
        } catch (Exception e) {
            Assert.fail("Failed to check Counter existence: " + e.getMessage());
        }
    }

    /**
     * get all registered OHMetrics MBean
     */
    public Set<ObjectName> getAllOHMetricsMBeans() {
        try {
            ObjectName pattern = new ObjectName(JMX_DOMAIN + ":*");
            return mBeanServer.queryNames(pattern, null);
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException("Failed to query MBeans", e);
        }
    }

    /**）
     * print all registered OHMetrics MBean info
     */
    public void printAllOHMetricsMBeans() {
        Set<ObjectName> mBeans = getAllOHMetricsMBeans();
        System.out.println("=== All OHMetrics MBeans ===");
        for (ObjectName objectName : mBeans) {
            System.out.println("MBean: " + objectName.getCanonicalName());
            try {
                MBeanInfo mBeanInfo = mBeanServer.getMBeanInfo(objectName);
                System.out.println("  Attributes:");
                for (MBeanAttributeInfo attr : mBeanInfo.getAttributes()) {
                    try {
                        Object value = mBeanServer.getAttribute(objectName, attr.getName());
                        System.out.println("    " + attr.getName() + " = " + value);
                    } catch (Exception e) {
                        System.out.println("    " + attr.getName() + " = (error: " + e.getMessage()
                                           + ")");
                    }
                }
            } catch (Exception e) {
                System.out.println("  (Failed to get MBeanInfo: " + e.getMessage() + ")");
            }
            System.out.println();
        }
    }

    public void assertAllMetricsRegistered(OHOperationType opType, String metricsName) {
        assertTimerExists(opType, metricsName);
        assertMeterExists(opType, metricsName);
        assertCounterExists(opType, metricsName, "totalSingleOpCount");
        assertCounterExists(opType, metricsName, "totalRuntime");
    }

    /**
     * get the entire metrics info
     */
    public JMXMetricsInfo getMetricsInfo(OHOperationType opType, String metricsName) {
        JMXMetricsInfo info = new JMXMetricsInfo();

        // latency histogram
        info.totalOpCount = (Long) getTimerAttribute(opType, metricsName, "Count");
        info.meanOps = (Double) getTimerAttribute(opType, metricsName, "MeanRate");
        info.oneMinuteRateOps = (Double) getTimerAttribute(opType, metricsName, "OneMinuteRate");
        info.meanLatency = (Double) getTimerAttribute(opType, metricsName, "Mean");
        info.minLatency = (Double) getTimerAttribute(opType, metricsName, "Min");
        info.maxLatency = (Double) getTimerAttribute(opType, metricsName, "Max");
        info.P99thPercentile = (Double) getTimerAttribute(opType, metricsName, "99thPercentile");

        // failure operations
        info.failedOpCount = (Long) getMeterAttribute(opType, metricsName, "Count");
        info.meanFailRate = (Double) getMeterAttribute(opType, metricsName, "MeanRate");
        info.oneMinuteFailRate = (Double) getMeterAttribute(opType, metricsName, "OneMinuteRate");

        // Counter
        info.totalSingleOpCount = (Long) getCounterAttribute(opType, metricsName,
            "totalSingleOpCount", "Count");
        info.totalRuntime = (Long) getCounterAttribute(opType, metricsName, "totalRuntime", "Count");

        return info;
    }

    /**
     * JMX info carrier
     */
    public static class JMXMetricsInfo {
        // Latency attributes
        public long   totalOpCount;
        public double meanOps;
        public double oneMinuteRateOps;
        public double meanLatency;
        public double minLatency;
        public double maxLatency;
        public double P99thPercentile;

        // Failure operation attributes
        public long   failedOpCount;
        public double meanFailRate;
        public double oneMinuteFailRate;

        // singleOpCount and totalRuntime
        public long   totalSingleOpCount;
        public long   totalRuntime;

        @Override
        public String toString() {
            return String
                .format(
                    "JMXMetricsInfo: { totalOpCount=%d, meanOps=%.2f, meanLatency=%.2f, "
                            + "P99thPercentile=%.2f, failedOpCount=%d, totalSingleOpCount=%d, totalRuntime=%d }",
                    totalOpCount, meanOps, meanLatency, P99thPercentile, failedOpCount,
                    totalSingleOpCount, totalRuntime);
        }
    }
}
