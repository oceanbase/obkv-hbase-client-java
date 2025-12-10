package com.alipay.oceanbase.hbase;

import com.alipay.oceanbase.hbase.util.JMXMetricsTestHelper;
import com.alipay.oceanbase.hbase.metrics.MetricsExporter;
import com.alipay.oceanbase.hbase.metrics.OHMetrics;
import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.OHOperationType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.sql.Connection;
import java.sql.Statement;

import static org.apache.hadoop.hbase.client.MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY;

/**
 * test JMX indicators registered in OHMetrics and OHMetricTracker
 */
public class JMXMetricsTest {
    private static org.apache.hadoop.hbase.client.Connection connection;
    private static Table hTable;
    private static JMXMetricsTestHelper jmxHelper;
    private static String metricsName;
    private static OHMetrics metrics;

    @BeforeClass
    public static void setUp() throws Exception {
        jmxHelper = new JMXMetricsTestHelper();
        Configuration config = ObHTableTestUtil.newConfiguration();
        config.set(CLIENT_SIDE_METRICS_ENABLED_KEY, "true");
        connection = ConnectionFactory.createConnection(config);
        hTable = connection.getTable(TableName.valueOf("test_multi_cf"));
        if (hTable instanceof OHTable) {
            metrics = ((OHTable) hTable).getMetrics();
            if (metrics != null) {
                metricsName = metrics.getMetricsName();
            }
        }

        if (metrics == null) {
            System.out.println("Metrics not enabled, skipping JMX test");
        }
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        Connection conn = ObHTableTestUtil.getConnection();
        Statement statement = conn.createStatement();
        statement.execute("truncate table test_multi_cf$family_with_group1");
        statement.execute("truncate table test_multi_cf$family_with_group2");
        statement.execute("truncate table test_multi_cf$family_with_group3");
        statement.close();
        conn.close();
        hTable.close();
        connection.close();
    }

    /**
     * test if all indicators have been registered to JMX
     */
    @Test
    public void testMetricsRegisteredInJMX() throws Exception {
        if (metrics == null) {
            Assert.assertTrue("for testing Metrics in JMX, set CLIENT_SIDE_METRICS_ENABLED_KEY", false);
        }
        // print all registered metrics items
        jmxHelper.printAllOHMetricsMBeans();

        for (OHOperationType type : OHOperationType.values()) {
            if (type.getValue() == OHOperationType.INVALID.getValue()) {
                continue;
            }
            jmxHelper.assertAllMetricsRegistered(type, metricsName);
        }
    }

    /**
     * test PUT metrics in JMX
     */
    @Test
    public void testPutMetricsViaJMX() throws Exception {
        if (metrics == null) {
            Assert.assertTrue("for testing Metrics in JMX, set CLIENT_SIDE_METRICS_ENABLED_KEY", false);
        }
        
        String row1 = "jmx_test_row1";
        String family1 = "family_with_group1";

        JMXMetricsTestHelper.JMXMetricsInfo initialInfo = 
            jmxHelper.getMetricsInfo(OHOperationType.PUT, metricsName);

        Put put = new Put(Bytes.toBytes(row1));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put);

        Thread.sleep(11000);

        JMXMetricsTestHelper.JMXMetricsInfo updatedInfo = 
            jmxHelper.getMetricsInfo(OHOperationType.PUT, metricsName);
        
        JMXChecker(OHOperationType.PUT, initialInfo, updatedInfo);
    }

    /**
     * test GET metrics in JMX
     */
    @Test
    public void testGetMetricsViaJMX() throws Exception {
        if (metrics == null) {
            Assert.assertTrue("for testing Metrics in JMX, set CLIENT_SIDE_METRICS_ENABLED_KEY", false);
        }
        
        String row1 = "jmx_test_row2";
        String family1 = "family_with_group1";

        Put put = new Put(Bytes.toBytes(row1));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put);

        JMXMetricsTestHelper.JMXMetricsInfo initialInfo = 
            jmxHelper.getMetricsInfo(OHOperationType.GET, metricsName);

        Get get = new Get(Bytes.toBytes(row1));
        hTable.get(get);

        Thread.sleep(11000);

        JMXMetricsTestHelper.JMXMetricsInfo updatedInfo = 
            jmxHelper.getMetricsInfo(OHOperationType.GET, metricsName);

        JMXChecker(OHOperationType.GET, initialInfo, updatedInfo);
    }

    /**
     * test failure metrics in JMX
     */
    @Test
    public void testFailedOperationMetricsViaJMX() throws Exception {
        if (metrics == null) {
            Assert.assertTrue("for testing Metrics in JMX, set CLIENT_SIDE_METRICS_ENABLED_KEY", false);
        }

        Long oldFailedOpCount = (Long) jmxHelper.getMeterAttribute(OHOperationType.PUT, metricsName, "Count");
        Double oldMeanFailRate = (Double) jmxHelper.getMeterAttribute(OHOperationType.PUT, metricsName, "MeanRate");
        Assert.assertTrue("Failed operation count should be 0",
                oldFailedOpCount == 0);
        Assert.assertTrue("Failed operation mean rate should be 0",
                oldMeanFailRate == 0);

        try {
            Put put = new Put(Bytes.toBytes("test_row"));
            hTable.put(put);
        } catch (Exception e) {
            System.out.println("Expected exception: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("No columns"));
        }

        Thread.sleep(11000);

        Long newFailedOpCount = (Long) jmxHelper.getMeterAttribute(OHOperationType.PUT, metricsName, "Count");
        Double newMeanFailRate = (Double) jmxHelper.getMeterAttribute(OHOperationType.PUT, metricsName, "MeanRate");
        
        System.out.println("Failed operation - Initial fail count: " + oldFailedOpCount);
        System.out.println("Failed operation - Updated fail count: " + newFailedOpCount);

        Assert.assertTrue("Failed operation count should increase",
                newFailedOpCount > oldFailedOpCount);
        Assert.assertTrue("Failed operation mean rate should be > 0",
                newMeanFailRate > 0);
    }

    /**
     * test PUT latency histogram
     */
    @Test
    public void testJMXLatencyHistogramAccess() throws Exception {
        if (metrics == null) {
            Assert.assertTrue("for testing Metrics in JMX, set CLIENT_SIDE_METRICS_ENABLED_KEY", false);
        }
        
        String row1 = "jmx_test_row3";
        String family1 = "family_with_group1";

        Put put = new Put(Bytes.toBytes(row1));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put);
        
        Thread.sleep(11000);

        Long count = (Long) jmxHelper.getTimerAttribute(
            OHOperationType.PUT, metricsName, "Count");
        Double averageLatency = (Double) jmxHelper.getTimerAttribute(
            OHOperationType.PUT, metricsName, "MeanRate");
        Double p99 = (Double) jmxHelper.getTimerAttribute(
            OHOperationType.PUT, metricsName, "99thPercentile");
        
        System.out.println("JMX Latency access:");
        System.out.println("  Count: " + count);
        System.out.println("  MeanRate: " + averageLatency);
        System.out.println("  99thPercentile: " + p99);
        
        Assert.assertTrue("Count should be > 0", count > 0);
        Assert.assertTrue("averageLatency should be > 0", averageLatency > 0);
        Assert.assertTrue("99thPercentile should be > 0", p99 > 0);
    }

    /**
     * test PUT Counter indicator, totalSingleOpCount and totalRuntime
     */
    @Test
    public void testJMXCounterAccess() throws Exception {
        if (metrics == null) {
            Assert.assertTrue("for testing Metrics in JMX, set CLIENT_SIDE_METRICS_ENABLED_KEY", false);
        }
        
        String row1 = "jmx_test_row4";
        String family1 = "family_with_group1";

        Long initialSingleOpCount = (Long) jmxHelper.getCounterAttribute(
            OHOperationType.PUT, metricsName, "totalSingleOpCount", "Count");
        Long initialRuntime = (Long) jmxHelper.getCounterAttribute(
            OHOperationType.PUT, metricsName, "totalRuntime", "Count");

        Put put = new Put(Bytes.toBytes(row1));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put);
        
        Thread.sleep(11000);

        Long updatedSingleOpCount = (Long) jmxHelper.getCounterAttribute(
            OHOperationType.PUT, metricsName, "totalSingleOpCount", "Count");
        Long updatedRuntime = (Long) jmxHelper.getCounterAttribute(
            OHOperationType.PUT, metricsName, "totalRuntime", "Count");
        
        System.out.println("Counter metrics:");
        System.out.println("  totalSingleOpCount: " + initialSingleOpCount + " -> " + updatedSingleOpCount);
        System.out.println("  totalRuntime: " + initialRuntime + " -> " + updatedRuntime);
        
        Assert.assertTrue("totalSingleOpCount should increase", 
            updatedSingleOpCount > initialSingleOpCount);
        Assert.assertTrue("totalRuntime should increase", 
            updatedRuntime > initialRuntime);
    }

    /**
     * test if the indicators acquired by MetricsExporter are the same with the ones from JMX
     */
    @Test
    public void testMetricsExporterVSJMX() throws Exception {
        if (metrics == null) {
            Assert.assertTrue("for testing Metrics in JMX, set CLIENT_SIDE_METRICS_ENABLED_KEY", false);
        }
        
        String row1 = "jmx_test_row5";
        String family1 = "family_with_group1";

        Put put = new Put(Bytes.toBytes(row1));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put);
        
        Thread.sleep(11000);

        MetricsExporter exporter = metrics.acquireMetrics(OHOperationType.PUT);
        long exporterCount = exporter.getCount();
        double exporterMeanRate = exporter.getAverageOps();
        double exporterMean = exporter.getAverageLatency();
        double exporterP99 = exporter.get99thPercentile();
        long exporterFailCount = exporter.getFailCount();

        JMXMetricsTestHelper.JMXMetricsInfo jmxInfo = 
            jmxHelper.getMetricsInfo(OHOperationType.PUT, metricsName);
        
        System.out.println("MetricsExporter vs JMX:");
        System.out.println("  Count: " + exporterCount + " vs " + jmxInfo.totalOpCount);
        System.out.println("  MeanRate: " + exporterMeanRate + " vs " + jmxInfo.meanOps);
        System.out.println("  Mean: " + exporterMean + " vs " + jmxInfo.meanLatency);
        System.out.println("  99thPercentile: " + exporterP99 + " vs " + jmxInfo.P99thPercentile);
        System.out.println("  FailCount: " + exporterFailCount + " vs " + jmxInfo.failedOpCount);

        Assert.assertEquals("Count should match", exporterCount, jmxInfo.totalOpCount);
        Assert.assertEquals("MeanRate should match", exporterMeanRate, jmxInfo.meanOps, 0.001);
        Assert.assertEquals("Mean should match", exporterMean, jmxInfo.meanLatency, 0.001);
        Assert.assertEquals("99thPercentile should match", exporterP99, jmxInfo.P99thPercentile, 0.001);
        Assert.assertEquals("FailCount should match", exporterFailCount, jmxInfo.failedOpCount);
    }

    private void JMXChecker(OHOperationType type,
                            JMXMetricsTestHelper.JMXMetricsInfo oldInfo,
                            JMXMetricsTestHelper.JMXMetricsInfo newInfo) throws Exception {
        System.out.printf("%s Initial metrics: " + oldInfo + "%n", type.name());
        System.out.printf("%s Updated metrics: " + newInfo + "%n", type.name());

        Assert.assertTrue(String.format("%s count should be greater", type.name()),
                newInfo.totalOpCount > oldInfo.totalOpCount);
        Assert.assertTrue(String.format("%s mean ops should be > 0", type.name()),
                newInfo.meanOps >= oldInfo.meanOps);
        Assert.assertTrue(String.format("%s mean latency should be > 0", type.name()),
                newInfo.meanLatency > 0);
        Assert.assertTrue(String.format("%s latency 99th percentile should be > 0", type.name()),
                newInfo.P99thPercentile > 0);
        Assert.assertTrue(String.format("%s total single op count should be greater", type.name()),
                newInfo.totalSingleOpCount > oldInfo.totalSingleOpCount);
    }
}

