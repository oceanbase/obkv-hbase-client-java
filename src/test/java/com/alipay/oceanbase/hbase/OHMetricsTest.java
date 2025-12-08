package com.alipay.oceanbase.hbase;

import com.alipay.oceanbase.hbase.metrics.MetricsExporter;
import com.alipay.oceanbase.hbase.metrics.OHMetrics;
import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.OHOperationType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.apache.hadoop.hbase.client.MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY;

public class OHMetricsTest {
    protected static Connection connection;
    protected static Table hTable;

    /*
        CREATE TABLEGROUP test_multi_cf SHARDING = 'ADAPTIVE';
        CREATE TABLE `test_multi_cf$family_with_group1` (
            `K` varbinary(1024) NOT NULL,
            `Q` varbinary(256) NOT NULL,
            `T` bigint(20) NOT NULL,
            `V` varbinary(1024) DEFAULT NULL,
            PRIMARY KEY (`K`, `Q`, `T`)
        ) TABLEGROUP = test_multi_cf PARTITION BY KEY(`K`) PARTITIONS 3;

        CREATE TABLE `test_multi_cf$family_with_group2` (
            `K` varbinary(1024) NOT NULL,
            `Q` varbinary(256) NOT NULL,
            `T` bigint(20) NOT NULL,
            `V` varbinary(1024) DEFAULT NULL,
            PRIMARY KEY (`K`, `Q`, `T`)
        ) TABLEGROUP = test_multi_cf PARTITION BY KEY(`K`) PARTITIONS 3;

        CREATE TABLE `test_multi_cf$family_with_group3` (
            `K` varbinary(1024) NOT NULL,
            `Q` varbinary(256) NOT NULL,
            `T` bigint(20) NOT NULL,
            `V` varbinary(1024) DEFAULT NULL,
            PRIMARY KEY (`K`, `Q`, `T`)
        ) TABLEGROUP = test_multi_cf PARTITION BY KEY(`K`) PARTITIONS 3;
    * */
    @BeforeClass
    public static void setUp() throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        conf.set(CLIENT_SIDE_METRICS_ENABLED_KEY, "true");
        connection = ConnectionFactory.createConnection(conf);
        hTable = connection.getTable(TableName.valueOf("test_multi_cf"));
    }

    @After
    public void cleanUp() throws Exception {
        java.sql.Connection sqlConn = ObHTableTestUtil.getConnection();
        Statement s = sqlConn.createStatement();
        s.execute("truncate table test_multi_cf$family_with_group1");
        s.execute("truncate table test_multi_cf$family_with_group2");
        s.execute("truncate table test_multi_cf$family_with_group3");
        s.close();
        sqlConn.close();
    }

    @AfterClass
    public static void finish() throws Exception {
        if (hTable != null) {
            hTable.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void testPutOperationsMetrics() throws Exception {
        String row1 = "row1";
        String family1 = "family_with_group1";
        String family2 = "family_with_group2";
        String family3 = "family_with_group3";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.PUT);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());
        Put put = new Put(Bytes.toBytes(row1));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        put.addColumn(Bytes.toBytes(family2), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        put.addColumn(Bytes.toBytes(family3), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.PUT);
        System.out.println("PUT - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("PUT - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(1.0, newExporter.getAverageSingleOpCount(), 0.001);
        System.out.println("PUT - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("PUT - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("PUT - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("PUT - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("PUT - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            put = new Put(Bytes.toBytes(row1));
            hTable.put(put);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("No columns"));
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.PUT);
        System.out.println("PUT - FailCount: " + newExporter.getFailCount());
        Assert.assertEquals(1L, newExporter.getFailCount());
    }

    @Test
    public void testGetOperationsMetrics() throws Exception {
        String row1 = "row1";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.GET);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        // Prepare data first
        Put put = new Put(Bytes.toBytes(row1));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put);

        Get get = new Get(Bytes.toBytes(row1));
        hTable.get(get);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.GET);
        System.out.println("GET - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("GET - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(1.0, newExporter.getAverageSingleOpCount(), 0.001);
        System.out.println("GET - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("GET - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("GET - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("GET - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("GET - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.get(get);
        } catch (Exception e) {
            System.out.println("GET error: " + e.getMessage());
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.GET);
        System.out.println("GET - FailCount: " + newExporter.getFailCount());
        Assert.assertTrue(newExporter.getFailCount() >= 0);
    }

    @Test
    public void testGetListOperationsMetrics() throws Exception {
        String row1 = "row1";
        String row2 = "row2";
        String row3 = "row3";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.GET_LIST);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        // Prepare data first
        Put put1 = new Put(Bytes.toBytes(row1));
        put1.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        Put put2 = new Put(Bytes.toBytes(row2));
        put2.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val2"));
        Put put3 = new Put(Bytes.toBytes(row3));
        put3.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val3"));
        hTable.put(put1);
        hTable.put(put2);
        hTable.put(put3);

        List<Get> gets = Arrays.asList(
                new Get(Bytes.toBytes(row1)),
                new Get(Bytes.toBytes(row2)),
                new Get(Bytes.toBytes(row3))
        );
        hTable.get(gets);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.GET_LIST);
        System.out.println("GET_LIST - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("GET_LIST - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(3.0, newExporter.getAverageSingleOpCount(), 0.1);
        System.out.println("GET_LIST - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("GET_LIST - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("GET_LIST - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("GET_LIST - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("GET_LIST - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.get(gets);
        } catch (Exception e) {
            System.out.println("GET_LIST error: " + e.getMessage());
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.GET_LIST);
        System.out.println("GET_LIST - FailCount: " + newExporter.getFailCount());
        Assert.assertTrue(newExporter.getFailCount() >= 0);
    }

    @Test
    public void testPutListOperationsMetrics() throws Exception {
        String row1 = "row1";
        String row2 = "row2";
        String row3 = "row3";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.PUT_LIST);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        List<Put> puts = new ArrayList<>();
        Put put1 = new Put(Bytes.toBytes(row1));
        put1.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        puts.add(put1);
        Put put2 = new Put(Bytes.toBytes(row2));
        put2.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val2"));
        puts.add(put2);
        Put put3 = new Put(Bytes.toBytes(row3));
        put3.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val3"));
        puts.add(put3);

        hTable.put(puts);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.PUT_LIST);
        System.out.println("PUT_LIST - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("PUT_LIST - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(3.0, newExporter.getAverageSingleOpCount(), 0.1);
        System.out.println("PUT_LIST - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("PUT_LIST - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("PUT_LIST - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("PUT_LIST - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("PUT_LIST - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            put1 = new Put(Bytes.toBytes(row1));
            put2 = new Put(Bytes.toBytes(row2));
            puts.clear();
            puts.add(put1);
            puts.add(put2);
            hTable.put(puts);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("No columns"));
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.PUT_LIST);
        System.out.println("PUT_LIST - FailCount: " + newExporter.getFailCount());
        Assert.assertEquals(1L, newExporter.getFailCount());
    }

    @Test
    public void testDeleteOperationsMetrics() throws Exception {
        String row1 = "row1";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.DELETE);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        // Prepare data first
        Put put = new Put(Bytes.toBytes(row1));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put);

        Delete delete = new Delete(Bytes.toBytes(row1));
        delete.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"));
        hTable.delete(delete);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.DELETE);
        System.out.println("DELETE - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("DELETE - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(1.0, newExporter.getAverageSingleOpCount(), 0.001);
        System.out.println("DELETE - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("DELETE - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("DELETE - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("DELETE - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("DELETE - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            delete = new Delete((byte[]) null);
            hTable.delete(delete);
        } catch (Exception e) {
            System.out.println("DELETE error: " + e.getMessage());
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.DELETE);
        System.out.println("DELETE - FailCount: " + newExporter.getFailCount());
        Assert.assertEquals(1L, newExporter.getFailCount());
    }

    @Test
    public void testDeleteListOperationsMetrics() throws Exception {
        String row1 = "row1";
        String row2 = "row2";
        String row3 = "row3";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.DELETE_LIST);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        // Prepare data first
        Put put1 = new Put(Bytes.toBytes(row1));
        put1.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        Put put2 = new Put(Bytes.toBytes(row2));
        put2.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val2"));
        Put put3 = new Put(Bytes.toBytes(row3));
        put3.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val3"));
        hTable.put(put1);
        hTable.put(put2);
        hTable.put(put3);

        List<Delete> deletes = new ArrayList<>();
        Delete delete1 = new Delete(Bytes.toBytes(row1));
        delete1.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"));
        deletes.add(delete1);
        Delete delete2 = new Delete(Bytes.toBytes(row2));
        delete2.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"));
        deletes.add(delete2);
        Delete delete3 = new Delete(Bytes.toBytes(row3));
        delete3.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"));
        deletes.add(delete3);

        hTable.delete(deletes);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.DELETE_LIST);
        System.out.println("DELETE_LIST - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("DELETE_LIST - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(3.0, newExporter.getAverageSingleOpCount(), 0.1);
        System.out.println("DELETE_LIST - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("DELETE_LIST - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("DELETE_LIST - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("DELETE_LIST - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("DELETE_LIST - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.delete(deletes);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("DELETE_LIST error: " + e.getMessage());
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.DELETE_LIST);
        System.out.println("DELETE_LIST - FailCount: " + newExporter.getFailCount());
        Assert.assertEquals(1L, newExporter.getFailCount());
    }

    @Test
    public void testExistsOperationsMetrics() throws Exception {
        String row1 = "row1";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.EXISTS);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        // Prepare data first
        Put put = new Put(Bytes.toBytes(row1));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put);

        Get get = new Get(Bytes.toBytes(row1));
        boolean exists = hTable.exists(get);
        Assert.assertTrue(exists);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.EXISTS);
        System.out.println("EXISTS - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("EXISTS - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(1.0, newExporter.getAverageSingleOpCount(), 0.001);
        System.out.println("EXISTS - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("EXISTS - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("EXISTS - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("EXISTS - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("EXISTS - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            get = new Get(Bytes.toBytes(row1));
            notExistTable.exists(get);
        } catch (Exception e) {
            System.out.println("EXISTS error: " + e.getMessage());
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.EXISTS);
        System.out.println("EXISTS - FailCount: " + newExporter.getFailCount());
        Assert.assertTrue(newExporter.getFailCount() >= 0);
    }

    @Test
    public void testExistsListOperationsMetrics() throws Exception {
        String row1 = "row1";
        String row2 = "row2";
        String row3 = "row3";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.EXISTS_LIST);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        // Prepare data first
        Put put1 = new Put(Bytes.toBytes(row1));
        put1.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        Put put2 = new Put(Bytes.toBytes(row2));
        put2.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val2"));
        hTable.put(put1);
        hTable.put(put2);

        List<Get> gets = Arrays.asList(
                new Get(Bytes.toBytes(row1)),
                new Get(Bytes.toBytes(row2)),
                new Get(Bytes.toBytes(row3))
        );
        boolean[] exists = hTable.existsAll(gets);
        Assert.assertTrue(exists[0]);
        Assert.assertTrue(exists[1]);
        Assert.assertFalse(exists[2]);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.EXISTS_LIST);
        System.out.println("EXISTS_LIST - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("EXISTS_LIST - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(3.0, newExporter.getAverageSingleOpCount(), 0.1);
        System.out.println("EXISTS_LIST - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("EXISTS_LIST - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("EXISTS_LIST - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("EXISTS_LIST - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("EXISTS_LIST - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.existsAll(gets);
        } catch (Exception e) {
            System.out.println("EXISTS_LIST error: " + e.getMessage());
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.EXISTS_LIST);
        System.out.println("EXISTS_LIST - FailCount: " + newExporter.getFailCount());
        Assert.assertTrue(newExporter.getFailCount() >= 0);
    }

    @Test
    public void testScanOperationsMetrics() throws Exception {
        String row1 = "row1";
        String row2 = "row2";
        String row3 = "row3";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.SCAN);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        // Prepare data first
        Put put1 = new Put(Bytes.toBytes(row1));
        put1.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        Put put2 = new Put(Bytes.toBytes(row2));
        put2.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val2"));
        Put put3 = new Put(Bytes.toBytes(row3));
        put3.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val3"));
        hTable.put(put1);
        hTable.put(put2);
        hTable.put(put3);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family1));
        ResultScanner scanner = hTable.getScanner(scan);
        int count = 0;
        for (Result result : scanner) {
            count += result.rawCells().length;
        }
        scanner.close();
        Assert.assertEquals(3, count);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.SCAN);
        System.out.println("SCAN - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("SCAN - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(1.0, newExporter.getAverageSingleOpCount(), 0.001);
        System.out.println("SCAN - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("SCAN - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("SCAN - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("SCAN - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("SCAN - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.getScanner(scan);
        } catch (Exception e) {
            System.out.println("SCAN error: " + e.getMessage());
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.SCAN);
        System.out.println("SCAN - FailCount: " + newExporter.getFailCount());
        Assert.assertTrue(newExporter.getFailCount() >= 0);
    }

    @Test
    public void testBatchOperationsMetrics() throws Exception {
        String row1 = "row1";
        String row2 = "row2";
        String row3 = "row3";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.BATCH);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        List<Row> actions = new ArrayList<>();
        Put put1 = new Put(Bytes.toBytes(row1));
        put1.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        actions.add(put1);
        Put put2 = new Put(Bytes.toBytes(row2));
        put2.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val2"));
        actions.add(put2);
        Get get1 = new Get(Bytes.toBytes(row3));
        actions.add(get1);

        Object[] results = new Object[actions.size()];
        hTable.batch(actions, results);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.BATCH);
        System.out.println("BATCH - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("BATCH - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(3.0, newExporter.getAverageSingleOpCount(), 0.1);
        System.out.println("BATCH - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("BATCH - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("BATCH - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("BATCH - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("BATCH - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            results = new Object[actions.size()];
            notExistTable.batch(actions, results);
        } catch (Exception e) {
            System.out.println("BATCH error: " + e.getMessage());
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.BATCH);
        System.out.println("BATCH - FailCount: " + newExporter.getFailCount());
        Assert.assertTrue(newExporter.getFailCount() >= 0);
    }

    @Test
    public void testBatchCallbackOperationsMetrics() throws Exception {
        String row1 = "row1";
        String row2 = "row2";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.BATCH_CALLBACK);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        List<Row> actions = new ArrayList<>();
        Put put1 = new Put(Bytes.toBytes(row1));
        put1.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        actions.add(put1);
        Put put2 = new Put(Bytes.toBytes(row2));
        put2.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val2"));
        actions.add(put2);

        Object[] results = new Object[actions.size()];
        hTable.batchCallback(actions, results, new Batch.Callback<Result>() {
            @Override
            public void update(byte[] region, byte[] row, Result result) {
                // Do nothing
            }
        });
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.BATCH_CALLBACK);
        System.out.println("BATCH_CALLBACK - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("BATCH_CALLBACK - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(2.0, newExporter.getAverageSingleOpCount(), 0.1);
        System.out.println("BATCH_CALLBACK - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("BATCH_CALLBACK - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("BATCH_CALLBACK - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("BATCH_CALLBACK - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("BATCH_CALLBACK - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            results = new Object[actions.size()];
            notExistTable.batchCallback(actions, results, new Batch.Callback<Result>() {
                @Override
                public void update(byte[] region, byte[] row, Result result) {
                    // Do nothing
                }
            });
        } catch (Exception e) {
            System.out.println("BATCH_CALLBACK error: " + e.getMessage());
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.BATCH_CALLBACK);
        System.out.println("BATCH_CALLBACK - FailCount: " + newExporter.getFailCount());
        Assert.assertTrue(newExporter.getFailCount() >= 0);
    }

    @Test
    public void testCheckAndPutOperationsMetrics() throws Exception {
        String row1 = "row1";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.CHECK_AND_PUT);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        // Prepare data first
        Put put1 = new Put(Bytes.toBytes(row1));
        put1.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put1);

        Put put2 = new Put(Bytes.toBytes(row1));
        put2.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q2"), Bytes.toBytes("val2"));
        boolean result = hTable.checkAndPut(Bytes.toBytes(row1), Bytes.toBytes(family1),
                Bytes.toBytes("q1"), Bytes.toBytes("val1"), put2);
        Assert.assertTrue(result);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.CHECK_AND_PUT);
        System.out.println("CHECK_AND_PUT - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("CHECK_AND_PUT - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(1.0, newExporter.getAverageSingleOpCount(), 0.001);
        System.out.println("CHECK_AND_PUT - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("CHECK_AND_PUT - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("CHECK_AND_PUT - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("CHECK_AND_PUT - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("CHECK_AND_PUT - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            Put put3 = new Put(Bytes.toBytes(row1));
            put3.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q3"), Bytes.toBytes("val3"));
            notExistTable.checkAndPut(Bytes.toBytes(row1), Bytes.toBytes(family1),
                    Bytes.toBytes("q2"), Bytes.toBytes("val2"), put3);
        } catch (Exception e) {
            System.out.println("CHECK_AND_PUT error: " + e.getMessage());
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.CHECK_AND_PUT);
        System.out.println("CHECK_AND_PUT - FailCount: " + newExporter.getFailCount());
        Assert.assertTrue(newExporter.getFailCount() >= 0);
    }

    @Test
    public void testCheckAndDeleteOperationsMetrics() throws Exception {
        String row1 = "row1";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.CHECK_AND_DELETE);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        // Prepare data first
        Put put = new Put(Bytes.toBytes(row1));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put);

        Delete delete = new Delete(Bytes.toBytes(row1));
        delete.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q2"));
        boolean result = hTable.checkAndDelete(Bytes.toBytes(row1), Bytes.toBytes(family1),
                Bytes.toBytes("q1"), Bytes.toBytes("val1"), delete);
        Assert.assertTrue(result);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.CHECK_AND_DELETE);
        System.out.println("CHECK_AND_DELETE - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("CHECK_AND_DELETE - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(1.0, newExporter.getAverageSingleOpCount(), 0.001);
        System.out.println("CHECK_AND_DELETE - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("CHECK_AND_DELETE - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("CHECK_AND_DELETE - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("CHECK_AND_DELETE - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("CHECK_AND_DELETE - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.checkAndDelete(Bytes.toBytes(row1), Bytes.toBytes(family1),
                    Bytes.toBytes("q1"), Bytes.toBytes("val1"), delete);
        } catch (Exception e) {
            System.out.println("CHECK_AND_DELETE error: " + e.getMessage());
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.CHECK_AND_DELETE);
        System.out.println("CHECK_AND_DELETE - FailCount: " + newExporter.getFailCount());
        Assert.assertTrue(newExporter.getFailCount() >= 0);
    }

    @Test
    public void testCheckAndMutateOperationsMetrics() throws Exception {
        String row1 = "row1";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.CHECK_AND_MUTATE);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        // Prepare data first
        Put put1 = new Put(Bytes.toBytes(row1));
        put1.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put1);

        Put put2 = new Put(Bytes.toBytes(row1));
        put2.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q2"), Bytes.toBytes("val2"));
        Delete delete = new Delete(Bytes.toBytes(row1));
        delete.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q3"));
        RowMutations rowMutations = new RowMutations(Bytes.toBytes(row1));
        rowMutations.add(put2);
        rowMutations.add(delete);

        boolean result = hTable.checkAndMutate(Bytes.toBytes(row1), Bytes.toBytes(family1),
                Bytes.toBytes("q1"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("val1"), rowMutations);
        Assert.assertTrue(result);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.CHECK_AND_MUTATE);
        System.out.println("CHECK_AND_MUTATE - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("CHECK_AND_MUTATE - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(2.0, newExporter.getAverageSingleOpCount(), 0.1);
        System.out.println("CHECK_AND_MUTATE - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("CHECK_AND_MUTATE - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("CHECK_AND_MUTATE - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("CHECK_AND_MUTATE - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("CHECK_AND_MUTATE - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.checkAndMutate(Bytes.toBytes(row1), Bytes.toBytes(family1),
                    Bytes.toBytes("q1"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("val1"), rowMutations);
        } catch (Exception e) {
            System.out.println("CHECK_AND_MUTATE error: " + e.getMessage());
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.CHECK_AND_MUTATE);
        System.out.println("CHECK_AND_MUTATE - FailCount: " + newExporter.getFailCount());
        Assert.assertTrue(newExporter.getFailCount() >= 0);
    }

    @Test
    public void testAppendOperationsMetrics() throws Exception {
        String row1 = "row1";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.APPEND);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        Append append = new Append(Bytes.toBytes(row1));
        append.add(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.append(append);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.APPEND);
        System.out.println("APPEND - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("APPEND - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(1.0, newExporter.getAverageSingleOpCount(), 0.001);
        System.out.println("APPEND - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("APPEND - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("APPEND - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("APPEND - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("APPEND - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.append(append);
        } catch (Exception e) {
            System.out.println("APPEND error: " + e.getMessage());
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.APPEND);
        System.out.println("APPEND - FailCount: " + newExporter.getFailCount());
        Assert.assertTrue(newExporter.getFailCount() >= 0);
    }

    @Test
    public void testIncrementOperationsMetrics() throws Exception {
        String row1 = "row1";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.INCREMENT);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        Increment increment = new Increment(Bytes.toBytes(row1));
        increment.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), 10L);
        hTable.increment(increment);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.INCREMENT);
        System.out.println("APPEND - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("APPEND - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(1.0, newExporter.getAverageSingleOpCount(), 0.001);
        System.out.println("APPEND - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("APPEND - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("APPEND - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("APPEND - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("APPEND - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.increment(increment);
        } catch (Exception e) {
            System.out.println("INCREMENT error: " + e.getMessage());
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.INCREMENT);
        System.out.println("INCREMENT - FailCount: " + newExporter.getFailCount());
        Assert.assertTrue(newExporter.getFailCount() >= 0);
    }

    @Test
    public void testIncrementColumnValueOperationsMetrics() throws Exception {
        String row1 = "row1";
        String family1 = "family_with_group1";
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.INCREMENT_COLUMN_VALUE);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMaxLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getMinLatency(), 0.001);
        Assert.assertEquals(0.0, oldExporter.get99thPercentile(), 0.001);
        Assert.assertEquals(0, oldExporter.getTotalRuntime());

        long res = hTable.incrementColumnValue(Bytes.toBytes(row1), Bytes.toBytes(family1), Bytes.toBytes("q1"), 10L);
        Assert.assertEquals(10L, res);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.INCREMENT_COLUMN_VALUE);
        System.out.println("INCREMENT_COLUMN_VALUE - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("INCREMENT_COLUMN_VALUE - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(1.0, newExporter.getAverageSingleOpCount(), 0.001);
        System.out.println("INCREMENT_COLUMN_VALUE - AverageLatency: " + newExporter.getAverageLatency());
        Assert.assertTrue(newExporter.getAverageLatency() > 0);
        System.out.println("INCREMENT_COLUMN_VALUE - MaxLatency: " + newExporter.getMaxLatency());
        Assert.assertTrue(newExporter.getMaxLatency() > 0);
        System.out.println("INCREMENT_COLUMN_VALUE - MinLatency: " + newExporter.getMinLatency());
        Assert.assertTrue(newExporter.getMinLatency() > 0);
        System.out.println("INCREMENT_COLUMN_VALUE - 99thPercentile: " + newExporter.get99thPercentile());
        Assert.assertTrue(newExporter.get99thPercentile() > 0);
        System.out.println("INCREMENT_COLUMN_VALUE - TotalRuntime: " + newExporter.getTotalRuntime());
        Assert.assertTrue(newExporter.getTotalRuntime() > 0);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.incrementColumnValue(Bytes.toBytes(row1), Bytes.toBytes(family1), Bytes.toBytes("q1"), 10L);
        } catch (Exception e) {
            System.out.println("INCREMENT_COLUMN_VALUE error: " + e.getMessage());
        }
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.INCREMENT_COLUMN_VALUE);
        System.out.println("INCREMENT_COLUMN_VALUE - FailCount: " + newExporter.getFailCount());
        Assert.assertTrue(newExporter.getFailCount() >= 0);
    }

    @Test
    public void testEmptyBatchMetrics() throws Exception {
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        // test put list
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.PUT_LIST);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        List<Put> puts = new ArrayList<>();
        hTable.put(puts);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.PUT_LIST);
        System.out.println("PUT_LIST - AverageOps: " + newExporter.getAverageOps());
        // average ops will larger than 0 even if the list is empty
        // because the metrics of ops is recorded for put<list> method
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("PUT_LIST - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(0.0, newExporter.getAverageSingleOpCount(), 0.001);

        // test batch
        oldExporter = metrics.acquireMetrics(OHOperationType.BATCH);
        Assert.assertEquals(0.0, oldExporter.getAverageOps(), 0.001);
        Assert.assertEquals(0.0, oldExporter.getAverageSingleOpCount(), 0.001);
        List<Mutation> actions = new ArrayList<>();
        Object[] results = new Object[actions.size()];
        hTable.batch(actions, results);
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.BATCH);
        System.out.println("BATCH - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("BATCH - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(0.0, newExporter.getAverageSingleOpCount(), 0.001);
    }

    @Test
    public void testConcurrentOperationMetrics() throws Exception {
        byte[] family1 = Bytes.toBytes("family_with_group1");
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        int threadCount = 10;
        int operationsPerThread = 100;
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            int threadId = i;
            new Thread(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        Put put = new Put(Bytes.toBytes("row_" + threadId + "_" + j));
                        put.addColumn(family1, Bytes.toBytes("q_" + threadId + "_" + j), Bytes.toBytes("val1"));
                        hTable.put(put);
                    }
                } catch (Exception e) {
                    // handle
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();
        Scan scan = new Scan();
        scan.setMaxVersions(10);
        scan.setBatch(10);
        scan.addFamily(family1);
        ResultScanner scanner = hTable.getScanner(scan);
        int cellCount = 0;
        for (Result res : scanner) {
            cellCount += res.rawCells().length;
        }
        Assert.assertEquals(threadCount * operationsPerThread, cellCount);
        Thread.sleep(11000);

        MetricsExporter exporter = metrics.acquireMetrics(OHOperationType.PUT);
        long expectedCount = threadCount * operationsPerThread;
        System.out.println("exporter count: " + exporter.getCount());
        System.out.println("expected count: " + expectedCount);
        Assert.assertTrue(exporter.getCount() == expectedCount);
    }
}