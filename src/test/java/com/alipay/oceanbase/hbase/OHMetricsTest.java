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
import static org.junit.Assert.fail;

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

        Put put = new Put(Bytes.toBytes(row1));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        put.addColumn(Bytes.toBytes(family2), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        put.addColumn(Bytes.toBytes(family3), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put);

        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.PUT);
        metricsChecker(OHOperationType.PUT, oldExporter, newExporter);

        // test failed op metrics
        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            put = new Put(Bytes.toBytes(row1));
            hTable.put(put);
            fail();
        } catch (Exception e) {
            System.out.println("PUT error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("No columns"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.PUT);
        failureMetricsChecker(OHOperationType.PUT, newExporter, failedExporter);
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

        // Prepare data first
        Put put = new Put(Bytes.toBytes(row1));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put);
        Get get = new Get(Bytes.toBytes(row1));
        hTable.get(get);

        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.GET);
        metricsChecker(OHOperationType.GET, oldExporter, newExporter);

        // test failed op metrics
        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.get(get);
            fail();
        } catch (Exception e) {
            System.out.println("GET error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("not_exist_table"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.GET);
        failureMetricsChecker(OHOperationType.GET, newExporter, failedExporter);
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
        metricsChecker(OHOperationType.GET_LIST, oldExporter, newExporter);

        // test failed op metrics
        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.get(gets);
            fail();
        } catch (Exception e) {
            System.out.println("GET_LIST error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("not_exist_table"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.GET_LIST);
        failureMetricsChecker(OHOperationType.GET_LIST, newExporter, failedExporter);
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
        metricsChecker(OHOperationType.PUT_LIST, oldExporter, newExporter);

        // test failed op metrics
        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            put1 = new Put(Bytes.toBytes(row1));
            put2 = new Put(Bytes.toBytes(row2));
            puts.clear();
            puts.add(put1);
            puts.add(put2);
            hTable.put(puts);
            fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("No columns"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.PUT_LIST);
        failureMetricsChecker(OHOperationType.PUT_LIST, newExporter, failedExporter);
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

        // Prepare data first
        Put put = new Put(Bytes.toBytes(row1));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put);
        Delete delete = new Delete(Bytes.toBytes(row1));
        delete.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"));
        hTable.delete(delete);

        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.DELETE);
        metricsChecker(OHOperationType.DELETE, oldExporter, newExporter);

        // test failed op metrics
        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.delete(delete);
            fail();
        } catch (Exception e) {
            System.out.println("DELETE error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("not_exist_table"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.DELETE);
        failureMetricsChecker(OHOperationType.DELETE, newExporter, failedExporter);
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
        metricsChecker(OHOperationType.DELETE_LIST, oldExporter, newExporter);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.delete(deletes);
            fail();
        } catch (Exception e) {
            System.out.println("DELETE_LIST error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("not_exist_table"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.DELETE_LIST);
        failureMetricsChecker(OHOperationType.DELETE_LIST, newExporter, failedExporter);
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

        // Prepare data first
        Put put = new Put(Bytes.toBytes(row1));
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.put(put);
        Get get = new Get(Bytes.toBytes(row1));
        boolean exists = hTable.exists(get);
        Assert.assertTrue(exists);

        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.EXISTS);
        metricsChecker(OHOperationType.EXISTS, oldExporter, newExporter);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.exists(get);
            fail();
        } catch (Exception e) {
            System.out.println("EXISTS error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("not_exist_table"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.EXISTS);
        failureMetricsChecker(OHOperationType.EXISTS, newExporter, failedExporter);
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
        boolean[] exists = hTable.exists(gets);
        Assert.assertTrue(exists[0]);
        Assert.assertTrue(exists[1]);
        Assert.assertFalse(exists[2]);

        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.EXISTS_LIST);
        metricsChecker(OHOperationType.EXISTS_LIST, oldExporter, newExporter);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.exists(gets);
            fail();
        } catch (Exception e) {
            System.out.println("EXISTS_LIST error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("not_exist_table"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.EXISTS_LIST);
        failureMetricsChecker(OHOperationType.EXISTS_LIST, newExporter, failedExporter);
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
        metricsChecker(OHOperationType.SCAN, oldExporter, newExporter);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.getScanner(scan);
            fail();
        } catch (Exception e) {
            System.out.println("SCAN error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("not_exist_table"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.SCAN);
        failureMetricsChecker(OHOperationType.SCAN, newExporter, failedExporter);
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
        metricsChecker(OHOperationType.BATCH, oldExporter, newExporter);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            results = new Object[actions.size()];
            notExistTable.batch(actions, results);
            fail();
        } catch (Exception e) {
            System.out.println("BATCH error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("not_exist_table"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.BATCH);
        failureMetricsChecker(OHOperationType.BATCH, newExporter, failedExporter);
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
        metricsChecker(OHOperationType.BATCH_CALLBACK, oldExporter, newExporter);

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
            fail();
        } catch (Exception e) {
            System.out.println("BATCH_CALLBACK error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("not_exist_table"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.BATCH_CALLBACK);
        failureMetricsChecker(OHOperationType.BATCH_CALLBACK, newExporter, failedExporter);
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
        metricsChecker(OHOperationType.CHECK_AND_PUT, oldExporter, newExporter);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            Put put3 = new Put(Bytes.toBytes(row1));
            put3.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q3"), Bytes.toBytes("val3"));
            notExistTable.checkAndPut(Bytes.toBytes(row1), Bytes.toBytes(family1),
                    Bytes.toBytes("q2"), Bytes.toBytes("val2"), put3);
            fail();
        } catch (Exception e) {
            System.out.println("CHECK_AND_PUT error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("not_exist_table"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.CHECK_AND_PUT);
        failureMetricsChecker(OHOperationType.CHECK_AND_PUT, newExporter, failedExporter);
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
        metricsChecker(OHOperationType.CHECK_AND_DELETE, oldExporter, newExporter);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.checkAndDelete(Bytes.toBytes(row1), Bytes.toBytes(family1),
                    Bytes.toBytes("q1"), Bytes.toBytes("val1"), delete);
            fail();
        } catch (Exception e) {
            System.out.println("CHECK_AND_DELETE error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("not_exist_table"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.CHECK_AND_DELETE);
        failureMetricsChecker(OHOperationType.CHECK_AND_DELETE, newExporter, failedExporter);
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
        metricsChecker(OHOperationType.CHECK_AND_MUTATE, oldExporter, newExporter);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.checkAndMutate(Bytes.toBytes(row1), Bytes.toBytes(family1),
                    Bytes.toBytes("q1"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("val1"), rowMutations);
            fail();
        } catch (Exception e) {
            System.out.println("CHECK_AND_MUTATE error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("not_exist_table"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.CHECK_AND_MUTATE);
        failureMetricsChecker(OHOperationType.CHECK_AND_MUTATE, newExporter, failedExporter);
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

        Append append = new Append(Bytes.toBytes(row1));
        append.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), Bytes.toBytes("val1"));
        hTable.append(append);

        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.APPEND);
        metricsChecker(OHOperationType.APPEND, oldExporter, newExporter);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.append(append);
            fail();
        } catch (Exception e) {
            System.out.println("APPEND error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("not_exist_table"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.APPEND);
        failureMetricsChecker(OHOperationType.APPEND, newExporter, failedExporter);
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

        Increment increment = new Increment(Bytes.toBytes(row1));
        increment.addColumn(Bytes.toBytes(family1), Bytes.toBytes("q1"), 10L);
        hTable.increment(increment);

        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.INCREMENT);
        metricsChecker(OHOperationType.INCREMENT, oldExporter, newExporter);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.increment(increment);
            fail();
        } catch (Exception e) {
            System.out.println("INCREMENT error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("not_exist_table"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.INCREMENT);
        failureMetricsChecker(OHOperationType.INCREMENT, newExporter, failedExporter);
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

        long res = hTable.incrementColumnValue(Bytes.toBytes(row1), Bytes.toBytes(family1), Bytes.toBytes("q1"), 10L);
        Assert.assertEquals(10L, res);

        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.INCREMENT_COLUMN_VALUE);
        metricsChecker(OHOperationType.INCREMENT_COLUMN_VALUE, oldExporter, newExporter);

        Assert.assertEquals(0L, newExporter.getFailCount());
        try {
            Table notExistTable = connection.getTable(TableName.valueOf("not_exist_table"));
            notExistTable.incrementColumnValue(Bytes.toBytes(row1), Bytes.toBytes(family1), Bytes.toBytes("q1"), 10L);
            fail();
        } catch (Exception e) {
            System.out.println("INCREMENT_COLUMN_VALUE error: " + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("not_exist_table"));
        }
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter failedExporter = metrics.acquireMetrics(OHOperationType.INCREMENT_COLUMN_VALUE);
        failureMetricsChecker(OHOperationType.INCREMENT_COLUMN_VALUE, newExporter, failedExporter);
    }

    @Test
    public void testEmptyBatchMetrics() throws Exception {
        OHMetrics metrics = ((OHTable) hTable).getMetrics();
        if (metrics == null) {
            throw new ObTableUnexpectedException("unexpected null metrics");
        }
        // test put list
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.PUT_LIST);
        List<Put> puts = new ArrayList<>();
        hTable.put(puts);
        Thread.sleep(11000); // sleep over 10 second
        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.PUT_LIST);
        // average ops will larger than 0 even if the list is empty
        // because the metrics of ops is recorded for put<list> method
        System.out.println("PUT_LIST - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("PUT_LIST - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(oldExporter.getAverageSingleOpCount(), newExporter.getAverageSingleOpCount(), 0.001);

        // test batch
        oldExporter = metrics.acquireMetrics(OHOperationType.BATCH);
        List<Mutation> actions = new ArrayList<>();
        Object[] results = new Object[actions.size()];
        hTable.batch(actions, results);
        Thread.sleep(11000); // sleep over 10 second
        newExporter = metrics.acquireMetrics(OHOperationType.BATCH);
        System.out.println("BATCH - AverageOps: " + newExporter.getAverageOps());
        Assert.assertTrue(newExporter.getAverageOps() > 0);
        System.out.println("BATCH - AverageSingleOpCount: " + newExporter.getAverageSingleOpCount());
        Assert.assertEquals(oldExporter.getAverageSingleOpCount(), newExporter.getAverageSingleOpCount(), 0.001);
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
        MetricsExporter oldExporter = metrics.acquireMetrics(OHOperationType.PUT);
        long oldCount = oldExporter.getCount();

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
        scan.readVersions(10);
        scan.setBatch(10);
        scan.addFamily(family1);
        ResultScanner scanner = hTable.getScanner(scan);
        int cellCount = 0;
        for (Result res : scanner) {
            cellCount += res.rawCells().length;
        }
        Assert.assertEquals(threadCount * operationsPerThread, cellCount);
        Thread.sleep(11000);

        MetricsExporter newExporter = metrics.acquireMetrics(OHOperationType.PUT);
        long realCount = newExporter.getCount() - oldCount;
        long expectedCount = threadCount * operationsPerThread;
        System.out.println("real count: " + realCount);
        System.out.println("expected count: " + expectedCount);
        Assert.assertEquals(expectedCount, realCount);
    }

    private void metricsChecker(OHOperationType type,
                                MetricsExporter oldExporter,
                                MetricsExporter newExporter) throws Exception {
        System.out.println(String.format("%s - current AverageOps: " + newExporter.getAverageOps()
                + ", before AverageOps: " + oldExporter.getAverageOps(), type.name()));
        Assert.assertTrue(String.format("%s average ops should larger than before", type.name()),
                 newExporter.getAverageOps() > oldExporter.getAverageOps());

        System.out.println(String.format("%s - current AverageSingleOpCount: " + newExporter.getAverageSingleOpCount()
                + ", before AverageSingleOpCount: " + oldExporter.getAverageSingleOpCount(), type.name()));
        Assert.assertTrue(String.format("%s average single op count should > 0", type.name()),
                newExporter.getAverageSingleOpCount() > 0);

        System.out.println(String.format("%s - current AverageLatency: " + newExporter.getAverageLatency()
                + ", before AverageLatency: " + oldExporter.getAverageLatency(), type.name()));
        Assert.assertTrue(String.format("%s average latency should > 0", type.name()),
                newExporter.getAverageLatency() > 0);

        System.out.println(String.format("%s - current MaxLatency: " + newExporter.getMaxLatency()
                + ", before MaxLatency: " + oldExporter.getMaxLatency(), type.name()));
        Assert.assertTrue(String.format("%s max latency should > 0", type.name()),
                newExporter.getMaxLatency() > 0);

        System.out.println(String.format("%s - current 99thPercentile: " + newExporter.get99thPercentile()
                + ", before 99thPercentile: " + oldExporter.get99thPercentile(), type.name()));
        Assert.assertTrue(String.format("%s P99 latency should > 0", type.name()),
                newExporter.get99thPercentile() > 0);

        System.out.println(String.format("%s - current TotalRuntime: " + newExporter.getTotalRuntime()
                + ", before TotalRuntime: " + oldExporter.getTotalRuntime(), type.name()));
        Assert.assertTrue(String.format("%s runtime should larger than before", type.name()),
                newExporter.getTotalRuntime() > oldExporter.getTotalRuntime());
    }

    private void failureMetricsChecker(OHOperationType type,
                                       MetricsExporter oldExporter,
                                       MetricsExporter newExporter) throws Exception {
        Assert.assertEquals(String.format("%s initial failed op count should be 0", type.name()),
                0L, oldExporter.getFailCount());
        Assert.assertEquals(String.format("%s failed op count should be 1", type.name()),
                1, newExporter.getFailCount());
    }
}
