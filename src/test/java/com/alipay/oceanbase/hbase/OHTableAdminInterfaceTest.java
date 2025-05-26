/*-
 * #%L
 * OBKV HBase Client Framework
 * %%
 * Copyright (C) 2022 OceanBase Group
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

package com.alipay.oceanbase.hbase;

import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import com.alipay.oceanbase.hbase.exception.FeatureNotSupportedException;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_TEST_LOAD_ENABLE;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertThrows;

public class OHTableAdminInterfaceTest {
    public OHTablePool setUpLoadPool() throws IOException {
        Configuration c = ObHTableTestUtil.newConfiguration();
        c.set(HBASE_HTABLE_TEST_LOAD_ENABLE, "true");
        OHTablePool ohTablePool = new OHTablePool(c, 10);

        ohTablePool.setRuntimeBatchExecutor("test", Executors.newFixedThreadPool(3));

        return ohTablePool;
    }

    public OHTablePool setUpPool() throws IOException {
        Configuration c = ObHTableTestUtil.newConfiguration();
        OHTablePool ohTablePool = new OHTablePool(c, 10);

        ohTablePool.setRuntimeBatchExecutor("test", Executors.newFixedThreadPool(3));

        return ohTablePool;
    }

    @Test
    public void testGetStartEndKeysOHTableClientRange() throws Exception {
        // Init OHTableClient
        OHTableClient ohTableClient = ObHTableTestUtil.newOHTableClient("testAdminRange");
        ohTableClient.init();

        Pair<byte[][], byte[][]> startEndKeys = ohTableClient.getStartEndKeys();

        Assert.assertEquals(3, startEndKeys.getFirst().length);
        Assert.assertEquals(3, startEndKeys.getSecond().length);
        Assert.assertEquals(0, startEndKeys.getFirst()[0].length);
        Assert.assertArrayEquals("a".getBytes(), startEndKeys.getFirst()[1]);
        Assert.assertArrayEquals("w".getBytes(), startEndKeys.getFirst()[2]);
        Assert.assertArrayEquals("a".getBytes(), startEndKeys.getSecond()[0]);
        Assert.assertArrayEquals("w".getBytes(), startEndKeys.getSecond()[1]);
        Assert.assertEquals(0, startEndKeys.getSecond()[2].length);
    }

    @Test
    public void testGetStartEndKeysOHTableClientKey() throws Exception {
        // Init OHTableClient
        OHTableClient ohTableClient = ObHTableTestUtil.newOHTableClient("testAdminKey");
        ohTableClient.init();

        Pair<byte[][], byte[][]> startEndKeys = ohTableClient.getStartEndKeys();

        // [none], [none]
        Assert.assertEquals(1, startEndKeys.getFirst().length);
        Assert.assertEquals(1, startEndKeys.getSecond().length);
        Assert.assertEquals(0, startEndKeys.getFirst()[0].length);
        Assert.assertEquals(0, startEndKeys.getSecond()[0].length);
    }

    @Test
    public void testGetStartEndKeysOHTableClientNon() throws Exception {
        // Init OHTableClient
        OHTableClient ohTableClient = ObHTableTestUtil.newOHTableClient("test");
        ohTableClient.init();

        Pair<byte[][], byte[][]> startEndKeys = ohTableClient.getStartEndKeys();

        // [none], [none]
        Assert.assertEquals(1, startEndKeys.getFirst().length);
        Assert.assertEquals(1, startEndKeys.getSecond().length);
        Assert.assertEquals(0, startEndKeys.getFirst()[0].length);
        Assert.assertEquals(0, startEndKeys.getSecond()[0].length);
    }

    @Test
    public void testGetStartEndKeysOHTablePoolRange() throws Exception {
        // Init PooledOHTable
        OHTablePool ohTablePool = setUpPool();
        OHTablePool.PooledOHTable hTable = (OHTablePool.PooledOHTable) ohTablePool
            .getTable("testAdminRange");

        Pair<byte[][], byte[][]> startEndKeys = hTable.getStartEndKeys();

        Assert.assertEquals(3, startEndKeys.getFirst().length);
        Assert.assertEquals(3, startEndKeys.getSecond().length);
        Assert.assertEquals(0, startEndKeys.getFirst()[0].length);
        Assert.assertArrayEquals("a".getBytes(), startEndKeys.getFirst()[1]);
        Assert.assertArrayEquals("w".getBytes(), startEndKeys.getFirst()[2]);
        Assert.assertArrayEquals("a".getBytes(), startEndKeys.getSecond()[0]);
        Assert.assertArrayEquals("w".getBytes(), startEndKeys.getSecond()[1]);
        Assert.assertEquals(0, startEndKeys.getSecond()[2].length);
    }

    @Test
    public void testGetStartEndKeysOHTablePoolKey() throws Exception {
        // Init PooledOHTable
        OHTablePool ohTablePool = setUpPool();
        OHTablePool.PooledOHTable hTable = (OHTablePool.PooledOHTable) ohTablePool
            .getTable("testAdminKey");

        Pair<byte[][], byte[][]> startEndKeys = hTable.getStartEndKeys();

        // [none], [none]
        Assert.assertEquals(1, startEndKeys.getFirst().length);
        Assert.assertEquals(1, startEndKeys.getSecond().length);
        Assert.assertEquals(0, startEndKeys.getFirst()[0].length);
        Assert.assertEquals(0, startEndKeys.getSecond()[0].length);
    }

    @Test
    public void testGetStartEndKeysOHTablePoolNon() throws Exception {
        // Init PooledOHTable
        OHTablePool ohTablePool = setUpPool();
        OHTablePool.PooledOHTable hTable = (OHTablePool.PooledOHTable) ohTablePool.getTable("test");

        Pair<byte[][], byte[][]> startEndKeys = hTable.getStartEndKeys();

        // [none], [none]
        Assert.assertEquals(1, startEndKeys.getFirst().length);
        Assert.assertEquals(1, startEndKeys.getSecond().length);
        Assert.assertEquals(0, startEndKeys.getFirst()[0].length);
        Assert.assertEquals(0, startEndKeys.getSecond()[0].length);
    }

    @Test
    public void testGetStartEndKeysOHTableClientLoadRange() throws Exception {
        // Init OHTableClient with load
        OHTableClient ohTableClient = ObHTableTestUtil.newOHTableClient("testAdminRange");
        ohTableClient.init();
        ohTableClient.getConfiguration().set(HBASE_HTABLE_TEST_LOAD_ENABLE, "true");

        Pair<byte[][], byte[][]> startEndKeys = ohTableClient.getStartEndKeys();

        Assert.assertEquals(3, startEndKeys.getFirst().length);
        Assert.assertEquals(3, startEndKeys.getSecond().length);
        Assert.assertEquals(0, startEndKeys.getFirst()[0].length);
        Assert.assertArrayEquals("a".getBytes(), startEndKeys.getFirst()[1]);
        Assert.assertArrayEquals("w".getBytes(), startEndKeys.getFirst()[2]);
        Assert.assertArrayEquals("a".getBytes(), startEndKeys.getSecond()[0]);
        Assert.assertArrayEquals("w".getBytes(), startEndKeys.getSecond()[1]);
        Assert.assertEquals(0, startEndKeys.getSecond()[2].length);
    }

    @Test
    public void testGetStartEndKeysOHTableClientLoadKey() throws Exception {
        // Init OHTableClient with load
        OHTableClient ohTableClient = ObHTableTestUtil.newOHTableClient("testAdminKey");
        ohTableClient.init();
        ohTableClient.getConfiguration().set(HBASE_HTABLE_TEST_LOAD_ENABLE, "true");

        Pair<byte[][], byte[][]> startEndKeys = ohTableClient.getStartEndKeys();

        // [none], [none]
        Assert.assertEquals(1, startEndKeys.getFirst().length);
        Assert.assertEquals(1, startEndKeys.getSecond().length);
        Assert.assertEquals(0, startEndKeys.getFirst()[0].length);
        Assert.assertEquals(0, startEndKeys.getSecond()[0].length);
    }

    @Test
    public void testGetStartEndKeysOHTableClientLoadNon() throws Exception {
        // Init OHTableClient with load
        OHTableClient ohTableClient = ObHTableTestUtil.newOHTableClient("test");
        ohTableClient.init();
        ohTableClient.getConfiguration().set(HBASE_HTABLE_TEST_LOAD_ENABLE, "true");

        Pair<byte[][], byte[][]> startEndKeys = ohTableClient.getStartEndKeys();

        // [none], [none]
        Assert.assertEquals(1, startEndKeys.getFirst().length);
        Assert.assertEquals(1, startEndKeys.getSecond().length);
        Assert.assertEquals(0, startEndKeys.getFirst()[0].length);
        Assert.assertEquals(0, startEndKeys.getSecond()[0].length);
    }

    @Test
    public void testGetStartEndKeysOHTablePoolLoadRange() throws Exception {
        // Init PooledOHTable
        OHTablePool ohTablePool = setUpLoadPool();
        OHTablePool.PooledOHTable hTable = (OHTablePool.PooledOHTable) ohTablePool
            .getTable("testAdminRange");
        hTable.getConfiguration().set(HBASE_HTABLE_TEST_LOAD_ENABLE, "true");

        Pair<byte[][], byte[][]> startEndKeys = hTable.getStartEndKeys();

        Assert.assertEquals(3, startEndKeys.getFirst().length);
        Assert.assertEquals(3, startEndKeys.getSecond().length);
        Assert.assertEquals(0, startEndKeys.getFirst()[0].length);
        Assert.assertArrayEquals("a".getBytes(), startEndKeys.getFirst()[1]);
        Assert.assertArrayEquals("w".getBytes(), startEndKeys.getFirst()[2]);
        Assert.assertArrayEquals("a".getBytes(), startEndKeys.getSecond()[0]);
        Assert.assertArrayEquals("w".getBytes(), startEndKeys.getSecond()[1]);
        Assert.assertEquals(0, startEndKeys.getSecond()[2].length);
    }

    @Test
    public void testGetStartEndKeysOHTablePoolLoadKey() throws Exception {
        // Init PooledOHTable
        OHTablePool ohTablePool = setUpLoadPool();
        OHTablePool.PooledOHTable hTable = (OHTablePool.PooledOHTable) ohTablePool
            .getTable("testAdminKey");
        hTable.getConfiguration().set(HBASE_HTABLE_TEST_LOAD_ENABLE, "true");

        Pair<byte[][], byte[][]> startEndKeys = hTable.getStartEndKeys();

        // [none], [none]
        Assert.assertEquals(1, startEndKeys.getFirst().length);
        Assert.assertEquals(1, startEndKeys.getSecond().length);
        Assert.assertEquals(0, startEndKeys.getFirst()[0].length);
        Assert.assertEquals(0, startEndKeys.getSecond()[0].length);
    }

    @Test
    public void testGetStartEndKeysOHTablePoolLoadNon() throws Exception {
        // Init PooledOHTable
        OHTablePool ohTablePool = setUpLoadPool();
        OHTablePool.PooledOHTable hTable = (OHTablePool.PooledOHTable) ohTablePool.getTable("test");
        hTable.getConfiguration().set(HBASE_HTABLE_TEST_LOAD_ENABLE, "true");

        Pair<byte[][], byte[][]> startEndKeys = hTable.getStartEndKeys();

        // [none], [none]
        Assert.assertEquals(1, startEndKeys.getFirst().length);
        Assert.assertEquals(1, startEndKeys.getSecond().length);
        Assert.assertEquals(0, startEndKeys.getFirst()[0].length);
        Assert.assertEquals(0, startEndKeys.getSecond()[0].length);
    }

    @Test
    public void testAdminGetRegionMetrics() throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        IOException thrown = assertThrows(IOException.class,
                () -> {
                    admin.getRegionMetrics(null, TableName.valueOf("tablegroup_not_exists"));
                });
        Assert.assertTrue(thrown.getCause() instanceof ObTableException);
        Assert.assertEquals(ResultCodes.OB_TABLEGROUP_NOT_EXIST.errorCode, ((ObTableException) thrown.getCause()).getErrorCode());
        assertThrows(FeatureNotSupportedException.class,
                () -> {
                    admin.getRegionMetrics(ServerName.valueOf("localhost,1,1"));
                });
        // insert 300 thousand of rows in each table under tablegroup test_multi_cf
        batchInsert(100000);
        List<RegionMetrics> metrics = admin.getRegionMetrics(null, TableName.valueOf("test_multi_cf"));
        for (RegionMetrics regionMetrics : metrics) {
            System.out.println("region name: " + regionMetrics.getNameAsString()
                    + ", storeFileSize: " + regionMetrics.getStoreFileSize()
                    + ", memFileSize: " + regionMetrics.getMemStoreSize());
        }
        // concurrently read while writing 150 thousand of rows to each table
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(100);
        List<Exception> exceptionCatcher = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            int taskId = i;
            executorService.submit(() -> {
                try {
                    if (taskId % 2 == 1) {
                        List<RegionMetrics> regionMetrics = admin.getRegionMetrics(null, TableName.valueOf("test_multi_cf"));
                        for (RegionMetrics m : regionMetrics) {
                            System.out.println("task: " + taskId + ", region name: " + m.getNameAsString()
                                    + ", storeFileSize: " + m.getStoreFileSize()
                                    + ", memFileSize: " + m.getMemStoreSize());
                        }
                    } else {
                        batchInsert(1000);
                        System.out.println("task: " + taskId + ", batchInsert");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    exceptionCatcher.add(e);
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
            exceptionCatcher.add(e);
        }
        executorService.shutdownNow();
        Assert.assertTrue(exceptionCatcher.isEmpty());
    }

    private void batchInsert(int rows) throws Exception {
        byte[] family1 = Bytes.toBytes("family_with_group1");
        byte[] family2 = Bytes.toBytes("family_with_group2");
        byte[] family3 = Bytes.toBytes("family_with_group3");
        byte[] family1_column1 = "family1_column1".getBytes();
        byte[] family1_column2 = "family1_column2".getBytes();
        byte[] family1_column3 = "family1_column3".getBytes();
        byte[] family2_column1 = "family2_column1".getBytes();
        byte[] family2_column2 = "family2_column2".getBytes();
        byte[] family2_column3 = "family2_column3".getBytes();
        byte[] family3_column1 = "family3_column1".getBytes();
        byte[] family3_column2 = "family3_column2".getBytes();
        byte[] family3_column3 = "family3_column3".getBytes();
        byte[] family1_value1 = Bytes.toBytes("family1_value1");
        byte[] family1_value2 = Bytes.toBytes("family1_value2");
        byte[] family1_value3 = Bytes.toBytes("family1_value3");
        byte[] family2_value1 = Bytes.toBytes("family2_value1");
        byte[] family2_value2 = Bytes.toBytes("family2_value2");
        byte[] family2_value3 = Bytes.toBytes("family2_value3");
        byte[] family3_value1 = Bytes.toBytes("family3_value1");
        byte[] family3_value2 = Bytes.toBytes("family3_value2");
        byte[] family3_value3 = Bytes.toBytes("family3_value3");
        Configuration conf = ObHTableTestUtil.newConfiguration();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("test_multi_cf"));
        List<Row> batchLsit = new LinkedList<>();
        for (int i = 0; i < rows; ++i) {
            Put put = new Put(toBytes("Key" + i));
            put.addColumn(family1, family1_column1, family1_value1);
            put.addColumn(family1, family1_column2, family1_value2);
            put.addColumn(family1, family1_column3, family1_value3);
            put.addColumn(family2, family2_column1, family2_value1);
            put.addColumn(family2, family2_column2, family2_value2);
            put.addColumn(family2, family2_column3, family2_value3);
            put.addColumn(family3, family3_column1, family3_value1);
            put.addColumn(family3, family3_column2, family3_value2);
            put.addColumn(family3, family3_column3, family3_value3);
            batchLsit.add(put);
            if (i % 100 == 0) { // 100 rows one batch to avoid OB_TIMEOUT
                Object[] results = new Object[batchLsit.size()];
                table.batch(batchLsit, results);
                batchLsit.clear();
            }
        }
        Object[] results = new Object[batchLsit.size()];
        table.batch(batchLsit, results);
    }
}
