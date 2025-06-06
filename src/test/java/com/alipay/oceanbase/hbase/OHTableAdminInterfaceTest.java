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

import com.alipay.oceanbase.hbase.util.OHRegionMetrics;
import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import com.alipay.oceanbase.hbase.exception.FeatureNotSupportedException;
import com.alipay.oceanbase.hbase.util.ResultSetPrinter;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import org.junit.Assert;
import org.junit.Test;


import java.io.IOException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_TEST_LOAD_ENABLE;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;
import static  com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;

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
    public void testAdminEnDisableTable() throws Exception {
        java.sql.Connection conn = ObHTableTestUtil.getConnection();
        Statement st = conn.createStatement();
        st.execute("CREATE TABLEGROUP IF NOT EXISTS test_en_dis_tb SHARDING = 'ADAPTIVE';\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `test_en_dis_tb$family_with_group1` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = test_en_dis_tb PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `test_en_dis_tb$family_with_group2` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = test_en_dis_tb PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `test_en_dis_tb$family_with_group3` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = test_en_dis_tb PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "\n" +
                "CREATE DATABASE IF NOT EXISTS `en_dis`;\n" +
                "use `en_dis`;\n" +
                "CREATE TABLEGROUP IF NOT EXISTS `en_dis:test` SHARDING = 'ADAPTIVE';\n" +
                "CREATE TABLE IF NOT EXISTS `en_dis:test$family_group` (\n" +
                "      `K` varbinary(1024) NOT NULL,\n" +
                "      `Q` varbinary(256) NOT NULL,\n" +
                "      `T` bigint(20) NOT NULL,\n" +
                "      `V` varbinary(1024) DEFAULT NULL,\n" +
                "      PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = `en_dis:test`;" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `en_dis:test$family1` (\n" +
                "      `K` varbinary(1024) NOT NULL,\n" +
                "      `Q` varbinary(256) NOT NULL,\n" +
                "      `T` bigint(20) NOT NULL,\n" +
                "      `V` varbinary(1024) DEFAULT NULL,\n" +
                "      PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = `en_dis:test`;");
        Configuration conf = ObHTableTestUtil.newConfiguration();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        assertTrue(admin.tableExists(TableName.valueOf("en_dis", "test")));
        assertTrue(admin.tableExists(TableName.valueOf("test_en_dis_tb")));
        // 1. disable a non-existed table
        {
            IOException thrown = assertThrows(IOException.class,
                    () -> {
                        admin.disableTable(TableName.valueOf("tablegroup_not_exists"));
                    });
            assertTrue(thrown.getCause() instanceof ObTableException);
            Assert.assertEquals(ResultCodes.OB_TABLEGROUP_NOT_EXIST.errorCode, ((ObTableException) thrown.getCause()).getErrorCode());
        }
        // 2. write an enabled table, should succeed
        {
            if (admin.isTableDisabled(TableName.valueOf("test_en_dis_tb"))) {
                admin.enableTable(TableName.valueOf("test_en_dis_tb"));
            }
            batchInsert(10, "test_en_dis_tb");
            batchGet(10, "test_en_dis_tb");
        }

        // 3. disable a enable table
        {
            if (admin.isTableEnabled(TableName.valueOf("test_en_dis_tb"))) {
                admin.disableTable(TableName.valueOf("test_en_dis_tb"));
            }
            // write and read disable table, should fail
            try {
                batchInsert(10, "test_en_dis_tb");
                Assert.fail();
            } catch (IOException ex) {
                Assert.assertTrue(ex.getCause() instanceof ObTableException);
                System.out.println(ex.getCause().getMessage());
            }
            try {
                batchGet(10, "test_en_dis_tb");
                Assert.fail();
            } catch (IOException ex) {
                Assert.assertTrue(ex.getCause() instanceof ObTableException);
                Assert.assertEquals(ResultCodes.OB_KV_TABLE_NOT_ENABLED.errorCode,
                        ((ObTableException) ex.getCause()).getErrorCode());
            }

        }

        // 4. enable a disabled table
        {
            if (admin.isTableDisabled(TableName.valueOf("test_en_dis_tb"))) {
                admin.enableTable(TableName.valueOf("test_en_dis_tb"));
            }
            // write an enabled table, should succeed
            batchInsert(10, "test_en_dis_tb");
            batchGet(10, "test_en_dis_tb");
        }

        // 5. enable an enabled table
        {
            if (admin.isTableDisabled(TableName.valueOf("en_dis", "test"))) {
                admin.enableTable(TableName.valueOf("en_dis", "test"));
            }
            try {
                admin.enableTable(TableName.valueOf("en_dis", "test"));
                Assert.fail();
            } catch (IOException ex) {
                Assert.assertTrue(ex.getCause() instanceof ObTableException);
                Assert.assertEquals(ResultCodes.OB_KV_TABLE_NOT_DISABLED.errorCode,
                        ((ObTableException) ex.getCause()).getErrorCode());
            }
        }

        // 6. disable a disabled table
        {
            if (admin.isTableEnabled(TableName.valueOf("en_dis", "test"))) {
                admin.disableTable(TableName.valueOf("en_dis", "test"));
            }
            try {
                admin.disableTable(TableName.valueOf("en_dis", "test"));
                Assert.fail();
            } catch (IOException ex) {
                Assert.assertTrue(ex.getCause() instanceof ObTableException);
                Assert.assertEquals(ResultCodes.OB_KV_TABLE_NOT_ENABLED.errorCode,
                        ((ObTableException) ex.getCause()).getErrorCode());
            }
        }

        admin.deleteTable(TableName.valueOf("test_en_dis_tb"));
        assertFalse(admin.tableExists(TableName.valueOf("test_en_dis_tb")));
        admin.deleteTable(TableName.valueOf("en_dis", "test"));
        assertFalse(admin.tableExists(TableName.valueOf("en_dis", "test")));
    }

    private void batchGet(int rows, String tablegroup) throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tablegroup));
        List<Row> batchLsit = new LinkedList<>();
        for (int i = 0; i < rows; ++i) {
            Get get = new Get(toBytes("Key" + i));
            batchLsit.add(get);
            if (i % 100 == 0) { // 100 rows one batch to avoid OB_TIMEOUT
                Object[] results = new Object[batchLsit.size()];
                table.batch(batchLsit, results);
                batchLsit.clear();
            }
        }
        Object[] results = new Object[batchLsit.size()];
        table.batch(batchLsit, results);
    }

    @Test
    public void testAdminGetRegionMetrics() throws Exception {
        java.sql.Connection conn = ObHTableTestUtil.getConnection();
        Statement st = conn.createStatement();
        st.execute("CREATE TABLEGROUP IF NOT EXISTS test_get_region_metrics SHARDING = 'ADAPTIVE';\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `test_get_region_metrics$family_with_group1` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = test_get_region_metrics PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `test_get_region_metrics$family_with_group2` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = test_get_region_metrics PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `test_get_region_metrics$family_with_group3` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = test_get_region_metrics PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "\n" +
                "CREATE DATABASE IF NOT EXISTS `get_region`;\n" +
                "use `get_region`;\n" +
                "CREATE TABLEGROUP IF NOT EXISTS `get_region:test_multi_cf` SHARDING = 'ADAPTIVE';\n" +
                "CREATE TABLE IF NOT EXISTS `get_region:test_multi_cf$family_with_group1` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "   PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = `get_region:test_multi_cf` PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "CREATE TABLE IF NOT EXISTS `get_region:test_multi_cf$family_with_group2` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = `get_region:test_multi_cf` PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "CREATE TABLE IF NOT EXISTS `get_region:test_multi_cf$family_with_group3` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = `get_region:test_multi_cf` PARTITION BY KEY(`K`) PARTITIONS 3;");
        st.close();
        conn.close();
        String tablegroup1 = "test_get_region_metrics";
        String tablegroup2 = "get_region:test_multi_cf";
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
        batchInsert(100000, tablegroup1);
        List<RegionMetrics> metrics = admin.getRegionMetrics(null, TableName.valueOf(tablegroup1));
        for (RegionMetrics regionMetrics : metrics) {
            System.out.println("region name: " + regionMetrics.getNameAsString()
                    + ", storeFileSize: " + regionMetrics.getStoreFileSize()
                    + ", memFileSize: " + regionMetrics.getMemStoreSize());
        }
        // concurrently read while writing 150 thousand of rows to 2 tablegroups
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(100);
        List<Exception> exceptionCatcher = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            int taskId = i;
            executorService.submit(() -> {
                try {
                    if (taskId % 2 == 1) {
                        List<RegionMetrics> regionMetrics = null;
                        // test get regionMetrics from different namespaces
                        if (taskId % 3 != 0) {
                            regionMetrics = admin.getRegionMetrics(null, TableName.valueOf(tablegroup1));
                        } else {
                            regionMetrics = admin.getRegionMetrics(null, TableName.valueOf(tablegroup2));
                        }
                        for (RegionMetrics m : regionMetrics) {
                            System.out.println("task: " + taskId + ", tablegroup: " + ((OHRegionMetrics) m).getTablegroup()
                                    + ", region name: " + m.getNameAsString()
                                    + ", storeFileSize: " + m.getStoreFileSize()
                                    + ", memFileSize: " + m.getMemStoreSize());
                        }
                    } else {
                        if (taskId % 8 == 0) {
                            batchInsert(1000, tablegroup2);
                        } else {
                            batchInsert(1000, tablegroup1);
                        }
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

    @Test
    public void testAdminDeleteTable() throws Exception {
        java.sql.Connection conn = ObHTableTestUtil.getConnection();
        Statement st = conn.createStatement();
        st.execute("CREATE TABLEGROUP IF NOT EXISTS test_del_tb SHARDING = 'ADAPTIVE';\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `test_del_tb$family_with_group1` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = test_del_tb PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `test_del_tb$family_with_group2` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = test_del_tb PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `test_del_tb$family_with_group3` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = test_del_tb PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "\n" +
                "CREATE DATABASE IF NOT EXISTS `del_tb`;\n" +
                "use `del_tb`;\n" +
                "CREATE TABLEGROUP IF NOT EXISTS `del_tb:test` SHARDING = 'ADAPTIVE';\n" +
                "CREATE TABLE IF NOT EXISTS `del_tb:test$family_group` (\n" +
                "      `K` varbinary(1024) NOT NULL,\n" +
                "      `Q` varbinary(256) NOT NULL,\n" +
                "      `T` bigint(20) NOT NULL,\n" +
                "      `V` varbinary(1024) DEFAULT NULL,\n" +
                "      PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = `del_tb:test`;" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `del_tb:test$family1` (\n" +
                "      `K` varbinary(1024) NOT NULL,\n" +
                "      `Q` varbinary(256) NOT NULL,\n" +
                "      `T` bigint(20) NOT NULL,\n" +
                "      `V` varbinary(1024) DEFAULT NULL,\n" +
                "      PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = `del_tb:test`;");
        st.close();
        conn.close();
        Configuration conf = ObHTableTestUtil.newConfiguration();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        assertTrue(admin.tableExists(TableName.valueOf("del_tb", "test")));
        assertTrue(admin.tableExists(TableName.valueOf("test_del_tb")));
        IOException thrown = assertThrows(IOException.class,
                () -> {
                    admin.deleteTable(TableName.valueOf("tablegroup_not_exists"));
                });
        Assert.assertTrue(thrown.getCause() instanceof ObTableException);
        Assert.assertEquals(ResultCodes.OB_TABLEGROUP_NOT_EXIST.errorCode, ((ObTableException) thrown.getCause()).getErrorCode());
        admin.deleteTable(TableName.valueOf("del_tb", "test"));
        admin.deleteTable(TableName.valueOf("test_del_tb"));
        assertFalse(admin.tableExists(TableName.valueOf("del_tb", "test")));
        assertFalse(admin.tableExists(TableName.valueOf("test_del_tb")));
    }

    @Test
    public void testAdminTableExists() throws Exception {
        java.sql.Connection conn = ObHTableTestUtil.getConnection();
        Statement st = conn.createStatement();
        st.execute("CREATE TABLEGROUP IF NOT EXISTS test_exist_tb SHARDING = 'ADAPTIVE';\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `test_exist_tb$family_with_group1` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = test_exist_tb PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `test_exist_tb$family_with_group2` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = test_exist_tb PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `test_exist_tb$family_with_group3` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = test_exist_tb PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "\n" +
                "CREATE DATABASE IF NOT EXISTS `exist_tb`;\n" +
                "use `exist_tb`;\n" +
                "CREATE TABLEGROUP IF NOT EXISTS `exist_tb:test` SHARDING = 'ADAPTIVE';\n" +
                "CREATE TABLE IF NOT EXISTS `exist_tb:test$family_group` (\n" +
                "      `K` varbinary(1024) NOT NULL,\n" +
                "      `Q` varbinary(256) NOT NULL,\n" +
                "      `T` bigint(20) NOT NULL,\n" +
                "      `V` varbinary(1024) DEFAULT NULL,\n" +
                "      PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = `exist_tb:test`;" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `exist_tb:test$family1` (\n" +
                "      `K` varbinary(1024) NOT NULL,\n" +
                "      `Q` varbinary(256) NOT NULL,\n" +
                "      `T` bigint(20) NOT NULL,\n" +
                "      `V` varbinary(1024) DEFAULT NULL,\n" +
                "      PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = `exist_tb:test`;");
        st.close();
        conn.close();
        Configuration conf = ObHTableTestUtil.newConfiguration();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        // TableName cannot contain $ symbol
        Assert.assertThrows(IllegalArgumentException.class,
                () -> {
                    TableName.valueOf("random_string$");
                });
        Assert.assertFalse(admin.tableExists(TableName.valueOf("tablegroup_not_exists")));
        Assert.assertTrue(admin.tableExists(TableName.valueOf("test_exist_tb")));
        Assert.assertTrue(admin.tableExists(TableName.valueOf("exist_tb", "test")));
    }

    private void batchInsert(int rows, String tablegroup) throws Exception {
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
        Table table = connection.getTable(TableName.valueOf(tablegroup));
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

    @Test
    public void testCreateDeleteTable() throws Exception {
        TableName tableName = TableName.valueOf("testCreateTable");
        byte[] cf1 = Bytes.toBytes("cf1");
        byte[] cf2 = Bytes.toBytes("cf2");
        byte[] cf3 = Bytes.toBytes("cf3");
        Configuration conf = ObHTableTestUtil.newConfiguration();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        // 1. construct htable desc and column family desc
        HColumnDescriptor hcd1 = new HColumnDescriptor(cf1);
        hcd1.setMaxVersions(2);
        hcd1.setTimeToLive(172800);

        HColumnDescriptor hcd2 = new HColumnDescriptor(cf2);
        hcd2.setMaxVersions(1);
        hcd2.setTimeToLive(86400);

        HColumnDescriptor hcd3 = new HColumnDescriptor(cf3);

        // 2. execute create table and check exists
        HTableDescriptor htd = new HTableDescriptor(tableName);
        htd.addFamily(hcd1);
        htd.addFamily(hcd2);
        htd.addFamily(hcd3);
        admin.createTable(htd);

        // 3. check table creation success and correctness
        assertTrue(admin.tableExists(tableName));
        // TODO: show create table, need to be replace by getDescriptor
        java.sql.Connection conn = ObHTableTestUtil.getConnection();
        String selectSql = "show create table " + tableName.getNameAsString() + "$" + Bytes.toString(cf1);
        System.out.println("execute sql: " + selectSql);
        java.sql.ResultSet resultSet = conn.createStatement().executeQuery(selectSql);
        ResultSetPrinter.print(resultSet);

        selectSql = "show create table " + tableName.getNameAsString() + "$" + Bytes.toString(cf2);
        System.out.println("execute sql: " + selectSql);
        resultSet = conn.createStatement().executeQuery(selectSql);
        ResultSetPrinter.print(resultSet);

        selectSql = "show create table " + tableName.getNameAsString() + "$" + Bytes.toString(cf3);
        System.out.println("execute sql: " + selectSql);
        resultSet = conn.createStatement().executeQuery(selectSql);
        ResultSetPrinter.print(resultSet);


        // 4. test put/get some data
        Table table = connection.getTable(tableName);
        Put put = new Put(toBytes("Key" + 1));
        put.addColumn(cf1, "c1".getBytes(), "hello world".getBytes());
        put.addColumn(cf2, "c2".getBytes(), "hello world".getBytes());
        put.addColumn(cf3, "c3".getBytes(), "hello world".getBytes());
        table.put(put);

        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        List<Cell> cells = getCellsFromScanner(resultScanner);
        Assert.assertEquals(3, cells.size());

        // 5. disable and delete table
        admin.disableTable(tableName);
        admin.deleteTable(tableName);

        // 5. test table exists after delete
        admin.tableExists(tableName);

        // 6. recreate and delete table
        admin.createTable(htd);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    void testConcurCreateDelTablesHelper(List<TableName> tableNames, Boolean ignoreException) throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        int tableNums = tableNames.size();

        // use some column family desc
        byte[] cf1 = Bytes.toBytes("cf1");
        byte[] cf2 = Bytes.toBytes("cf2");
        byte[] cf3 = Bytes.toBytes("cf3");
        HColumnDescriptor hcd1 = new HColumnDescriptor(cf1);
        hcd1.setMaxVersions(2);
        hcd1.setTimeToLive(172800);
        HColumnDescriptor hcd2 = new HColumnDescriptor(cf2);
        hcd1.setMaxVersions(1);
        hcd1.setTimeToLive(86400);
        HColumnDescriptor hcd3 = new HColumnDescriptor(cf3);

        // 1. generate create table task, one task per table
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < tableNums; i++) {
            int finalI = i;
            tasks.add(()->{
                HTableDescriptor htd = new HTableDescriptor(tableNames.get(finalI));
                htd.addFamily(hcd1);
                htd.addFamily(hcd2);
                htd.addFamily(hcd3);
                try {
                    admin.createTable(htd);
                } catch (Exception e) {
                    System.out.println(e);
                    if (!ignoreException) {
                        throw e;
                    }
                }
                return null;
            });
        }

        // 2. execute concurrent create table tasks
        ExecutorService executorService = Executors.newFixedThreadPool(tableNums);
        executorService.invokeAll(tasks);
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        // 3. check table create success
        for (int i = 0; i < tableNames.size(); i++) {
            TableName tableName = tableNames.get(i);
            assertTrue(admin.tableExists(tableName));
        }

        // 4. test put/get/delete some data
        for (int i = 0; i < tableNames.size(); i++) {
            Table table = connection.getTable(tableNames.get(i));
            Put put = new Put(toBytes("Key" + 1));
            put.addColumn(cf1, "c1".getBytes(), "hello world".getBytes());
            put.addColumn(cf2, "c2".getBytes(), "hello world".getBytes());
            put.addColumn(cf3, "c3".getBytes(), "hello world".getBytes());
            table.put(put);

            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            List<Cell> cells = getCellsFromScanner(resultScanner);
            Assert.assertEquals(3, cells.size());

            table.delete(new Delete(toBytes("Key" + 1)));
        }

        // 4. disable all tables;
        for (int i = 0; i < tableNames.size(); i++) {
            TableName tableName = tableNames.get(i);
            try {
                admin.disableTable(tableName);
            } catch (IOException e) {
                System.out.println(e);
            }
        }
        assertTrue(admin.isTableDisabled(tableNames.get(0)));

        // 5. generate delete table task
        List<Callable<Void>> delTasks = new ArrayList<>();
        for (int i = 0; i < tableNums; i++) {
            int finalI = i;
            delTasks.add(()->{
                try {
                    admin.deleteTable(tableNames.get(finalI));
                } catch (Exception e) {
                    System.out.println(e);
                    if (!ignoreException) {
                        throw e;
                    }
                }
                return null;
            });
        }

        // 6. execute concurrent delete table tasks
        ExecutorService delExecutorService = Executors.newFixedThreadPool(tableNums);
        delExecutorService.invokeAll(delTasks);
        delExecutorService.shutdown();
        delExecutorService.awaitTermination(1, TimeUnit.MINUTES);

        // 7. check table deletion success
        for (int i = 0; i < tableNames.size(); i++) {
            TableName tableName = tableNames.get(i);
            assertFalse(admin.tableExists(tableName));
        }
    }

    // 2. test concurrent create or delete different table
    // step1: create different tables concurrently, it must succeed
    // step2: execute put/read/delete on created table
    // step3: delete different tables concurrently, it must succeed
    @Test
    public void testConcurCreateDelTables() throws Exception {
        final int tableNums = 20;
        List<TableName> tableNames = new ArrayList<>();
        for (int i = 0; i < tableNums; i++) {
           tableNames.add(TableName.valueOf("testConcurCreateTable" + i));
        }
        testConcurCreateDelTablesHelper(tableNames, false);
    }

    // 3. test concurrent create or delete same table
    // step1: create one table concurrently, only one table was successfully created
    // step2: execute put/read/delete on created table
    // step3: delete one table concurrently, the table will be deleted successfully
    @Test
    public void testConcurCreateOneTable() throws Exception {
        final int taskNum = 20;
        TableName tableName = TableName.valueOf("testConcurCreateOneTable");
        List<TableName> tableNames = new ArrayList<>();
        for (int i = 0; i < taskNum; i++) {
            tableNames.add(tableName);
        }
        testConcurCreateDelTablesHelper(tableNames, true);
    }

    // 4. test the performance of concurrent create/delete table
    @Test
    public void testConcurCreateDelPerf() throws Exception {
        final int tableNums = 100;
        List<TableName> tableNames = new ArrayList<>();
        Configuration conf = ObHTableTestUtil.newConfiguration();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        byte[] cf1 = Bytes.toBytes("cf1");
        byte[] cf2 = Bytes.toBytes("cf2");
        byte[] cf3 = Bytes.toBytes("cf3");
        HColumnDescriptor hcd1 = new HColumnDescriptor(cf1);
        hcd1.setMaxVersions(2);
        hcd1.setTimeToLive(172800);
        HColumnDescriptor hcd2 = new HColumnDescriptor(cf2);
        hcd1.setMaxVersions(1);
        hcd1.setTimeToLive(86400);
        HColumnDescriptor hcd3 = new HColumnDescriptor(cf3);

        for (int i = 0; i < tableNums; i++) {
            tableNames.add(TableName.valueOf("testConcurCreateDelPerf" + i));
        }

        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < tableNums; i++) {
            int finalI = i;
            tasks.add(()->{
                HTableDescriptor htd = new HTableDescriptor(tableNames.get(finalI));
                htd.addFamily(hcd1);
                htd.addFamily(hcd2);
                htd.addFamily(hcd3);
                try {
                    admin.createTable(htd);
                } catch (Exception e) {
                    System.out.println(e);
                }
                return null;
            });
        }

        // 2. execute concurrent create table tasks
        long start = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(tableNums);
        executorService.invokeAll(tasks);
        executorService.shutdown();
        executorService.awaitTermination(2, TimeUnit.MINUTES);
        long duration = System.currentTimeMillis() - start;
        System.out.println("create " + tableNums + " tables cost " + duration + " ms.");

        // 3. disable all tables;
        for (int i = 0; i < tableNames.size(); i++) {
            TableName tableName = tableNames.get(i);
            admin.disableTable(tableName);
        }

        // 4. generate delete table task
        List<Callable<Void>> delTasks = new ArrayList<>();
        for (int i = 0; i < tableNums; i++) {
            int finalI = i;
            delTasks.add(()->{
                try {
                    admin.deleteTable(tableNames.get(finalI));
                } catch (Exception e) {
                    System.out.println(e);
                }
                return null;
            });
        }

        // 6. execute concurrent delete table tasks
        start = System.currentTimeMillis();
        ExecutorService delExecutorService = Executors.newFixedThreadPool(tableNums);
        delExecutorService.invokeAll(delTasks);
        delExecutorService.shutdown();
        delExecutorService.awaitTermination(1, TimeUnit.MINUTES);
        duration = System.currentTimeMillis() - start;
        System.out.println("delete " + tableNums + " tables cost " + duration + " ms.");
    }
}
