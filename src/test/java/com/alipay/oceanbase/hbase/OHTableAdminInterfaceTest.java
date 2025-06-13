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
import com.alipay.oceanbase.hbase.util.ResultSetPrinter;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.exception.ObTableGetException;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import org.junit.Assert;
import org.junit.Test;


import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_TEST_LOAD_ENABLE;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.executeSQL;
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

    public static void createTable(Admin admin, TableName tableName, String... columnFamilies) throws IOException {
        HTableDescriptor htd = new HTableDescriptor(tableName);
        // Add column families
        for (String cf : columnFamilies) {
            HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes(cf));
            htd.addFamily(hcd);
        }
        // Create the table
        admin.createTable(htd);
    }

    @Test
    public void testAdminEnDisableTable() throws Exception {
        java.sql.Connection conn = ObHTableTestUtil.getConnection();
        Statement st = conn.createStatement();
        st.execute("CREATE DATABASE IF NOT EXISTS `en_dis`");
        st.close();
        conn.close();
        Configuration conf = ObHTableTestUtil.newConfiguration();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        try {
            createTable(admin, TableName.valueOf("test_en_dis_tb"), "cf1", "cf2", "cf3");
            createTable(admin, TableName.valueOf("en_dis", "test"), "cf1", "cf2", "cf3");
            assertTrue(admin.tableExists(TableName.valueOf("en_dis", "test")));
            assertTrue(admin.tableExists(TableName.valueOf("test_en_dis_tb")));
            String kvAttrStrDefault = "{\"Hbase\": {\"MaxVersions\": 1, \"CreatedBy\": \"Admin\"}}";
            String kvAttributeDisable = "{\"Hbase\": {\"MaxVersions\": 1, \"CreatedBy\": \"Admin\", \"State\": \"disable\"}}";
            String kvAttributeEnable = "{\"Hbase\": {\"MaxVersions\": 1, \"CreatedBy\": \"Admin\", \"State\": \"enable\"}}";

            checkKVAttributes("test_en_dis_tb$cf1", kvAttrStrDefault);
            checkKVAttributes("test_en_dis_tb$cf2", kvAttrStrDefault);
            checkKVAttributes("test_en_dis_tb$cf3", kvAttrStrDefault);
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
                checkKVAttributes("test_en_dis_tb$cf1", kvAttrStrDefault);
                checkKVAttributes("test_en_dis_tb$cf2", kvAttrStrDefault);
                checkKVAttributes("test_en_dis_tb$cf3", kvAttrStrDefault);

                batchInsert(10, "test_en_dis_tb");
                batchGet(10, "test_en_dis_tb");
            }

            // 3. disable a enable table
            {
                if (admin.isTableEnabled(TableName.valueOf("test_en_dis_tb"))) {
                    admin.disableTable(TableName.valueOf("test_en_dis_tb"));
                }
            checkKVAttributes("test_en_dis_tb$cf1", kvAttributeDisable);
            checkKVAttributes("test_en_dis_tb$cf2", kvAttributeDisable);
            checkKVAttributes("test_en_dis_tb$cf3", kvAttributeDisable);
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
                checkKVAttributes("test_en_dis_tb$cf1", kvAttributeEnable);
                checkKVAttributes("test_en_dis_tb$cf2", kvAttributeEnable);
                checkKVAttributes("test_en_dis_tb$cf3", kvAttributeEnable);
                // write an enabled table, should succeed
                batchInsert(10, "test_en_dis_tb");
                batchGet(10, "test_en_dis_tb");
            }

            // 5. enable an enabled table
            {
                if (admin.isTableDisabled(TableName.valueOf("en_dis", "test"))) {
                    admin.enableTable(TableName.valueOf("en_dis", "test"));
                }
                checkKVAttributes("en_dis:test$cf1", kvAttrStrDefault);
                checkKVAttributes("en_dis:test$cf2", kvAttrStrDefault);
                checkKVAttributes("en_dis:test$cf3", kvAttrStrDefault);
                try {
                    admin.enableTable(TableName.valueOf("en_dis", "test"));
                    Assert.fail();
                } catch (IOException ex) {
                    Assert.assertTrue(ex.getCause() instanceof ObTableException);
                    Assert.assertEquals(ResultCodes.OB_KV_TABLE_NOT_ENABLED.errorCode,
                            ((ObTableException) ex.getCause()).getErrorCode());
                }
            }

            // 6. disable a disabled table
            {
                if (admin.isTableEnabled(TableName.valueOf("en_dis", "test"))) {
                    admin.disableTable(TableName.valueOf("en_dis", "test"));
                }
                checkKVAttributes("en_dis:test$cf1", kvAttributeDisable);
                checkKVAttributes("en_dis:test$cf1", kvAttributeDisable);
                checkKVAttributes("en_dis:test$cf1", kvAttributeDisable);
                try {
                    admin.disableTable(TableName.valueOf("en_dis", "test"));
                    Assert.fail();
                } catch (IOException ex) {
                    Assert.assertTrue(ex.getCause() instanceof ObTableException);
                    Assert.assertEquals(ResultCodes.OB_KV_TABLE_NOT_DISABLED.errorCode,
                            ((ObTableException) ex.getCause()).getErrorCode());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            admin.deleteTable(TableName.valueOf("test_en_dis_tb"));
            assertFalse(admin.tableExists(TableName.valueOf("test_en_dis_tb")));
            admin.deleteTable(TableName.valueOf("en_dis", "test"));
            assertFalse(admin.tableExists(TableName.valueOf("en_dis", "test")));
        }
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
        // test tablegroup not existed
        IOException thrown = assertThrows(IOException.class,
                () -> {
                    admin.getRegionMetrics(null, TableName.valueOf("tablegroup_not_exists"));
                });
        Assert.assertTrue(thrown.getCause() instanceof ObTableException);
        Assert.assertEquals(ResultCodes.OB_TABLEGROUP_NOT_EXIST.errorCode, ((ObTableException) thrown.getCause()).getErrorCode());

        // test use serverName without tableName to get region metrics
        assertThrows(FeatureNotSupportedException.class,
                () -> {
                    admin.getRegionMetrics(ServerName.valueOf("localhost,1,1"));
                });

        // test single-thread getRegionMetrics after writing
        batchInsert(100000, tablegroup1);
        // test ServerName is any string
        long start = System.currentTimeMillis();
        List<RegionMetrics> metrics = admin.getRegionMetrics(ServerName.valueOf("localhost,1,1"), TableName.valueOf(tablegroup1));
        long cost = System.currentTimeMillis() - start;
        System.out.println("get region metrics time usage: " + cost + "ms, tablegroup: " + tablegroup1);
        assertEquals(30, metrics.size());

        // test getRegionMetrics concurrently reading while writing
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
                            long thrStart = System.currentTimeMillis();
                            regionMetrics = admin.getRegionMetrics(null, TableName.valueOf(tablegroup1));
                            long thrCost = System.currentTimeMillis() - thrStart;
                            System.out.println("task: " + taskId + ", get region metrics time usage: " + thrCost + "ms, tablegroup: " + tablegroup1);
                            if (regionMetrics.size() != 30) {
                                throw new ObTableGetException(
                                        "the number of region metrics does not match the number of tablets, the number of region metrics: " + regionMetrics.size());
                            }
                        } else {
                            long thrStart = System.currentTimeMillis();
                            regionMetrics = admin.getRegionMetrics(null, TableName.valueOf(tablegroup2));
                            long thrCost = System.currentTimeMillis() - thrStart;
                            System.out.println("task: " + taskId + ", get region metrics time usage: " + thrCost + "ms, tablegroup: " + tablegroup1);
                            if (regionMetrics.size() != 9) {
                                throw new ObTableGetException(
                                        "the number of region metrics does not match the number of tablets, the number of region metrics: " + regionMetrics.size());
                            }
                        }
                    } else {
                        try {
                            if (taskId % 8 == 0) {
                                batchInsert(1000, tablegroup2);
                            } else {
                                batchInsert(1000, tablegroup1);
                            }
                        } catch (Exception e) {
                            Exception originalCause = e;
                            while (originalCause.getCause() != null && originalCause.getCause() instanceof ObTableException) {
                                originalCause = (Exception) originalCause.getCause();
                            }
                            if (originalCause instanceof ObTableException && ((ObTableException) originalCause).getErrorCode() == ResultCodes.OB_TIMEOUT.errorCode) {
                                // ignore
                                System.out.println("taskId: " + taskId + " OB_TIMEOUT");
                            } else {
                                throw e;
                            }
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

        // test getRegionMetrics from non-partitioned table
        String non_part_tablegroup = "test_no_part";
        batchInsert(10000, non_part_tablegroup);
        start = System.currentTimeMillis();
        metrics = admin.getRegionMetrics(null, TableName.valueOf(non_part_tablegroup));
        cost = System.currentTimeMillis() - start;
        System.out.println("get region metrics time usage: " + cost + "ms, tablegroup: " + non_part_tablegroup);
        assertEquals(3, metrics.size());
    }

    @Test
    public void testAdminDeleteTable() throws Exception {
        java.sql.Connection conn = ObHTableTestUtil.getConnection();
        Statement st = conn.createStatement();
        st.execute("CREATE DATABASE IF NOT EXISTS `del_tb`");
        st.close();
        conn.close();
        Configuration conf = ObHTableTestUtil.newConfiguration();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        createTable(admin, TableName.valueOf("test_del_tb"), "cf1", "cf2", "cf3");
        createTable(admin, TableName.valueOf("del_tb", "test"), "cf1", "cf2", "cf3");
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
        st.execute("CREATE DATABASE IF NOT EXISTS `exist_tb`");
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
        createTable(admin, TableName.valueOf("test_exist_tb"), "cf1", "cf2", "cf3");
        createTable(admin, TableName.valueOf("exist_tb", "test"), "cf1", "cf2", "cf3");
        Assert.assertTrue(admin.tableExists(TableName.valueOf("test_exist_tb")));
        Assert.assertTrue(admin.tableExists(TableName.valueOf("exist_tb", "test")));
    }

    private void batchInsert(int rows, String tablegroup) throws Exception {
        byte[] family1 = Bytes.toBytes("cf1");
        byte[] family2 = Bytes.toBytes("cf2");
        byte[] family3 = Bytes.toBytes("cf3");
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
        List<HColumnDescriptor> originalColumnDescriptors = Arrays.asList(hcd1, hcd2, hcd3);

        // 1. generate create table task, one task per table
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < tableNums; i++) {
            int finalI = i;
            tasks.add(()->{
                HTableDescriptor htd = new HTableDescriptor(tableNames.get(finalI));
                List<HColumnDescriptor> columnDescriptors = new ArrayList<>(originalColumnDescriptors);
                // 随机打乱列族顺序
                Collections.shuffle(columnDescriptors);
                String shuffledCfNames = columnDescriptors.stream()
                        .map(hcd -> Bytes.toString(hcd.getName()))
                        .collect(Collectors.joining(", "));
                System.out.println("Table " + tableNames.get(finalI) + " shuffled column families: " + shuffledCfNames);
                for (HColumnDescriptor hcd : columnDescriptors) {
                    htd.addFamily(hcd);
                }
                try {
                    admin.createTable(htd);
                    System.out.println("success to create table:" + tableNames.get(finalI));
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
        List<Callable<Void>> disableTasks = new ArrayList<>();
        for (int i = 0; i < tableNums; i++) {
            int finalI = i;
            disableTasks.add(()->{
                try {
                    admin.disableTable(tableNames.get(finalI));
                    System.out.println("success to disable table:" + tableNames.get(finalI));
                } catch (Exception e) {
                    System.out.println(e);
                    if (!ignoreException) {
                        throw e;
                    }
                }
                return null;
            });
        }
        ExecutorService disExecutorService = Executors.newFixedThreadPool(tableNums);
        disExecutorService.invokeAll(disableTasks);
        disExecutorService.shutdown();
        disExecutorService.awaitTermination(1, TimeUnit.MINUTES);

        assertTrue(admin.isTableDisabled(tableNames.get(0)));
        for (int i = 0; i < tableNames.size(); i++) {
            TableName tableName = tableNames.get(i);
            assertTrue(admin.isTableDisabled(tableName));
        }

        // 5. generate delete table task
        List<Callable<Void>> delTasks = new ArrayList<>();
        for (int i = 0; i < tableNums; i++) {
            int finalI = i;
            delTasks.add(()->{
                try {
                    admin.deleteTable(tableNames.get(finalI));
                    System.out.println("success to drop table:" + tableNames.get(finalI));
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

    @Test
    public void testHTableDDLDefense() throws Exception {
        TableName tableName = TableName.valueOf("testHTableDefense");
        byte[] cf1 = Bytes.toBytes("cf1");
        byte[] cf2 = Bytes.toBytes("cf2");
        Configuration conf = ObHTableTestUtil.newConfiguration();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        // 1. construct htable desc and column family desc
        HColumnDescriptor hcd1 = new HColumnDescriptor(cf1);
        hcd1.setMaxVersions(2);
        hcd1.setTimeToLive(172800);
        HColumnDescriptor hcd2 = new HColumnDescriptor(cf2);
        hcd1.setMaxVersions(1);
        hcd1.setTimeToLive(86400);
        java.sql.Connection conn = ObHTableTestUtil.getConnection();

        // 2. execute create table and check exists
        try {
            HTableDescriptor htd = new HTableDescriptor(tableName);
            htd.addFamily(hcd1);
            htd.addFamily(hcd2);
            admin.createTable(htd);
            assertTrue(admin.tableExists(tableName));

            /// execute the following ddl stmt in created by admin table, should be prohibited
            // 3. alter table add constraint
            try {
                executeSQL(conn, "alter table testHTableDefense$cf1 ADD CONSTRAINT cons1 CHECK(T < 0)", true);
                fail();
            } catch (SQLException e) {
                Assert.assertEquals(1235, e.getErrorCode());
                Assert.assertEquals("table kv_attribute with '\"CreateBy\": \"Admin\"' not supported", e.getMessage());
            }

            // 4. alter table add index
            try {
                executeSQL(conn, "alter table testHTableDefense$cf1 ADD INDEX idx_1(T)", true);
                fail();
            } catch (SQLException e) {
                Assert.assertEquals(1235, e.getErrorCode());
                Assert.assertEquals("table kv_attribute with '\"CreateBy\": \"Admin\"' not supported", e.getMessage());
            }

            // 5. alter table modify column to lob
            try {
                executeSQL(conn, "alter table testHTableDefense$cf1 MODIFY COLUMN V LONGTEXT", true);
                fail();
            } catch (SQLException e) {
                Assert.assertEquals(1235, e.getErrorCode());
                Assert.assertEquals("table kv_attribute with '\"CreateBy\": \"Admin\"' not supported", e.getMessage());
            }

            // 6. alter hbase admin table add fk
            try {
                executeSQL(conn, "alter table testHTableDefense$cf1 ADD CONSTRAINT hbase_fk_1 FOREIGN KEY(K) REFERENCES testHTableDefense$cf2(K)", true);
                fail();
            } catch (SQLException e) {
                Assert.assertEquals(1235, e.getErrorCode());
                Assert.assertEquals("table kv_attribute with '\"CreateBy\": \"Admin\"' not supported", e.getMessage());
            }

            // 7. create a normal table to refer to hbase admin table
            try {
                executeSQL(conn, "create table  testHTableDefense_t1(a varbinary(1024) primary key, FOREIGN KEY(a) REFERENCES testHTableDefense$cf1(K));" , true);
                fail();
            } catch (SQLException e) {
                Assert.assertEquals(1235, e.getErrorCode());
                Assert.assertEquals("table kv_attribute with '\"CreateBy\": \"Admin\"' not supported", e.getMessage());
            }

            // 8. alter a normal table to refer to hbase admin table
            try {
                executeSQL(conn, "create table testHTableDefense_t2(a varbinary(1024) primary key)", true);
                executeSQL(conn, "alter table testHTableDefense_t2 ADD CONSTRAINT hbase_fk_1 FOREIGN KEY(a) REFERENCES testHTableDefense$cf1(K);", true);
                fail();
            } catch (SQLException e) {
                Assert.assertEquals(1235, e.getErrorCode());
                Assert.assertEquals("table kv_attribute with '\"CreateBy\": \"Admin\"' not supported", e.getMessage());
            }
            // 9. create a normal table A to refer to a table mock parent table B, and create table B using hbase admin
            try {
                executeSQL(conn, "SET foreign_key_checks = 0", true);

                executeSQL(conn, "create table testHTableDefense_t3(a varbinary(1024) primary key, FOREIGN KEY(a) REFERENCES testHTableDefense2$cf1(K));", true);
                HTableDescriptor htd2 = new HTableDescriptor(TableName.valueOf("testHTableDefense2"));
                HColumnDescriptor hcd4 = new HColumnDescriptor("cf1".getBytes());
                hcd4.setMaxVersions(2);
                hcd4.setTimeToLive(172800);
                htd2.addFamily(hcd4);
                admin.createTable(htd2);
                fail();
            } catch (Exception e) {
                Assert.assertEquals(-4007, ((ObTableException)e.getCause()).getErrorCode());
            }


            // 10. create trigger
            try {
                executeSQL(conn, " CREATE TRIGGER hbase_trigger_1" +
                             " AFTER INSERT ON testHTableDefense$cf1 FOR EACH ROW" +
                             " BEGIN END", true);
                fail();
            } catch (SQLException e) {
                Assert.assertEquals(1235, e.getErrorCode());
                Assert.assertEquals("table kv_attribute with '\"CreateBy\": \"Admin\"' not supported", e.getMessage());
            }

            // 11. create view
            try {
                executeSQL(conn, " CREATE VIEW hbase_view_1 as select * from testHTableDefense$cf1", true);
                fail();
            } catch (SQLException e) {
                Assert.assertEquals(1235, e.getErrorCode());
                Assert.assertEquals("table kv_attribute with '\"CreateBy\": \"Admin\"' not supported", e.getMessage());
            }

            // 12. alter view
            try {
                executeSQL(conn, "ALTER VIEW hbase_view_1 as select * from testHTableDefense$cf1", true);
                fail();
            } catch (SQLException e) {
                Assert.assertEquals(1235, e.getErrorCode());
                Assert.assertEquals("table kv_attribute with '\"CreateBy\": \"Admin\"' not supported", e.getMessage());
            }

            // 13. create index
            try {
                executeSQL(conn, " CREATE INDEX testHTableDefense$cf1_idx_T on testHTableDefense$cf1(T)", true);
                fail();
            } catch (SQLException e) {
                Assert.assertEquals(1235, e.getErrorCode());
                Assert.assertEquals("table kv_attribute with '\"CreateBy\": \"Admin\"' not supported", e.getMessage());
            }


            // 14. explicit create table and specify created_by:admin, should be prohibited
            try {
                executeSQL(conn, "CREATE TABLE testHTableDefense$cf3(a int primary key) kv_attributes ='{\"Hbase\": {\"CreatedBy\": \"Admin\"}}'", true);
                fail();
            } catch (SQLException e) {
                Assert.assertEquals(1235, e.getErrorCode());
                Assert.assertEquals("table kv_attribute with '\"CreateBy\": \"Admin\"' not supported", e.getMessage());
            }

            // 15. alter table to created_by:admin, should be prohibited
            try {
                executeSQL(conn, "CREATE TABLE testHTableDefense$cf3(a int primary key)", true);
                executeSQL(conn, "alter table testHTableDefense$cf3 kv_attributes ='{\"Hbase\": {\"CreatedBy\": \"Admin\"}}'", true);
                fail();
            } catch (SQLException e) {
                Assert.assertEquals(1235, e.getErrorCode());
                Assert.assertEquals("alter table kv attributes to created by admin not supported", e.getMessage());
                // clean table
                String sql3 = "drop table if exists testHTableDefense$cf3";
                System.out.println("execute sql: " + sql3);
                conn.createStatement().execute(sql3);
            }

            // 16. disable a htable did not created by admin is not suppported
            try {
                executeSQL(conn, "CREATE TABLEGROUP IF NOT EXISTS testHTableDefense2", true);
                executeSQL(conn, "CREATE TABLE IF NOT EXISTS testHTableDefense2$cf4(a int primary key) kv_attributes ='{\"Hbase\": {}}' TABLEGROUP=testHTableDefense2", true);
                admin.disableTable(TableName.valueOf("testHTableDefense2"));
                fail();
            } catch (Exception e) {
                Assert.assertEquals(-4007, ((ObTableException)e.getCause()).getErrorCode());
            }

            // 17. delete a htable did not created by admin is not suppported
            try {
                executeSQL(conn, "CREATE TABLEGROUP IF NOT EXISTS testHTableDefense2", true);
                executeSQL(conn,
                        "CREATE TABLE IF NOT EXISTS testHTableDefense2$cf5(a int primary key) kv_attributes ='{\"Hbase\": {}}' TABLEGROUP=testHTableDefense2", true);
                admin.deleteTable(TableName.valueOf("testHTableDefense2"));
                fail();
            } catch (Exception e) {
                Assert.assertEquals(-4007, ((ObTableException)e.getCause()).getErrorCode());
            }

        } catch (Exception e) {
           e.printStackTrace();
           assertTrue(false);
        } finally {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            executeSQL(conn, "DROP TABLE IF EXISTS testHTableDefense2$cf4", true);
            executeSQL(conn, "DROP TABLE IF EXISTS testHTableDefense2$cf5", true);
            executeSQL(conn, "DROP TABLEGROUP IF EXISTS testHTableDefense2", true);
            executeSQL(conn, "DROP TABLE IF EXISTS testHTableDefense_t1", true);
            executeSQL(conn, "DROP TABLE IF EXISTS testHTableDefense_t2", true);
            executeSQL(conn, "DROP TABLE IF EXISTS testHTableDefense_t3", true);
        }
    }

    void checkKVAttributes(String tableName, String kvAttributes) throws Exception {
        java.sql.Connection conn = ObHTableTestUtil.getConnection();
        java.sql.ResultSet resultSet = conn.createStatement().executeQuery("select kv_attributes from oceanbase.__all_table where table_name = '" + tableName + "'");
        resultSet.next();
        String value = resultSet.getString(1);
        Assert.assertEquals(kvAttributes, value);
        Assert.assertFalse(resultSet.next());
    }
}
