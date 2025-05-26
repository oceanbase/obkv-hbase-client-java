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
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.Statement;
import java.util.concurrent.Executors;

import static com.alipay.oceanbase.hbase.constants.OHConstants.HBASE_HTABLE_TEST_LOAD_ENABLE;
import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

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
    public void testAdminDeleteTable() throws Exception {
        java.sql.Connection conn = ObHTableTestUtil.getConnection();
        Statement st = conn.createStatement();
        st.execute("CREATE TABLEGROUP IF NOT EXISTS test_multi_cf SHARDING = 'ADAPTIVE';\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `test_multi_cf$family_with_group1` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = test_multi_cf PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `test_multi_cf$family_with_group2` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = test_multi_cf PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `test_multi_cf$family_with_group3` (\n" +
                "    `K` varbinary(1024) NOT NULL,\n" +
                "    `Q` varbinary(256) NOT NULL,\n" +
                "    `T` bigint(20) NOT NULL,\n" +
                "    `V` varbinary(1024) DEFAULT NULL,\n" +
                "    PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = test_multi_cf PARTITION BY KEY(`K`) PARTITIONS 3;\n" +
                "\n" +
                "CREATE DATABASE IF NOT EXISTS `n1`;\n" +
                "use `n1`;\n" +
                "CREATE TABLEGROUP IF NOT EXISTS `n1:test` SHARDING = 'ADAPTIVE';\n" +
                "CREATE TABLE IF NOT EXISTS `n1:test$family_group` (\n" +
                "      `K` varbinary(1024) NOT NULL,\n" +
                "      `Q` varbinary(256) NOT NULL,\n" +
                "      `T` bigint(20) NOT NULL,\n" +
                "      `V` varbinary(1024) DEFAULT NULL,\n" +
                "      PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = `n1:test`;" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS `n1:test$family1` (\n" +
                "      `K` varbinary(1024) NOT NULL,\n" +
                "      `Q` varbinary(256) NOT NULL,\n" +
                "      `T` bigint(20) NOT NULL,\n" +
                "      `V` varbinary(1024) DEFAULT NULL,\n" +
                "      PRIMARY KEY (`K`, `Q`, `T`)\n" +
                ") TABLEGROUP = `n1:test`;");
        Configuration conf = ObHTableTestUtil.newConfiguration();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        assertTrue(admin.tableExists(TableName.valueOf("n1", "test")));
        assertTrue(admin.tableExists(TableName.valueOf("test_multi_cf")));
        IOException thrown = assertThrows(IOException.class,
                () -> {
                    admin.deleteTable(TableName.valueOf("tablegroup_not_exists"));
                });
        Assert.assertTrue(thrown.getCause() instanceof ObTableException);
        Assert.assertEquals(ResultCodes.OB_TABLEGROUP_NOT_EXIST.errorCode, ((ObTableException) thrown.getCause()).getErrorCode());
        admin.deleteTable(TableName.valueOf("n1", "test"));
        admin.deleteTable(TableName.valueOf("test_multi_cf"));
        assertFalse(admin.tableExists(TableName.valueOf("n1", "test")));
        assertFalse(admin.tableExists(TableName.valueOf("test_multi_cf")));
    }

    @Test
    public void testAdminTableExists() throws Exception {
        Configuration conf = ObHTableTestUtil.newConfiguration();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        Assert.assertThrows(IllegalArgumentException.class,
                () -> {
                    admin.tableExists(TableName.valueOf(""));
                });
        Assert.assertFalse(admin.tableExists(TableName.valueOf("tablegroup_not_exists")));
        Assert.assertTrue(admin.tableExists(TableName.valueOf("test_multi_cf")));
    }
}
