/*-
 * #%L
 * OBKV HBase Client Framework
 * %%
 * Copyright (C) 2025 OceanBase Group
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

package com.alipay.oceanbase.hbase.secondary;

import com.alipay.oceanbase.hbase.OHTable;
import com.alipay.oceanbase.hbase.OHTableClient;
import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import com.alipay.oceanbase.hbase.util.TableTemplateManager;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static com.alipay.oceanbase.hbase.util.TableTemplateManager.NORMAL_TABLES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OHTableSecondaryPartIncrementTest {
    private static List<String>              tableNames       = new LinkedList<String>();
    private static Map<String, List<String>> group2tableNames = null;

    @BeforeClass
    public static void before() throws Exception {
        openDistributedExecute();
        for (TableTemplateManager.TableType type : NORMAL_TABLES) {
            createTables(type, tableNames, group2tableNames, true);
        }
    }

    @AfterClass
    public static void finish() throws Exception {
        closeDistributedExecute();
        //        dropTables(tableNames, group2tableNames);
    }

    @Before
    public void prepareCase() throws Exception {
        truncateTables(tableNames, group2tableNames);
    }

    public static void testIncrement(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        byte[] cf = getColumnFamilyName(tableName).getBytes();
        byte[] row = Bytes.toBytes("rk");
        byte[] qualifier = Bytes.toBytes("qual");
        byte[] qualifier2 = Bytes.toBytes("qual2");
        byte[] val = Bytes.toBytes(0L);
        Put p = new Put(row);
        p.add(cf, qualifier, val);
        hTable.put(p);

        for (int count = 0; count < 13; count++) {
            Increment inc = new Increment(row);
            inc.addColumn(cf, qualifier, 100L);
            hTable.increment(inc);
        }
        Get get = new Get(row);
        get.setMaxVersions(1);
        get.addFamily(cf);
        Result result = hTable.get(get);
        assertEquals(1300L, Bytes.toLong(result.raw()[0].getValue()));
        get.setMaxVersions(100);
        result = hTable.get(get);
        assertEquals(14, result.size());

        Increment inc = new Increment(row);
        inc.addColumn(cf, qualifier, -100L);
        inc.addColumn(cf, qualifier2, -100L);
        hTable.increment(inc);
        get.setMaxVersions(1);
        result = hTable.get(get);
        assertEquals(1200L, Bytes.toLong(result.getColumnCells(cf, qualifier).get(0).getValue()));
        assertEquals(-100L, Bytes.toLong(result.getColumnCells(cf, qualifier2).get(0).getValue()));
        hTable.close();
    }

    private static void testIncBorder(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        byte[] FAMILY = getColumnFamilyName(tableName).getBytes();
        byte[] ROW = "incKey".getBytes();
        byte[] v1 = Bytes.toBytes("ab");
        byte[][] QUALIFIERS = new byte[][] { Bytes.toBytes("b"), Bytes.toBytes("a"),
                Bytes.toBytes("c") };
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIERS[1], v1);
        hTable.put(put);
        Increment inc = new Increment(ROW);
        inc.addColumn(FAMILY, QUALIFIERS[1], 2L);
        try {
            hTable.increment(inc);
        } catch (Exception e) {
            assertTrue(e.getCause().getMessage().contains("OB_KV_HBASE_INCR_FIELD_IS_NOT_LONG"));
        }
        Get get = new Get(ROW);
        get.setMaxVersions(10);
        get.addFamily(FAMILY);
        Result result = hTable.get(get);
        assertEquals(1, result.size());
        byte[] ROW1 = "incKey1".getBytes();
        inc = new Increment(ROW1);
        inc.addColumn(FAMILY, QUALIFIERS[1], 2L);
        hTable.increment(inc);
        get = new Get(ROW1);
        get.setMaxVersions(10);
        get.addFamily(FAMILY);
        result = hTable.get(get);
        assertEquals(1, result.size());
        assertEquals(2L, Bytes.toLong(result.raw()[0].getValue()));
        inc.addColumn(FAMILY, QUALIFIERS[0], 2L);
        hTable.increment(inc);
        get.setMaxVersions(10);
        get.addFamily(FAMILY);
        result = hTable.get(get);
        assertEquals(3, result.size());
        assertEquals(4L,
            Bytes.toLong(result.getColumnCells(FAMILY, QUALIFIERS[1]).get(0).getValue()));
        assertEquals(2L,
            Bytes.toLong(result.getColumnCells(FAMILY, QUALIFIERS[1]).get(1).getValue()));
        assertEquals(2L,
            Bytes.toLong(result.getColumnCells(FAMILY, QUALIFIERS[0]).get(0).getValue()));
        hTable.close();
    }

    private static void testIncCon(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        byte[] FAMILY = getColumnFamilyName(tableName).getBytes();
        String column = "incColumn";
        byte[] ROW = "incKey".getBytes();
        long expect = 0;
        ThreadPoolExecutor threadPoolExecutor = OHTable.createDefaultThreadPoolExecutor(1, 100,100);
        AtomicInteger atomicInteger = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(100);
        for (int i = 0; i < 100; i++) {
            Increment inc = new Increment(ROW);
            inc.addColumn(FAMILY, column.getBytes(), 2L);
            threadPoolExecutor.submit(() -> {
                try {
                    hTable.increment(inc);
                    atomicInteger.incrementAndGet();
                } catch (Exception e) {
                    if (!e.getCause().getMessage().contains("OB_TRY_LOCK_ROW_CONFLICT") && !e.getCause().getMessage().contains("OB_TIMEOUT")) {
                        throw new RuntimeException(e);
                    }
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await(100000, TimeUnit.MILLISECONDS);
        for (int i = 0; i < atomicInteger.get(); i++) {
            expect += 2;
        }
        Get get = new Get(ROW);
        get.setMaxVersions(1);
        get.addColumn(FAMILY, column.getBytes());
        Result result = hTable.get(get);
        assertEquals(expect, Bytes.toLong(result.getColumnCells(FAMILY, column.getBytes()).get(0).getValue()));
        hTable.close();
    }

    @Test
    public void testIncrement() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartIncrementTest::testIncrement);
    }

    @Test
    public void testBorderInc() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartIncrementTest::testIncBorder);
    }

    @Test
    public void testIncConcurrency() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartIncrementTest::testIncCon);
    }
}
