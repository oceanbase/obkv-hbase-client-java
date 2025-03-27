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
import com.google.common.base.Strings;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.getConnection;
import static com.alipay.oceanbase.hbase.util.TableTemplateManager.NORMAL_TABLES;
import static org.junit.Assert.*;

public class OHTableSecondaryPartAppendTest {
    private static List<String>              tableNames       = new LinkedList<String>();
    private static Map<String, List<String>> group2tableNames = new LinkedHashMap<String, List<String>>();

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
    }

    @Before
    public void prepareCase() throws Exception {
        truncateTables(tableNames, group2tableNames);
    }

    private static void assertNullResult(Result result) throws Exception {
        assertTrue("expected null result but received a non-null result", result == null);
    }

    private static void testAppend(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        try {
            byte[] FAMILY = getColumnFamilyName(tableName).getBytes();
            byte[] ROW = "appendKey".getBytes();
            byte[] v1 = Bytes.toBytes("42");
            byte[] v2 = Bytes.toBytes("23");
            byte[][] QUALIFIERS = new byte[][] { Bytes.toBytes("b"), Bytes.toBytes("a"),
                    Bytes.toBytes("c") };
            Append a = new Append(ROW);
            a.add(FAMILY, QUALIFIERS[0], v1);
            a.add(FAMILY, QUALIFIERS[1], v2);
            a.setReturnResults(false);
            assertNullResult(hTable.append(a));

            a = new Append(ROW);
            a.add(FAMILY, QUALIFIERS[0], v2);
            a.add(FAMILY, QUALIFIERS[1], v1);
            a.add(FAMILY, QUALIFIERS[2], v2);
            Result r = hTable.append(a);
            assertEquals(0, Bytes.compareTo(Bytes.add(v1, v2), r.getValue(FAMILY, QUALIFIERS[0])));
            assertEquals(0, Bytes.compareTo(Bytes.add(v2, v1), r.getValue(FAMILY, QUALIFIERS[1])));
            // QUALIFIERS[2] previously not exist, verify both value and timestamp are correct
            assertEquals(0, Bytes.compareTo(v2, r.getValue(FAMILY, QUALIFIERS[2])));
            assertEquals(r.getColumnLatest(FAMILY, QUALIFIERS[0]).getTimestamp(), r
                .getColumnLatest(FAMILY, QUALIFIERS[2]).getTimestamp());

            Get get = new Get(ROW);
            get.setMaxVersions(10);
            get.addFamily(FAMILY);
            Result result = hTable.get(get);
            assertEquals(2, result.getColumnCells(FAMILY, QUALIFIERS[0]).size());
            assertEquals(2, result.getColumnCells(FAMILY, QUALIFIERS[1]).size());
            assertEquals(1, result.getColumnCells(FAMILY, QUALIFIERS[2]).size());
            assertEquals(
                0,
                Bytes.compareTo(Bytes.add(v1, v2), result.getColumnCells(FAMILY, QUALIFIERS[0])
                    .get(0).getValue()));
            assertEquals(0,
                Bytes.compareTo(v2, result.getColumnCells(FAMILY, QUALIFIERS[2]).get(0).getValue()));
        } finally {
            hTable.close();
        }
    }

    private static void testAppendBorder(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        try {
            byte[] FAMILY = getColumnFamilyName(tableName).getBytes();
            byte[] ROW = "appendKey".getBytes();
            byte[] v1 = Bytes.toBytes("ab");
            byte[][] QUALIFIERS = new byte[][] { Bytes.toBytes("b"), Bytes.toBytes("a"),
                    Bytes.toBytes("c") };
            Put put = new Put(ROW);
            put.addColumn(FAMILY, QUALIFIERS[1], v1);
            hTable.put(put);
            Append a = new Append(ROW);
            a.add(FAMILY, QUALIFIERS[1], v1);
            a.add(FAMILY, QUALIFIERS[2], "".getBytes());
            hTable.append(a);
            Get get = new Get(ROW);
            get.setMaxVersions(10);
            get.addFamily(FAMILY);
            Result result = hTable.get(get);
            assertEquals(3, result.size());

            a = new Append(ROW);
            a.add(FAMILY, QUALIFIERS[2], v1);
            a.add(FAMILY, QUALIFIERS[2], "".getBytes());
            hTable.append(a);
            get = new Get(ROW);
            get.setMaxVersions(10);
            get.addFamily(FAMILY);
            result = hTable.get(get);
            assertEquals(4, result.size());

            byte[] randomBytes = new byte[1025];
            Random random = new Random();
            random.nextBytes(randomBytes);
            a = new Append(ROW);
            a.add(FAMILY, QUALIFIERS[2], randomBytes);
            try {
                hTable.append(a);
                fail("unexpect error, too long data should fail");
            } catch (IOException e) {
                assertTrue(e.getCause().getMessage().contains("Data too long for column 'V'"));
            }
        } finally {
            hTable.close();
        }

    }

    private static void testAppendCon(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        try {
            hTable.init();
            byte[] FAMILY = getColumnFamilyName(tableName).getBytes();
            String column = "appColumn";
            byte[] ROW = "appendKey".getBytes();
            byte[] v = "a".getBytes();
            ThreadPoolExecutor threadPoolExecutor = OHTable.createDefaultThreadPoolExecutor(1, 100, 100);
            AtomicInteger atomicInteger = new AtomicInteger(0);
            CountDownLatch countDownLatch = new CountDownLatch(100);

            for (int i = 0; i < 100; i++) {
                Append append = new Append(ROW);
                append.add(FAMILY, column.getBytes(), v);
                threadPoolExecutor.submit(() -> {
                    try {
                        hTable.append(append);
                    } catch (Exception e) {
                        if (!e.getCause().getMessage().contains("OB_TRY_LOCK_ROW_CONFLICT") 
                            && !e.getCause().getMessage().contains("OB_TIMEOUT")) {
                            throw new RuntimeException(e);
                        }
                    } finally {
                        atomicInteger.incrementAndGet();
                        countDownLatch.countDown();
                    }
                });
            }
            threadPoolExecutor.shutdown();
            countDownLatch.await(100000, TimeUnit.MILLISECONDS);
            final byte[] expect = Strings.repeat("a", atomicInteger.get()).getBytes();
            System.out.println("atomicInteger: " + atomicInteger.get());
            Get get = new Get(ROW);
            get.setMaxVersions(1);
            get.addColumn(FAMILY, column.getBytes());
            Result result = hTable.get(get);
            ObHTableTestUtil.Assert(tableName, ()-> assertTrue(0 <= Bytes.compareTo(expect, result.getColumnCells(FAMILY, column.getBytes()).get(0).getValue())));
        } finally {
            hTable.close();
        }
    }

    private static void testAppendMultiCF(Map.Entry<String, List<String>> entry) throws Exception {
        String groupName = getTableName(entry.getKey());
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(groupName);
        hTable.init();
        List<String> tableNames = entry.getValue();
        String column = "appColumn";
        byte[] ROW = "appendKey".getBytes();
        byte[] v = "a".getBytes();
        Append append = new Append(ROW);
        for (String tableName : tableNames) {
            byte[] FAMILY = getColumnFamilyName(tableName).getBytes();
            append.add(FAMILY, column.getBytes(), v);
        }
        try {
            hTable.append(append);
            fail("unexpect error, append should not support multi cf");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("multi family is not supported"));
        }
        hTable.close();
    }

    private static void testAppendSeires(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        byte[] FAMILY = getColumnFamilyName(tableName).getBytes();
        String column = "appColumn";
        byte[] ROW = "appendKey".getBytes();
        byte[] v = "a".getBytes();
        Append append = new Append(ROW);
        append.add(FAMILY, column.getBytes(), v);
        try {
            hTable.append(append);
            fail("unexpect error, append should not support series table");
        } catch (Exception e) {
            assertTrue(e.getCause().getMessage()
                .contains("query and mutate with hbase series type not supported"));
        }
        hTable.close();
    }

    @Test
    public void testAppend() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartAppendTest::testAppend);
    }

    @Test
    public void testBorderAppend() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartAppendTest::testAppendBorder);
    }

    @Test
    public void testAppendConcurrency() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartAppendTest::testAppendCon);
    }

    @Test
    public void testAppendMultiCF() throws Throwable {
        FOR_EACH(group2tableNames, OHTableSecondaryPartAppendTest::testAppendMultiCF);
    }

    @Test
    public void testAppendSeires() throws Throwable {
        List<String> series_tables = new LinkedList<String>();
        createTables(TableTemplateManager.TableType.SECONDARY_PARTITIONED_TIME_RANGE_KEY, series_tables, null, true);
        FOR_EACH(series_tables, OHTableSecondaryPartAppendTest::testAppendSeires);
    }
}
