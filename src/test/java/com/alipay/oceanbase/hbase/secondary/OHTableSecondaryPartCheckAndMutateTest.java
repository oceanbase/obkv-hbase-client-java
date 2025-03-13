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

import com.alipay.oceanbase.hbase.OHTableClient;
import com.alipay.oceanbase.hbase.util.ObHTableTestUtil;
import com.alipay.oceanbase.hbase.util.TableTemplateManager;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static org.junit.Assert.*;


public class OHTableSecondaryPartCheckAndMutateTest {
    private static List<String>              tableNames       = new LinkedList<String>();
    private static Map<String, List<String>> group2tableNames = new LinkedHashMap<>();
    private static byte []                   ROW              = Bytes.toBytes("testRow");
    private static byte []                   QUALIFIER        = Bytes.toBytes("testQualifier");
    private static byte []                   VALUE_1          = Bytes.toBytes("testValue");
    private static byte []                   ROW_1            = Bytes.toBytes("testRow1");
    private static byte []                   VALUE_2          = Bytes.toBytes("abcd");


    @BeforeClass
    public static void before() throws Exception {
        openDistributedExecute();
        for (TableTemplateManager.TableType type : TableTemplateManager.TableType.values()) {
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

    public static void testCheckAndMutate(String tableName) throws Throwable {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        final byte[] ROW = Bytes.toBytes("12345");
        final byte[] FAMILY = getColumnFamilyName(tableName).getBytes();
        Put put = new Put(ROW);
        put.add(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"));
        put.add(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"));
        put.add(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"));
        hTable.put(put);
        // get row back and assert the values
        Get get = new Get(ROW);
        Result result = hTable.get(get);
        assertTrue("Column A value should be a",
                Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("A"))).equals("a"));
        assertTrue("Column B value should be b",
                Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))).equals("b"));
        assertTrue("Column C value should be c",
                Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("C"))).equals("c"));

        // put the same row again with C column deleted
        RowMutations rm = new RowMutations(ROW);
        put = new Put(ROW);
        put.add(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"));
        put.add(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"));
        rm.add(put);
        Delete del = new Delete(ROW);
        del.deleteColumn(FAMILY, Bytes.toBytes("C"));
        rm.add(del);
        boolean res = hTable.checkAndMutate(ROW, FAMILY, Bytes.toBytes("A"), CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("a"), rm);
        assertTrue(res);

        // get row back and assert the values
        get = new Get(ROW);
        get.addFamily(FAMILY);
        result = hTable.get(get);
        assertTrue("Column A value should be a",
                Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("A"))).equals("a"));
        assertTrue("Column B value should be b",
                Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))).equals("b"));
        assertTrue("Column C should not exist",
                result.getValue(FAMILY, Bytes.toBytes("C")) == null);

        //Test that we get a hTable level exception
        try {
            Put p = new Put(ROW);
            p.add(new byte[]{'b', 'o', 'g', 'u', 's'}, new byte[]{'A'},  new byte[0]);
            rm = new RowMutations(ROW);
            rm.add(p);
            hTable.checkAndMutate(ROW, FAMILY, Bytes.toBytes("A"), CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes("a"), rm);
            fail("Expected NoSuchColumnFamilyException");
        } catch (RetriesExhaustedWithDetailsException e) {
            try {
                throw e.getCause(0);
            } catch (NoSuchColumnFamilyException e1) {
                // expected
            }
        }
        hTable.close();
    }

    public static void testCheckAndMutateDiffRow(String tableName) throws Throwable {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        byte[] family = getColumnFamilyName(tableName).getBytes();
        byte[] row1 = Bytes.toBytes("row1");
        byte[] qualifier = Bytes.toBytes("q1");
        byte[] value1 = Bytes.toBytes("value1");

        Put put = new Put(row1);
        put.addColumn(family, qualifier, value1);
        hTable.put(put);

        Result result = hTable.get(new Get(row1));
        assertArrayEquals("the value of column q in row1 should be value1",
                value1, result.getValue(family, qualifier));

        byte[] row2 = Bytes.toBytes("row2");
        byte[] value2 = Bytes.toBytes("value2");
        RowMutations mutations = new RowMutations(row2);
        put = new Put(row2);
        put.addColumn(family, qualifier, value2);
        mutations.add(put);

        // check row1 and put row2
        assertTrue(hTable.checkAndMutate(row1, family, qualifier,
                CompareFilter.CompareOp.GREATER, value2, mutations));

        result = hTable.get(new Get(row2));
        assertArrayEquals("the value of column q in row2 should be value2",
                value2, result.getValue(family, qualifier));
        hTable.close();
    }

    public static void testCheckAndPut(String tableName) throws Throwable {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        byte [] FAMILY = getColumnFamilyName(tableName).getBytes();

        Put put1 = new Put(ROW);
        put1.add(FAMILY, QUALIFIER, VALUE_1);

        // row doesn't exist, so using non-null value should be considered "not match".
        boolean ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, VALUE_1, put1);
        assertEquals(ok, false);

        // row doesn't exist, so using "null" to check for existence should be considered "match".
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, null, put1);
        assertEquals(ok, true);

        // row now exists, so using "null" to check for existence should be considered "not match".
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, null, put1);
        assertEquals(ok, false);

        Put put2 = new Put(ROW);
        put2.add(FAMILY, QUALIFIER, VALUE_2);

        // row now exists, use the matching value to check
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, VALUE_1, put2);
        assertEquals(ok, true);

        Put put3 = new Put(ROW_1);
        put3.add(FAMILY, QUALIFIER, VALUE_1);

        // try to do CheckAndPut on different rows
        try {
            ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, VALUE_2, put3);
            fail("trying to check and modify different rows should have failed.");
        } catch(Exception e) {}
        hTable.close();
    }

    public static void testCheckAndPutWithCompareOp(String tableName) throws Throwable {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        final byte [] value1 = Bytes.toBytes("aaaa");
        final byte [] value2 = Bytes.toBytes("bbbb");
        final byte [] value3 = Bytes.toBytes("cccc");
        final byte [] value4 = Bytes.toBytes("dddd");
        byte [] FAMILY = getColumnFamilyName(tableName).getBytes();

        Put put2 = new Put(ROW);
        put2.add(FAMILY, QUALIFIER, value2);

        Put put3 = new Put(ROW);
        put3.add(FAMILY, QUALIFIER, value3);

        // row doesn't exist, so using "null" to check for existence should be considered "match".
        boolean ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, null, put2);
        assertEquals(ok, true);

        // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
        // turns out "match"
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.GREATER, value1, put2);
        assertEquals(ok, false);
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.EQUAL, value1, put2);
        assertEquals(ok, false);
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.GREATER_OR_EQUAL, value1, put2);
        assertEquals(ok, false);
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.LESS, value1, put2);
        assertEquals(ok, true);
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.LESS_OR_EQUAL, value1, put2);
        assertEquals(ok, true);
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.NOT_EQUAL, value1, put3);
        assertEquals(ok, true);

        // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
        // turns out "match"
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.LESS, value4, put3);
        assertEquals(ok, false);
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.LESS_OR_EQUAL, value4, put3);
        assertEquals(ok, false);
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.EQUAL, value4, put3);
        assertEquals(ok, false);
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.GREATER, value4, put3);
        assertEquals(ok, true);
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.GREATER_OR_EQUAL, value4, put3);
        assertEquals(ok, true);
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.NOT_EQUAL, value4, put2);
        assertEquals(ok, true);

        // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
        // turns out "match"
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.GREATER, value2, put2);
        assertEquals(ok, false);
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.NOT_EQUAL, value2, put2);
        assertEquals(ok, false);
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.LESS, value2, put2);
        assertEquals(ok, false);
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.GREATER_OR_EQUAL, value2, put2);
        assertEquals(ok, true);
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.LESS_OR_EQUAL, value2, put2);
        assertEquals(ok, true);
        ok = hTable.checkAndPut(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.EQUAL, value2, put3);
        assertEquals(ok, true);
    }

    public static void testCheckAndDeleteWithCompareOp(String tableName) throws Throwable {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();
        final byte [] value1 = Bytes.toBytes("aaaa");
        final byte [] value2 = Bytes.toBytes("bbbb");
        final byte [] value3 = Bytes.toBytes("cccc");
        final byte [] value4 = Bytes.toBytes("dddd");
        byte [] FAMILY = getColumnFamilyName(tableName).getBytes();

        Put put2 = new Put(ROW);
        put2.add(FAMILY, QUALIFIER, value2);
        hTable.put(put2);

        Put put3 = new Put(ROW);
        put3.add(FAMILY, QUALIFIER, value3);

        Delete delete = new Delete(ROW);
        delete.deleteColumns(FAMILY, QUALIFIER);

        // cell = "bbbb", using "aaaa" to compare only LESS/LESS_OR_EQUAL/NOT_EQUAL
        // turns out "match"
        boolean ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.GREATER, value1, delete);
        assertEquals(ok, false);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.EQUAL, value1, delete);
        assertEquals(ok, false);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.GREATER_OR_EQUAL, value1, delete);
        assertEquals(ok, false);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.LESS, value1, delete);
        assertEquals(ok, true);
        hTable.put(put2);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.LESS_OR_EQUAL, value1, delete);
        assertEquals(ok, true);
        hTable.put(put2);

        assertEquals(ok, true);

        // cell = "cccc", using "dddd" to compare only LARGER/LARGER_OR_EQUAL/NOT_EQUAL
        // turns out "match"
        hTable.put(put3);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.LESS, value4, delete);

        assertEquals(ok, false);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.LESS_OR_EQUAL, value4, delete);

        assertEquals(ok, false);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.EQUAL, value4, delete);

        assertEquals(ok, false);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.GREATER, value4, delete);

        assertEquals(ok, true);
        hTable.put(put3);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.GREATER_OR_EQUAL, value4, delete);
        assertEquals(ok, true);
        hTable.put(put3);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.NOT_EQUAL, value4, delete);

        assertEquals(ok, true);

        // cell = "bbbb", using "bbbb" to compare only GREATER_OR_EQUAL/LESS_OR_EQUAL/EQUAL
        // turns out "match"
        hTable.put(put2);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.GREATER, value2, delete);
        assertEquals(ok, false);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.NOT_EQUAL, value2, delete);
        assertEquals(ok, false);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.LESS, value2, delete);
        assertEquals(ok, false);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.GREATER_OR_EQUAL, value2, delete);
        assertEquals(ok, true);
        hTable.put(put2);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.LESS_OR_EQUAL, value2, delete);
        assertEquals(ok, true);
        hTable.put(put2);
        ok = hTable.checkAndDelete(ROW, FAMILY, QUALIFIER, CompareFilter.CompareOp.EQUAL, value2, delete);
        assertEquals(ok, true);
    }
    @Test
    public void testCheckAndMutate() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartCheckAndMutateTest::testCheckAndMutate);
    }

    @Test
    public void testCheckAndDelete() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartCheckAndMutateTest::testCheckAndDeleteWithCompareOp);
    }

    @Test
    public void testCheckAndPut() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartCheckAndMutateTest::testCheckAndPut);
    }

    @Test
    public void testCheckAndMutateDiffRow() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartCheckAndMutateTest::testCheckAndMutateDiffRow);
    }

    @Test
    public void testCheckAndPutWithCompareOp() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartCheckAndMutateTest::testCheckAndPutWithCompareOp);
    }
}
