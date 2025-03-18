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
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.hbase.util.ObHTableSecondaryPartUtil.*;
import static com.alipay.oceanbase.hbase.util.ObHTableTestUtil.FOR_EACH;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class OHTableSecondaryPartCellTTLTest {
    private static List<String> tableNames       = new LinkedList<String>();
    private static Map<String, List<String>> group2tableNames = new LinkedHashMap<>();

    @BeforeClass
    public static void before() throws Exception {
        openDistributedExecute();
        for (TableTemplateManager.TableType type : TableTemplateManager.CELL_TTL_TABLES) {
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

    public static void testCellTTL(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String key1 = "key1";
        String column1 = "cf1";
        String column2 = "cf2";
        String column3 = "cf3";
        String family = getColumnFamilyName(tableName);
        String value1 = "v1";
        String value2 = "v2";
        String app = "app";

        Result r;
        Put put1 = new Put(key1.getBytes());
        put1.addColumn(family.getBytes(), column1.getBytes(), toBytes(11L));
        put1.setTTL(5000);
        Put put2 = new Put(key1.getBytes());
        put2.addColumn(family.getBytes(), column1.getBytes(), toBytes(22L));
        put2.addColumn(family.getBytes(), column2.getBytes(), toBytes(33L));
        put2.setTTL(10000);
        Put put3 = new Put(key1.getBytes());
        put3.addColumn(family.getBytes(), column1.getBytes(), toBytes(11L));
        put3.setTTL(-3000);
        Put put4 = new Put(key1.getBytes());
        put4.addColumn(family.getBytes(), column1.getBytes(), toBytes(11L));
        put4.setTTL(0);
        Put errorPut = new Put(key1.getBytes());
        errorPut.addColumn("family1".getBytes(), column1.getBytes(), toBytes(11L));
        errorPut.setTTL(10);

        Get get = new Get(key1.getBytes());
        get.addFamily(family.getBytes());
        get.setMaxVersions(10);
        try {
            hTable.put(errorPut);
        } catch (Exception e) {
            assertTrue(e.getCause().toString().contains("Unknown column 'TTL'"));
        }
        // test put and get
        hTable.put(put1);
        hTable.put(put2);
        hTable.put(put3);
        hTable.put(put4);
        r = hTable.get(get);
        assertEquals(3, r.size());
        Thread.sleep(5000);
        r = hTable.get(get);
        assertEquals(2, r.size());
        Thread.sleep(5000);
        r = hTable.get(get);
        assertEquals(0, r.size());

        // test increment
        hTable.put(put1);
        hTable.put(put2);
        Thread.sleep(1000);
        Increment increment = new Increment(key1.getBytes());
        increment.addColumn(family.getBytes(), column1.getBytes(), 1L);
        increment.addColumn(family.getBytes(), column2.getBytes(), 2L);
        increment.addColumn(family.getBytes(), column3.getBytes(), 5L);
        increment.setTTL(-5000);
        hTable.increment(increment);
        increment.setTTL(5000);
        hTable.increment(increment);
        get.setMaxVersions(1);
        r = hTable.get(get);

        assertEquals(3, r.size());
        assertEquals(23L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column1.getBytes()).get(0))));
        assertEquals(35L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column2.getBytes()).get(0))));
        assertEquals(5L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column3.getBytes()).get(0))));

        Thread.sleep(10000);
        r = hTable.get(get);
        assertEquals(0, r.size());

        increment = new Increment(key1.getBytes());
        increment.addColumn(family.getBytes(), column1.getBytes(), 1L);
        increment.addColumn(family.getBytes(), column2.getBytes(), 2L);
        increment.setTTL(5000);
        hTable.increment(increment);
        r = hTable.get(get);

        assertEquals(2, r.size());
        assertEquals(1L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column1.getBytes()).get(0))));
        assertEquals(2L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column2.getBytes()).get(0))));

        Thread.sleep(5000);
        r = hTable.get(get);
        assertEquals(0, r.size());

        hTable.put(put1);
        hTable.put(put2);
        increment.addColumn(family.getBytes(), column1.getBytes(), 4L);
        hTable.increment(increment);
        r = hTable.get(get);
        assertEquals(2, r.size());
        assertEquals(26L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column1.getBytes()).get(0))));
        assertEquals(35L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column2.getBytes()).get(0))));

        // test append
        Thread.sleep(10000);
        r = hTable.get(get);
        assertEquals(0, r.size());

        put3 = new Put(key1.getBytes());
        put3.addColumn(family.getBytes(), column1.getBytes(), toBytes(value1));
        put3.addColumn(family.getBytes(), column2.getBytes(), toBytes(value2));
        put3.setTTL(10000);
        hTable.put(put3);
        Append append = new Append(key1.getBytes());
        KeyValue kv = new KeyValue(key1.getBytes(), family.getBytes(), column1.getBytes(),
                app.getBytes());
        append.add(kv);
        append.setTTL(-3000);
        hTable.append(append);
        append.setTTL(3000);
        hTable.append(append);

        r = hTable.get(get);
        assertEquals(2, r.size());
        assertEquals(
                value1 + app,
                Bytes.toString(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                        column1.getBytes()).get(0))));

        Thread.sleep(3000);
        r = hTable.get(get);
        assertEquals(2, r.size());
        assertEquals(
                value1,
                Bytes.toString(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                        column1.getBytes()).get(0))));

        Thread.sleep(7000);
        r = hTable.get(get);
        assertEquals(0, r.size());

        append.add(family.getBytes(), column1.getBytes(), app.getBytes());
        hTable.append(append);
        r = hTable.get(get);
        assertEquals(1, r.size());
        assertEquals(
                app,
                Bytes.toString(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                        column1.getBytes()).get(0))));

        Thread.sleep(3000);
        append.add(family.getBytes(), column2.getBytes(), app.getBytes());
        hTable.append(append);
        r = hTable.get(get);
        assertEquals(2, r.size());
        assertEquals(
                app,
                Bytes.toString(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                        column1.getBytes()).get(0))));
        assertEquals(
                app,
                Bytes.toString(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                        column2.getBytes()).get(0))));

        // test checkAndMutate
        Thread.sleep(3000);
        r = hTable.get(get);
        assertEquals(0, r.size());
        hTable.put(put1);
        RowMutations rowMutations = new RowMutations(key1.getBytes());
        rowMutations.add(put2);
        Delete delete = new Delete(key1.getBytes());
        delete.addColumn(family.getBytes(), column1.getBytes());
        rowMutations.add(delete);
        boolean succ = hTable.checkAndMutate(key1.getBytes(), family.getBytes(),
                column1.getBytes(), CompareFilter.CompareOp.EQUAL, toBytes(11L), rowMutations);
        assertTrue(succ);
        r = hTable.get(get);
        assertEquals(r.size(), 2);
        assertEquals(22L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column1.getBytes()).get(0))));
        assertEquals(33L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column2.getBytes()).get(0))));

        Thread.sleep(10000);
        r = hTable.get(get);
        assertEquals(r.size(), 0);

        hTable.put(put1);
        rowMutations = new RowMutations(key1.getBytes());
        put4 = new Put(key1.getBytes());
        put4.addColumn(family.getBytes(), column1.getBytes(), toBytes(22L));
        put4.addColumn(family.getBytes(), column2.getBytes(), toBytes(33L));
        put4.setTTL(10000);
        rowMutations.add(put4);
        succ = hTable.checkAndMutate(key1.getBytes(), family.getBytes(), column1.getBytes(),
                CompareFilter.CompareOp.EQUAL, toBytes(1L), rowMutations);
        assertFalse(succ);
        succ = hTable.checkAndMutate(key1.getBytes(), family.getBytes(), column1.getBytes(),
                CompareFilter.CompareOp.EQUAL, toBytes(11L), rowMutations);
        assertTrue(succ);

        r = hTable.get(get);
        assertEquals(r.size(), 2);
        assertEquals(22L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column1.getBytes()).get(0))));
        assertEquals(33L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column2.getBytes()).get(0))));

        Thread.sleep(5000);
        r = hTable.get(get);
        assertEquals(2, r.size());
        assertEquals(22L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column1.getBytes()).get(0))));
        assertEquals(33L, Bytes.toLong(CellUtil.cloneValue(r.getColumnCells(family.getBytes(),
                column2.getBytes()).get(0))));

        Thread.sleep(5000);
        r = hTable.get(get);
        assertEquals(r.size(), 0);
        put1 = new Put(key1.getBytes());
        put1.addColumn(family.getBytes(), column1.getBytes(), toBytes(11L));
        hTable.put(put1);

        increment = new Increment(key1.getBytes());
        increment.addColumn(family.getBytes(), column1.getBytes(), 1L);
        hTable.increment(increment);
        r = hTable.get(get);
        assertEquals(r.size(), 1);
        assertEquals(Bytes.toLong(r.raw()[0].getValue()), 12L);
        get.setMaxVersions(10);
        r = hTable.get(get);
        assertEquals(r.size(), 2);

        put1.setTTL(-100);
        hTable.put(put1);
        Delete delete1 = new Delete(key1.getBytes());
        delete1.addColumn(family.getBytes(), column1.getBytes());
        hTable.delete(delete1);
        get.setMaxVersions(10);
        r = hTable.get(get);
        assertEquals(r.size(), 1);
        assertEquals(Bytes.toLong(r.raw()[0].getValue()), 11L);
        hTable.close();
    }

    public static void testCellTTLSQL(String tableName) throws Exception {
        OHTableClient hTable = ObHTableTestUtil.newOHTableClient(getTableName(tableName));
        hTable.init();

        String key1 = "key1";
        String column1 = "cf1";
        String column2 = "cf2";
        String column3 = "cf3";
        String family = getColumnFamilyName(tableName);
        String value1 = "v1";
        String value2 = "v2";
        String app = "app";

        Result r;
        Put put1 = new Put(key1.getBytes());
        put1.addColumn(family.getBytes(), column1.getBytes(), toBytes(11L));
        put1.setTTL(5000);
        Put put2 = new Put(key1.getBytes());
        put2.addColumn(family.getBytes(), column1.getBytes(), toBytes(22L));
        put2.addColumn(family.getBytes(), column2.getBytes(), toBytes(33L));
        put2.setTTL(10000);
        Put put3 = new Put(key1.getBytes());
        put3.addColumn(family.getBytes(), column1.getBytes(), toBytes(11L));
        put3.setTTL(-3000);
        Put put4 = new Put(key1.getBytes());
        put4.addColumn(family.getBytes(), column1.getBytes(), toBytes(11L));
        put4.setTTL(0);
        Put errorPut = new Put(key1.getBytes());
        errorPut.addColumn("family1".getBytes(), column1.getBytes(), toBytes(11L));
        errorPut.setTTL(10);

        // test put and get
        hTable.put(put1);
        hTable.put(put2);
        hTable.put(put3);
        hTable.put(put4);

        Assert.assertEquals(5, getSQLTableRowCnt(tableName));
        Thread.sleep(10000);
        openTTLExecute();

        checkUtilTimeout(()-> {
            try {
                return getRunningNormalTTLTaskCnt() == 0;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, 50000, 3000);

        Assert.assertEquals(0, getSQLTableRowCnt(tableName));

        // 6. close ttl knob
        closeTTLExecute();
        hTable.close();
    }

    @Test
    public void testCellTTL() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartCellTTLTest::testCellTTL);
    }

    @Test
    public void testCellTTLSQL() throws Throwable {
        FOR_EACH(tableNames, OHTableSecondaryPartCellTTLTest::testCellTTLSQL);
    }

}
